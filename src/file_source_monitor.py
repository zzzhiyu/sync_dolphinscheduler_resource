import os
from watchdog.observers import Observer
from file_event_handler import FileEventHandler,FileSystem
from watchdog.utils.dirsnapshot import DirectorySnapshot
from configure import logger
import dao


# 文件源监控类：调用start()方法后，源文件会与dolps资源中心进行同步，并进行备份
class SourceMonitor(object):
    def __init__(self, src_dir, dolps_src_dir):
        self.src_dir = src_dir
        self.dolps_src_dir = dolps_src_dir

    # 遍历资源目录，取出没有更新到dolps和备份中心的文件，并进行更新。并返回备份文件中的过期文件
    # 文件路径按由短到长规律创建
    def update_latest_resource(self, src_snapshot, his_snapshot_dict):
        for path in sorted(src_snapshot.paths, key=len, reverse=False):
            # 过滤根目录
            if path == self.src_dir:
                continue
            file_sys = FileSystem(path,
                                  src_snapshot.mtime(path),
                                  src_snapshot.size(path),
                                  src_snapshot.isdir(path))
            dolps_file = his_snapshot_dict.get(path, None)
            # dolps中有该文件或者目录，dict进行删除
            if dolps_file is not None:
                his_snapshot_dict.pop(path)
            if file_sys.is_dir:
                # dolps没有该目录,需要上传文件到dolps_resource
                if dolps_file is None:
                    # dolps文件路径和备份文件路径
                    dolps_dir = str(path).replace(self.src_dir, self.dolps_src_dir)
                    file_sys.mkdir_in_dolps(dolps_dir)
            else:
                # dolps没有该文件,需要上传文件到dolps_resource
                # 资源目录的更新或者创建时间大于dolps文件时间,需要更新文件到dolps_resource
                # 资源目录与dolps文件大小不相同，需要更新文件到dolps_resource
                if dolps_file is None or file_sys.update_time > dolps_file.update_time \
                        or file_sys.size != dolps_file.size:
                    # dolps文件路径
                    dolps_file = str(path).replace(self.src_dir, self.dolps_src_dir)
                    file_sys.create_or_update_file_in_dolps(dolps_file)

    #删除过期的文件
    def delete_expire_resource(self, his_snapshot_dict):
        # 删除文件或者目录，按照长度最长的原则：子目录或者子文件的全路径一定比父目录的更长
        for key in sorted(his_snapshot_dict.keys(), key=len, reverse=True):
            # 删除dolps文件
            backup_file_sys = his_snapshot_dict.get(key)
            backup_file_sys.delete_dolps_resource()

    # 程序启动之前，对以前没有同步到dolphinscheduler_resource的文件提前同步
    def resource_his_sync(self):
        # 获取资源目录下文件的镜像
        src_snapshot = DirectorySnapshot(self.src_dir)
        # 获取海豚资源中心所有文件信息
        dolps_files_info = dao.get_all_dolps_resource_info()
        # 获取dolps目录信息并写入dict
        his_snapshot_dict = {}
        for file_info in dolps_files_info:
            # 获取文件信息
            dolps_file_sys = FileSystem(file_info['name'], file_info['mtime'], file_info['size'], file_info['is_dir'])
            # 将dolps文件的名字改为对于的资源文件名
            new_path = str(file_info['name']).replace(self.dolps_src_dir, self.src_dir)
            his_snapshot_dict[new_path] = dolps_file_sys

        # 清空文件信息
        dolps_files_info.clear()

        # 更新最新资源
        self.update_latest_resource(src_snapshot, his_snapshot_dict)
        logger.info("update new resource end...")
        # 删除过期资源
        if his_snapshot_dict is not None and len(his_snapshot_dict) > 0:
            self.delete_expire_resource(his_snapshot_dict)
        his_snapshot_dict.clear()
        logger.info("delete expired resource end...")
        logger.info('success sync the history file')

    # 开始监控
    def start(self):
        # 创建observer对事件进行监控
        observer = Observer()
        try:
            # 创建文件事件处理器
            event_handle = FileEventHandler(self.src_dir, self.dolps_src_dir)
            # 设置监控逻辑和监控目录
            observer.schedule(event_handle, self.src_dir, True)
            # 开始监控
            observer.start()
            # 先对初始的资源文件进行同步，然后再监控更新：这样会导致在初始资源(取当前时间文件的镜像)同步过程中，文件会可能会进行更新，而初
            # 始文件更新完成后再去监控，会导致丢失最新的数据。
            # 开始同步初始文件夹数据--->获取文件夹下文件的镜像--->开始同步--->同步完成--->开始监控
            #                                           文件夹下的文件有更新--->更新信息丢失
            # 先监控资源文件，再同步数据：同步当中，假如数据进行更新，监控可以保存更新信息，同步完成后再更新。而监控开始到开始同步以前资
            # 源的过程中，文件会可能进行更新，历史同步和监控更新都会有更新信息。但由于更新操作都是可重复执行的，重复执行不会影响两个地方文
            # 件的一致性，所以没有问题。
            # 开始监控 -->文件有更新--->检测到同步没成功--->等待---------------------------->检测到同步完成--->更新文件信息
            #                        (镜像和保存信息中可能有重复的更新信息，但更新信息可以重复执行，文件信息的一致性不变)
            #                  开始同步初始文件夹数据--->获取文件夹下文件的镜像--->开始同步--->同步完成
            #
            # 将文件同步到dolps资源和备份文件中
            self.resource_his_sync()
            #成功同步历史文件
            event_handle.success_sync_his = True
            # 程序挂起
            observer.join()
        except Exception as error:
            logger.error(error.__str__())
            observer.stop()
            exit(-1)
