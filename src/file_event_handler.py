import time
from typing import Optional
from watchdog.events import *
import os
from configure import logger
from dolps_resource import DolpsResource


# 事件文件在海豚资源中心中进行删除，创建，更新
class FileSystem:
    def __init__(self, file_path,
                 update_time: Optional[str] = None,
                 size: Optional[int] = None,
                 is_dir: Optional[bool] = None):
        self.file_path = file_path
        self.update_time = update_time
        self.size = size
        self.is_dir = is_dir

    def delete_dolps_resource(self):
        logger.info('start delete dolps dir[' + self.file_path + ']...')
        # 删除dolps里面的文件
        dolps_resource = DolpsResource(name=self.file_path)
        # 对文件进行删除
        if dolps_resource.is_resource_exist():
            success = dolps_resource.delete_resource()
            if not success:
                logger.error("fail to delete dolps file:" + self.file_path)
                raise IOError("fail to delete dolps file:" + self.file_path)
        else:
            logger.warning(self.file_path + ' is not exists! fail to delete dolps resource')
        logger.info('success sync: delete resource[' + self.file_path + '] in dolps')

    # 将文件内容复制到dolps和备份
    def create_or_update_file_in_dolps(self, dolps_file: str):
        logger.info('start create or update dolps file[' + dolps_file + ']...')
        # 判断文件是否存在
        if not os.path.exists(self.file_path):
            logger.warning(self.file_path + 'is not exists')
            return
        # 获取文件的大小
        if self.size is None:
            self.size = os.stat(self.file_path).st_size
        # 对于空文件和大于5M的文件直接返回
        if self.size == 0:
            logger.warning(self.file_path + 'is empty! fail to create or update dolps resource')
            return
        elif self.size >= 5 * 1024 * 1024:
            logger.warning(self.file_path + '\'s size more than 5M! fail to create or update dolps resource')
            return
        else:
            dolps_resource = DolpsResource(name=dolps_file)
            success = dolps_resource.create_or_update_file(self.file_path)
            if not success:
                logger.error("fail to create or update dolps file:" + dolps_file)
                raise IOError("fail to create or update dolps file:" + dolps_file)
            logger.info('success sync: copy file[' + self.file_path + '] to dolps[' + dolps_file + ']')

    # 在dolps和备份中创建相应的目录
    def mkdir_in_dolps(self, dolps_dir: str):
        logger.info('start mkdir dolps dir[' + dolps_dir + ']...')
        # 创建dopls的目录
        dolps_resource = DolpsResource(name=dolps_dir)
        if not dolps_resource.is_resource_exist(dolps_dir):
            success = dolps_resource.create_dir()
            if not success:
                logger.error("fail to make dolps dir:" + dolps_dir)
                raise IOError("fail to make dolps dir:" + dolps_dir)
        else:
            logger.warning(dolps_dir + ' already exists! fail to create dir in dolps resource')
        logger.info('success sync: dir[' + self.file_path + '] to dolps[' + dolps_dir + ']')


class FileEventHandler(FileSystemEventHandler):
    # @Param src_dir:资源目录
    # @Param dolps_src_dir:dolps 资源目录
    def __init__(self, src_dir, dolps_src_dir):
        FileSystemEventHandler.__init__(self)
        self.src_dir = src_dir
        self.dolps_src_dir = dolps_src_dir
        self.success_sync_his = False

    # 资源中心的文件路径转化为海豚资源中心的文件路径
    def src_transfer_to_dolps(self, path):
        return str(path).replace(self.src_dir, self.dolps_src_dir)

    # 等待同步完成
    def wait_sync_his_success(self):
        if not self.success_sync_his:
            while not self.success_sync_his:
                time.sleep(2)

    def on_moved(self, event: FileMovedEvent):
        self.wait_sync_his_success()
        # 更新前的dolps文件名
        old_dolps_file = self.src_transfer_to_dolps(event.src_path)
        # 更新后的dolps文件名
        new_dolps_file = self.src_transfer_to_dolps(event.dest_path)
        # dols的文件系统，进行删除
        old_dolps_file_sys = FileSystem(old_dolps_file)
        # 文件夹文件系统进行复制
        new_file_sys = FileSystem(event.dest_path)
        if event.is_directory:
            new_file_sys.mkdir_in_dolps(new_dolps_file)
        else:
            new_file_sys.create_or_update_file_in_dolps(new_dolps_file)
        old_dolps_file_sys.delete_dolps_resource()

    def on_created(self, event: FileCreatedEvent):
        # 创建dolps资源文件信息
        self.wait_sync_his_success()
        file_sys = FileSystem(event.src_path)
        dolps_file = self.src_transfer_to_dolps(event.src_path)
        if event.is_directory:
            file_sys.mkdir_in_dolps(dolps_file)
        else:
            file_sys.create_or_update_file_in_dolps(dolps_file)

    def on_deleted(self, event: FileDeletedEvent):
        self.wait_sync_his_success()
        # 删除dolps资源信息
        dolps_file = self.src_transfer_to_dolps(event.src_path)
        dolps_file_sys = FileSystem(dolps_file)
        dolps_file_sys.delete_dolps_resource()

    def on_modified(self, event: FileModifiedEvent):
        self.wait_sync_his_success()
        # 更新dolps文件信息
        if not event.is_directory:
            file_sys = FileSystem(event.src_path)
            dolps_file = self.src_transfer_to_dolps(event.src_path)
            file_sys.create_or_update_file_in_dolps(dolps_file)
