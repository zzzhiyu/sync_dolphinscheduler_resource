from configure import logger
import configure
from dolps_resource import DolpsResource
import os
from file_source_monitor import SourceMonitor


if __name__ == '__main__':
    # 判断linux存放的源目录是否存在,假如不存在就退出
    if not os.path.exists(configure.src_dir):
        logger.error(configure.src_dir + ': source is not exist')
        exit('source is not exist')
    # 创建dolphinscheduler的资源目录
    dolpls_dir = DolpsResource(configure.dolps_src_dir)
    if not dolpls_dir.is_resource_exist():
        dolpls_dir.create_dir()
    logger.info('init the environment success...')
    # 监控文件，并上传信息
    monitor = SourceMonitor(configure.src_dir, configure.dolps_src_dir)
    monitor.start()
