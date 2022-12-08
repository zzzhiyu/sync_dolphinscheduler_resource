import configparser
import logging
import os
import datetime
from logging.handlers import TimedRotatingFileHandler


# 日志文件输入
def get_logger(name, current_log_dir):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    # set handler of print log message's format
    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)
    # set handler write
    # 创建日志存放目录
    if not os.path.exists(current_log_dir):
        os.makedirs(current_log_dir)
    dt = datetime.datetime.now().strftime('%Y_%m_%dT%H_%M_%S')
    path = current_log_dir + '/logs_' + dt
    path = path.replace('\\', '/')
    log_handler = TimedRotatingFileHandler(path, when="midnight", encoding="utf-8")
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(formatter)
    log.addHandler(log_handler)
    return log


# 获取配置文件的值
def get_value(section, option):
    # config file's path
    path = os.path.dirname(os.path.realpath(__file__)) + '/conf.ini'
    path = path.replace('\\', '/')
    # read config file
    config = configparser.ConfigParser()
    config.read(path, encoding='utf-8')
    # get the param
    return config.get(section, option)

# [url]
# 创建dolps目录url
url_create_dir = get_value('url', 'create_dir')
# 创建dolps文件url
url_create_file = get_value('url', 'create_file')
# 删除olps资源url
url_delete_resource = get_value('url', 'delete_update_resource')
# 重命名olps资源url
url_rename_resource = get_value('url', 'delete_update_resource')
# 查询dolps是否存在url
url_query_resource = get_value('url', 'query_resource')

# [db]
db_type = get_value('db', 'type')
db_host = get_value('db', 'host')
db_port = int(get_value('db', 'port'))
db_user = get_value('db', 'user')
db_database = get_value('db', 'database')
db_table = get_value('db', 'table')
db_password = get_value('db', 'password')

# [dolps]
# dolps的用户
user_name = get_value('dolps', 'user_name')
# dolps用户的token
token = get_value('dolps', 'token')
# 存放数据的dolps目录
dolps_src_dir = get_value('dolps', 'src_dir')
if dolps_src_dir is None or len(dolps_src_dir) == 0:
    dolps_src_dir = 'sync_dolps_resource'

# [source_file]
# 源数据的目录
src_dir = get_value('source_file', 'src_dir')
# 日志目录
log_dir = get_value('source_file', 'log_dir')

# 日志写入
logger = get_logger('sync_dolps_source_log', log_dir)
