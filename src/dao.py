import pymysql
import psycopg2
import time
import configure
from configure import logger


# 得到dolps文件的所有信息
def get_all_dolps_resource_info():
    if configure.db_type == 'mysql':
        conn = pymysql.connect(host=configure.db_host, port=configure.db_port, user=configure.db_user,
                               db=configure.db_database, passwd=configure.db_password)
    elif configure.db_type == 'postgresql':
        conn = psycopg2.connect(host=configure.db_host, port=configure.db_port, user=configure.db_user,
                                database=configure.db_database, password=configure.db_password)
    else:
        logger.error('Can\'t support this database:' + configure.db_type)
        raise ConnectionError('Can\'t support this database:' + configure.db_type)
    cur = conn.cursor()
    sql = '''select full_name, is_directory, size, update_time
             from {0}.{1}
             where type = 0 and full_name like '{2}%' and full_name != '{3}';'''.format(configure.db_database,
                                                                                        configure.db_table,
                                                                                        configure.dolps_src_dir,
                                                                                        configure.dolps_src_dir)
    cur.execute(sql)
    fetch_all_infos = cur.fetchall()
    file_infos = []
    for info in fetch_all_infos:
        file_info = {'name': info[0], 'is_dir': False if int(info[1]) == 0 else True, 'size': int(info[2]),
                     'mtime': time.mktime(time.strptime(str(info[3]), "%Y-%m-%d %H:%M:%S"))}
        file_infos.append(file_info)
    return file_infos
