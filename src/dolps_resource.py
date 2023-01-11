from pydolphinscheduler.core.resource import Resource
from typing import Optional
import requests
import os
from pydolphinscheduler.java_gateway import JavaGate
from pydolphinscheduler.exceptions import PyDSJavaGatewayException
import configure
from configure import logger
from pathlib import Path


class DolpsResource(Resource):
    ##资源类型为FILE
    type = 'FILE'

    def __init__(self,
                 name: str,
                 content: Optional[str] = ' ',
                 description: Optional[str] = None,
                 user_name: Optional[str] = configure.user_name,
                 token: Optional[str] = configure.token
                 ):
        super().__init__(name, content, description, user_name)
        # 获取文件的目录
        self.headers = {'Accept': 'application/json', 'token': token}
        self.parent_dir = os.path.dirname(name)

    # 检查返回的值
    def check_response(self, response, func_name, path: Optional[str] = None):
        if response.status_code == 200:
            response_json = response.json()
            if response_json['success']:
                return True
            else:
                if path is None:
                    logger.warning('[' + func_name + ']' + self.name + ':' + response_json['msg'])
                else:
                    logger.warning('[' + func_name + ']' + path + ':' + response_json['msg'])
                return False
        else:
            logger.error(
                '[' + func_name + ']: send request is fail, please check internet.  status code: ' + str(
                    response.status_code))
            raise PyDSJavaGatewayException(
                '[' + func_name + ']: send request is fail, please check internet.  status code: ' + str(
                    response.status_code))

    # 创建目录
    def create_dir(self):
        ##判断父目录id是否存在
        if self.is_resource_exist(self.parent_dir):
            pid = self.get_parent_dir_id()
        else:
            logger.error('`parent_dir`: ' + self.parent_dir + ' is not exist in dolphinsheduler resource.')
            raise PyDSJavaGatewayException(
                "`parent_dir`: " + self.parent_dir + " is not exist in dolphinsheduler resource."
            )
        file = Path(self.name).name
        params = {'currentDir': self.parent_dir, 'pid': pid, 'name': file, 'type': self.type,
                  'description': self.description}
        response = requests.post(configure.url_create_dir, headers=self.headers, data=params)
        return self.check_response(response, 'create_dir')

    def create_file(self, fp):
        # 判断父目录id是否存在
        if self.is_resource_exist(self.parent_dir):
            pid = self.get_parent_dir_id()
        else:
            logger.error('`parent_dir`: ' + self.parent_dir + ' is not exist in dolphinsheduler resource.')
            raise PyDSJavaGatewayException(
                "`parent_dir`: " + self.parent_dir + " is not exist in dolphinsheduler resource."
            )
        # 获取参数
        file_name = Path(self.name).name
        params = {'type': self.type, 'name': file_name, 'pid': pid, 'currentDir': self.parent_dir}
        # 配置传输的文件
        files = {'file': (file_name, fp, 'application/json')}
        response = requests.post(configure.url_create_resource, headers=self.headers, params=params, files=files)
        return self.check_response(response, 'create_file')

    def update_file(self, fp):
        id = self.get_id_from_database()
        # 修改url
        url_param = {'id': id}
        url = configure.url_delete_update_resource.format(**url_param)
        # 配置参数
        file_name = Path(self.name).name
        params = {'id': id, 'name': file_name, 'type': self.type}
        # 配置传输的文件
        files = {'file': (file_name, fp, 'application/json')}
        response = requests.put(url, headers=self.headers, params=params, files=files)
        return self.check_response(response, 'update_file')

    def create_or_update_file(self, file_path):
        # 打开原文件
        fp = open(file_path, 'rb')
        # 判断文件是否存在
        if not self.is_resource_exist():
            success = self.create_file(fp)
        else:
            success = self.update_file(fp)
        fp.close()
        return success

    # 删除资源或者目录
    def delete_resource(self):
        ##获取name的id
        id = self.get_id_from_database()
        params = {'id': id}
        url = configure.url_delete_update_resource.format(**params)
        response = requests.delete(url, headers=self.headers)
        return self.check_response(response, 'delete_resource')

    # 获取目录id
    def get_parent_dir_id(self):
        if self.parent_dir == '/':
            return -1
        if not self.parent_dir:
            raise PyDSJavaGatewayException(
                "`currentDir` is required when querying resources from python gate."
            )
        return JavaGate().query_resources_file_info(self.user_name, self.parent_dir).getId()

    # 重命名resource
    # @Param new_name: new file' name(warn: not full name)
    def rename_resource(self, new_name):
        ##获取name的id
        id = self.get_id_from_database()
        url_id = {'id': id}
        params = {'id': id, 'description': self.description, 'name': new_name, type: self.type}
        ##url拼接
        url = configure.url_rename_resource.format(**url_id)
        response = requests.put(url, headers=self.headers, params=params)
        return self.check_response(response, 'rename_resource')

    # 判断资源是否存在
    def is_resource_exist(self, full_name: Optional[str] = None):
        if full_name is None:
            file_name = self.name
        else:
            file_name = full_name
        ##跟目录,直接返回TURE
        if file_name == '/':
            return True
        params = {'fullName': file_name, 'type': self.type}
        ##假如资源存在，respond的success = false 报resource already exists 错误
        response = requests.get(configure.url_query_resource, headers=self.headers, params=params)
        return not self.check_response(response, 'is_resource_exist', file_name)


