# -*- coding:utf-8 -*-

import http.client as httplib
import DataBlock_pb2
import os

class GDSDataService:
    def __init__(self, gdsIp, gdsPort):
        self.gdsIp = gdsIp
        self.gdsPort = gdsPort  # GDS服务器地址

    def _get_concate_url(self, requestType, directory, fileName, filter):
        "将请求参数拼接到url"

        url = ""
        url += "?requestType=" + requestType
        url += "&directory=" + directory
        url += "&fileName=" + fileName
        url += "&filter=" + filter
        return url

    def _get_http_result(self, url):
        http_client = None
        try:
            http_client = httplib.HTTPConnection(self.gdsIp, self.gdsPort, timeout=120)
            http_client.request('GET', url)

            response = http_client.getresponse()
            return response.status, response.read()
        except Exception as e:
            print(e)
            return 0
        finally:
            if http_client:
                http_client.close()

    def get_latest_data_name(self, directory, filter):
        "获取directory目录下最近数据文件名,如果返回None,表示获取失败"

        status, response = self._get_http_result("/DataService" +
                                           self._get_concate_url("getLatestDataName", directory, "", filter))
        StringResult = DataBlock_pb2.StringResult()

        if status == 200:
            StringResult.ParseFromString(response)

            if StringResult is not None:
                return StringResult.name
            else:
                return None
        else:
            return None

    def get_file_list(self, directory, filter=''):
        "获取directory目录下以filter开头的所有文件名组成的列表,如果返回None,表示获取失败"

        status, response = self._get_http_result("/DataService" + self._get_concate_url("getFileList", directory, "", ""))
        MappingResult = DataBlock_pb2.MapResult()

        if status == 200:
            MappingResult.ParseFromString(response)

            if MappingResult is not None:
                file_list_results = MappingResult.resultMap

                file_list = (file for file in file_list_results if file.startswith(filter))
                return file_list
            else:
                return None
        else:
            return None

    def get_data(self, directory, file_name):
        "获取directory目录下文件名为file_name的二进制数据,如果返回None,表示获取数据失败"

        status, response = self._get_http_result("/DataService" + self._get_concate_url("getData", directory, file_name, ""))
        ByteArrayResult = DataBlock_pb2.ByteArrayResult()

        if status == 200:
            ByteArrayResult.ParseFromString(response)

            if ByteArrayResult is not None:
                return ByteArrayResult.byteArray
            else:
                return None
        else:
            return None

    def isfile(self, directory, path):
        '判断是不是数据文件'
        if self.get_data(directory, path) != b'':
            return True
        else:
            return False

    def isdir(self, directory, path):
        #todo 仍然有问题
        if self.get_data(directory, path) == b'':
            return True
        else:
            return False

    def is_ultimate_dir(self, directory):
        l = self.get_file_list(directory)
        pass

    def bulk_download(self, output_directory, directory, filter=''):
        "批量下载directory目录下文件名以filter开头的数据文件，下载保存目录为output_directory"

        file_list = self.get_file_list(directory, filter)
        for file_name in file_list:
            byteArray = self.get_data(directory, file_name)
            with open(os.path.join(output_directory, file_name), 'wb') as f:
                f.write(byteArray)


if __name__ == '__main__':
    gds = GDSDataService("10.69.72.112", 8080)

    gds.bulk_download('D:/ECMWF','ECMWF_HR/TMP/850','17121420.1')

