# -*- coding:utf-8 -*-

import DataBlock_pb2
import os, sys, time
from datetime import datetime, timedelta

if sys.version_info[0] == 3:
    import http.client as httplib
else:
    import httplib


class GDSDataService(object):
    def __init__(self, gdsIp, gdsPort):
        self.http_client = httplib.HTTPConnection(gdsIp, gdsPort, timeout=120)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.http_client.close()

    def _get_concate_url(self, requestType, directory, fileName, filter):
        """
        将请求参数拼接到url
        """

        url = ""
        url += "?requestType=" + requestType
        url += "&directory=" + directory
        url += "&fileName=" + fileName
        url += "&filter=" + filter
        return url

    def _get_http_result(self, url):

        self.http_client.request('GET', url)
        response = self.http_client.getresponse()

        return response.status, response.read()

    def get_latest_data_name(self, directory, filter):
        """
        获取directory目录下最近数据文件名,如果返回None,表示获取失败
        """

        status, response = self._get_http_result(
            "/DataService" + self._get_concate_url("getLatestDataName", directory, "", filter))
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
        """
        获取directory目录下以filter开头的所有文件名组成的列表,如果返回None,表示获取失败
        """

        status, response = self._get_http_result(
            "/DataService" + self._get_concate_url("getFileList", directory, "", ""))
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
        """
        获取directory目录下文件名为file_name的二进制数据,如果返回None,表示获取数据失败
        """

        status, response = self._get_http_result(
            "/DataService" + self._get_concate_url("getData", directory, file_name, ""))
        ByteArrayResult = DataBlock_pb2.ByteArrayResult()

        if status == 200:
            ByteArrayResult.ParseFromString(response)

            if ByteArrayResult is not None:
                return ByteArrayResult.byteArray
            else:
                return None
        else:
            return None

    def download_data(self, output_directory, directory, filter=''):
        """
        下载directory目录下文件名以filter开头的数据文件，下载保存目录为output_directory
        """

        file_list = self.get_file_list(directory, filter)
        for file_name in file_list:
            byteArray = self.get_data(directory, file_name)
            with open(os.path.join(output_directory, file_name), 'wb') as f:
                f.write(byteArray)

    def bulk_download(self, output_directory, data_type, time='', config=''):

        if time == '':
            now = datetime.now()
            today = now.strftime('%y%m%d')
            nowtime = now.strftime('%y%m%d%H')
            if nowtime < today + "12":
                yesterday = now - timedelta(days=1)
                start_predict = yesterday.strftime('%y%m%d') + '20'
            else:
                start_predict = today + '08'
            time = start_predict  # "17020220"

        file_list = self.get_file_list(directory, time)

        pass


if __name__ == '__main__':

    with GDSDataService("10.69.72.112", 8080) as gds:
        # gds = GDSDataService("10.69.72.112", 8080)
        start = time.clock()
        # ***********************测试程序*********************************"
        # gds.bulk_download('D:/ECMWF','ECMWF_HR/TMP/850','171217')
        gds.download_data('D:/ECMWF', 'ECMWF_HR/TMP/850', '171218')
        # ***********************测试程序*********************************"
        end = time.clock()
        elapsed = end - start
        print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))
