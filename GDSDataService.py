# -*- coding:utf-8 -*-

import http.client as httplib
import DataBlock_pb2
import os

def get_http_result (host, port, url):
    http_client = None
    try:
        http_client = httplib.HTTPConnection(host, port, timeout=120)
        http_client.request('GET', url)

        response = http_client.getresponse()
        return response.status,  response.read()
    except Exception as e:
        print(e)
        return 0
    finally:
        if http_client:
            http_client.close()


class GDSDataService:
    def __init__(self, gdsIp, gdsPort):
        self.gdsIp = gdsIp
        self.gdsPort = gdsPort #GDS服务器地址


    def getLatestDataName(self, directory, filter):
        return get_http_result(self.gdsIp, self.gdsPort, "/DataService" +
                                          self.get_concate_url("getLatestDataName", directory, "", filter))

    def getFileList(self,directory):
        return get_http_result(self.gdsIp,self.gdsPort,"/DataService" +
                                          self.get_concate_url("getFileList", directory, "",""))

    def getData(self, directory, fileName):
        return get_http_result(self.gdsIp, self.gdsPort, "/DataService" +
                                          self.get_concate_url("getData", directory, fileName, ""))


    # 将请求参数拼接到url
    def get_concate_url(self, requestType, directory, fileName, filter) :
        url = ""
        url += "?requestType=" + requestType
        url += "&directory=" + directory
        url += "&fileName=" + fileName
        url += "&filter=" + filter
        return url

    def download_data(self, output_directory, directory, filter):
        status, response = self.getFileList(directory)
        MappingResult = DataBlock_pb2.MapResult()


        if status == 200:
            MappingResult.ParseFromString(response)

            if MappingResult is not None:
                file_list_results = MappingResult.resultMap
                file_list = (file for file in file_list_results if file.startswith(filter))


                for file_name in file_list:
                    print(file_name)
                    status, response = self.getData(directory, file_name)
                    ByteArrayResult = DataBlock_pb2.ByteArrayResult()
                    if status == 200:
                        ByteArrayResult.ParseFromString(response)

                        if ByteArrayResult is not None:
                            byteArray = ByteArrayResult.byteArray

                            with open(os.path.join(output_directory,file_name), 'wb') as f:
                                f.write(byteArray)
                    else:
                        print('getData failed!')
            else:
                print('mapping fileList failed!')
        else:
            print('getFileList failed!')



if __name__ == '__main__':
    gds = GDSDataService("10.69.72.112", 8080)
    gds.download_data('D:/ECMWF','ECMWF_HR/TMP/850','17120708')


