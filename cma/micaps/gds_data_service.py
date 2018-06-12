# -*- coding:utf-8 -*-

from . import data_block_pb2
import os, sys, time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import threading
import configparser
from queue import Queue
if sys.version_info[0] == 3:
    import http.client as httplib
else:
    import httplib


#获取分布式MICAPS的GDS服务器地址和端口号
#todo 读取配置信息的方法还要完善一下
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR,'config/config.ini'))
ip = config['MICAPS_MDFS_GDS']['ip']
port = config['MICAPS_MDFS_GDS'].getint('port')

class AcquireAbnormalData(Exception):
    def __init__(self,err='获取的数据为空，请求数据不存在或超过时间期限'):
        Exception.__init__(self,err)


class GDSDataService(object):

    total_connect_times = 0
    total_request_times = 0

    def __init__(self, gds_ip=ip, gds_port=port):
        self.http_client = httplib.HTTPConnection(gds_ip, gds_port, timeout=120)  # 多线程下载必须重开连接
        self.gds_ip = gds_ip
        self.gds_port = gds_port

        # 用于多线程下载
        self.http_clients_pool = []
        #self.download_queue = Queue()

        # 不使用列表自己实现线程通讯，主要原因是不方便动态生成下载列表
        # self.DOWNLOAD_LIST = []
        # self.lock = threading.Lock()

        # 记录连接次数
        self.add_one_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        # 关闭单个连接
        self.http_client.close()
        # 关闭多线程使用过程中产生的多个连接
        for http_client in self.http_clients_pool:
            http_client.close()

    @staticmethod
    def _get_concate_url(request_type, directory, file_name, filter_):
        """
        将请求参数拼接到url
        """
        url = ''.join(["/DataService", "?requestType=", request_type, "&directory=", directory,
                       "&fileName=", file_name, "&filter=", filter_])
        return url

    def _get_http_result(self, url):

        self.http_client.request('GET', url)
        response = self.http_client.getresponse()

        self.add_one_request()

        return response.status, response.read()

    def _get_http_result_threading(self, url):
        """
        返回http请求结果的多线程版，主要用于多线程数据下载
        """
        http_client = httplib.HTTPConnection(self.gds_ip, self.gds_port, timeout=120)  # 多线程下载必须重开连接
        self.http_clients_pool.append(http_client)

        http_client.request('GET', url)
        response = http_client.getresponse()

        self.add_one_connect()
        self.add_one_request()

        return response.status, response.read()

    def get_latest_data_name(self, directory, filter_):
        """
        获取directory目录下最近数据文件名
        例：get_latest_data_name('ECMWF_HR/TMP/850','*.240')
        """
        status, response = self._get_http_result(self._get_concate_url("getLatestDataName", directory, "", filter_))
        string_result = data_block_pb2.StringResult()

        if status == 200:
            string_result.ParseFromString(response)

            if string_result is not None:
                return string_result.name
            else:
                raise Exception('Google protobuf error!')
        else:
            raise Exception('http connection error!')

    def get_file_list(self, directory, filter_='', is_absolute=False, no_unclipped=True):
        """
        获取directory下级目录的所有目录或文件列表，过滤条件为以filter_开头,
        返回结果是列表生成器
        返回根目录下的所有目录：get_file_list('/')
        返回德国细网格下一级目录：get_file_list('GERMAN_HR/')或get_file_list('GERMAN_HR')，
                                注意不能是get_file_list('/GERMAN_HR')
        """
        status, response = self._get_http_result(self._get_concate_url("getFileList", directory, "", ""))
        mapping_result = data_block_pb2.MapResult()

        if status == 200:
            mapping_result.ParseFromString(response)

            if mapping_result is not None:
                file_list_results = mapping_result.resultMap
                if no_unclipped:
                    if is_absolute:
                        file_list = (os.path.join(directory,file) for file in file_list_results
                                     if (file.startswith(filter_) and 'UNCLIPPED' not in file))
                    else:
                        file_list = (file for file in file_list_results if (file.startswith(filter_) and 'UNCLIPPED' not in file))
                else:
                    if is_absolute:
                        file_list = (os.path.join(directory,file) for file in file_list_results if file.startswith(filter_))
                    else:
                        file_list = (file for file in file_list_results if file.startswith(filter_))

                return file_list
            else:
                raise Exception('Google protobuf error!')
        else:
            raise Exception('http connection error!')

    def get_data(self, path, file_name=None):
        """
        获取path目录下文件名为file_name的二进制数据, 或者文件文件直接由path给出
        """
        if not file_name:
            directory = os.path.dirname(path)
            file_name = os.path.basename(path)
        else:
            directory = path

        status, response = self._get_http_result(self._get_concate_url("getData", directory, file_name, ""))
        byte_array_result = data_block_pb2.ByteArrayResult()

        if status == 200:
            byte_array_result.ParseFromString(response)

            if byte_array_result is not None:
                if not byte_array_result.byteArray:
                    raise AcquireAbnormalData()

                return byte_array_result.byteArray
            else:
                raise Exception('Google protobuf error!')
        else:
            raise Exception('http connection error!')

    def _get_data_threading(self, path, file_name=None):
        """
        get_data的多线程版本，由于多线程下载，获取path目录下文件名为file_name的二进制数据, 或者文件文件直接由path给出
        """
        if not file_name:
            directory = os.path.dirname(path)
            file_name = os.path.basename(path)
        else:
            directory = path

        status, response = self._get_http_result_threading(self._get_concate_url("getData", directory, file_name, ""))
        byte_array_result = data_block_pb2.ByteArrayResult()

        if status == 200:
            byte_array_result.ParseFromString(response)

            if byte_array_result is not None:
                if not byte_array_result.byteArray:
                    raise AcquireAbnormalData()

                return byte_array_result.byteArray
            else:
                raise Exception('Google protobuf error!')
        else:
            raise Exception('http connection error!')

    def _get_file_info(self, path):
        '''
        判断路径path是文件还是目录，返回的代码为-1表示path为目录，>0表示path为文件
        '''
        status, response = self._get_http_result(self._get_concate_url("getFileInfo", path, "", ""))

        file_info_result = data_block_pb2.FileInfoResult()

        if status == 200:
            file_info_result.ParseFromString(response)

            if file_info_result is not None:

                return file_info_result.fileSize
            else:
                raise Exception('Google protobuf error!')
        else:
            raise Exception('http connection error!')

    # todo print(gds.is_dir('APCP'))也返回true bug！
    def is_dir(self, path):
        if self._get_file_info(path) == -1:
            return True
        else:
            return False

    def is_file(self, path):
        if self._get_file_info(path) > 0:
            return True
        else:
            return False


    def _produce_download_queue(self, to_download):
        self.download_queue = Queue()  # Queue必须每次下载重新初始化，否则第二次下载无法终止
        for item in to_download:
            self.download_queue.put(item)


    def _download_worker(self, output_directory, overwrite, output_path_fun):
        # todo 尝试使用协程进行下载
        """
        用于多线程下载方法 download_data_threading 内部调用
        """
        #start = time.clock()
        while True:
            file_path = self.download_queue.get()
            if file_path is None:
                break

            try:
                byte_array = self._get_data_threading(file_path)
            except AcquireAbnormalData():
                continue
            # todo 错误处理需要进一步细化
            except Exception:
                self.download_queue.put(file_path)
                continue

            # 输出路径处理：
            file_name = os.path.basename(file_path)
            if not output_path_fun:
                output_dir = os.path.join(output_directory, os.path.dirname(file_path))
            else:
                output_dir = output_path_fun(file_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # 下载写入文件
            if overwrite or not os.path.isfile(os.path.join(output_dir, file_name)):
                with open(os.path.join(output_dir, file_name), 'wb') as f:
                    f.write(byte_array)

            self.download_queue.task_done()


    def download(self, output_directory, to_download, overwrite=False, output_path_fun=None, thread_n=8):
        """
        多线程下载，下载directory目录下文件名以filter开头的数据文件，下载保存目录为output_directory
        默认线程数目为8
        :param output_directory: 数据下载输出的根目录
        :param to_download: 等待下载的列表或可以生成列表的函数，下载列表中的元素必须是指定数据的全路径，如'ECMWF_HR/TMP_2M/18060208.012'
        :param overwrite: 是否覆盖已经存在的同名数据, 默认不覆盖
        :param thread_n: 多线程下载默认启用的线程数目，默认为8
        :param output_path_fun: 对输出路径进行控制的函数，默认不需要提供，默认输出路径与下载列表元素的全路径相同
        :return: 无
        """
        # todo output_path_fun需要处理

        if isinstance(to_download, str):
            # todo 处理字符串格式的下载列表请求
            to_download = self.get_file_list('ECMWF_LR/PRMSL','18060320',True)

        produce_thread = threading.Thread(target=self._produce_download_queue, args=(to_download,))   # 注意参数必须是元组，一个元素要加逗号
        produce_thread.start()

        consume_threads = []
        for i in range(thread_n):
            th = threading.Thread(target=self._download_worker, args=(output_directory, overwrite, output_path_fun))
            th.start()
            consume_threads.append(th)


        # 等待下载列表生成完毕
        produce_thread.join()

        # 等待下载任务完成
        self.download_queue.join()

        # 终止工作线程
        for i in range(thread_n):
            self.download_queue.put(None)

        for th in consume_threads:  # 让主线程等待其他线程，不能将join和start放在同一个循环中，否则实际上只启动了单个线程
            th.join()


    def download_sequential(self, output_directory, directory, filter_):
        """
        顺序下载，下载directory目录下文件名以filter开头的数据文件，下载保存目录为output_directory
        """
        for file_name in self.get_file_list(directory, filter_):
            byte_array = self.get_data(directory, file_name)
            with open(os.path.join(output_directory, file_name), 'wb') as f:
                f.write(byte_array)


    def walk(self, root, no_unclipped=True):
        """
        遍历分布式micaps中root下的所有目录，默认不包含unclipped的目录
        目录形式为(root_dir, dir_names)的元组，类似于python os.walk方法返回的(root_dir,dir_names,file_names)
        不同的地方是返回结果没有包含最终文件file_names,因为分布式micaps中的文件比较多，被动返回比较耗时
        """
        if not list(self.get_file_list(root)):#递归返回条件，最终是文件
             return
        else:
            for i in self.get_file_list(root):

                if no_unclipped and 'UNCLIPPED' in i: # 忽略UNCLIPPED目录
                    continue

                root_current = '/'.join([root,i]) # 必须是全路径，is_directory才能正确判断，路径不要使用\，否则出错

                if self.is_dir(root_current):# 当前目录是文件夹

                    for j in self.get_file_list(root_current):
                        if self.is_file('/'.join([root_current,j])): # 当前目录已经是最后一级目录
                            yield root_current, []
                            break
                    else: # 当前目录还有次级目录
                        if no_unclipped:
                            next_dir = [i for i in self.get_file_list(root_current) if 'UNCLIPPED' not in i]
                        else:
                            next_dir = [i for i in self.get_file_list(root_current)]
                        yield (root_current, next_dir)

                    yield from self.walk(root_current, no_unclipped) # 以当前目录作为根目录递归

                else: #不是文件夹说明已经进入最终文件所在目录，直接退出，可以加速很多
                    break


    # todo 增加迭代器循环迭代数据
    items = get_file_list

    @classmethod
    def add_one_connect(cls):
        cls.total_connect_times += 1

    @classmethod
    def add_one_request(cls):
        cls.total_request_times += 1