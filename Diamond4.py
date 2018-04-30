# -*- coding:utf-8 -*-

import datetime
from math import sqrt, fabs, ceil, floor
import os
import numpy as np
import struct
from GDSDataService import GDSDataService


class Grid(object):
    def __init__(self, data_frame, data_parameters=None):
        if os.path.isfile(data_frame):  # 本地文件
            with open(data_frame, 'rb') as f:
                if struct.unpack('4s', f.read(4))[0] == b'mdfs':  # mdfs格式的本地文件
                    f.seek(0, 0)
                    self._unpack_bytes(f.read())
                else:  # micaps3格式
                    f.close()
                    with open(data_frame, 'r') as f:
                        self._parse_strings(f.readlines())

        else:  # 直接从GDS分布式服务器调取
            with GDSDataService("10.69.72.112", 8080) as gds:
                byte_arrays = gds.get_data(data_frame)
                self._unpack_bytes(byte_arrays)

        # 为一些常用字段起简单易用的别名，以x，y，row，col来标记相对更容易理解
        self.x_start = self.start_longitude  # 起始x，即起始经度
        self.x_end = self.end_longitude  # 终止x，即终止经度
        self.dx = self.longitude_grid_space  # 经度（x方向）格距, 一般为正
        self.y_start = self.start_latitude  # 起始y，即起始纬度
        self.y_end = self.end_latitude  # 终止y，即终止纬度
        self.dy = self.latitude_grid_space  # 纬度（y方向）格距，有正负号
        self.cols = self.latitude_grid_number  # 列数，即纬向(x方向)格点数目
        self.rows = self.longitude_grid_number  # 行数，即经向(y方向)格点数目

    def _unpack_bytes(self, byte_arrays):

        (discriminator,  # 合法数据关键字,始终为小写的mdfs，不以mdfs开头的数据为非法数据
         self.type  # 数据类型, 4为模式标量数据，11为模式矢量数据，与原系统diamond 4和diamond 11含义一致
         ) = struct.unpack('4s h', byte_arrays[0:6])

        (model_name,  # 模式名称, 建议采用全大写字母表示模式名称，不建议使用汉字。
         element,  # 物理量
         description,  # 附加描述信息
         self.level,  # 层次
         self.year,  # 起报日期：年
         self.month,  # 起报日期：月
         self.day,  # 起报日期：日
         self.hour,  # 起报时刻
         self.timezone,  # 时区
         self.period,  # 预报时效
         self.start_longitude,  # 起始经度
         self.end_longitude,  # 终止经度
         self.longitude_grid_space,  # 经度格距
         self.latitude_grid_number,  # 纬向经线格点数
         self.start_latitude,  # 起始纬度
         self.end_latitude,  # 终止纬度
         self.latitude_grid_space,  # 纬度格距
         self.longitude_grid_number,  # 经向纬线格点数
         self.isoline_start_value,  # 等值线起始值
         self.isoline_end_value,  # 等值线终止值
         self.isoline_space,  # 等值线间隔
         # extent  #扩展区，100字节
         ) = struct.unpack('20s 50s 30s f 6i 3fi 3fi 3f', byte_arrays[6:178])

        # 对字符串字段信息进行额外的解码处理
        text_info = [discriminator, model_name, element, description]
        [self.discriminator,
         self.model_name,
         self.element,
         self.description
         ] = [i.strip(b'\x00').decode('GBK') for i in text_info]

        data_num = self.latitude_grid_number * self.longitude_grid_number
        self.data = struct.unpack('%sf' % data_num, byte_arrays[278:])

    def _parse_strings(self, str_lines):

        # 去除空行读入,将原文件分割成一维字符串数组
        data_raw = [word for line in str_lines if line[:-1].strip() for word in line.split()]
        if data_raw[0] != 'diamond':
            raise Exception('格式错误！')

        self.type = int(data_raw[1])  # 数据类型, 4为模式标量数据，11为模式矢量数据
        self.description = data_raw[2]  # .decode('gbk')  # 说明字符串

        (year,  # 年
         self.month,  # 月
         self.day,  # 日
         self.hour,  # 时
         self.period,  # 时效
         self.level  # 层次
         ) = data_raw[3:9]

        if len(year) == 2:
            year = ('20' + year) if int(year) < 49 else ('19' + year)
        elif len(year) == 4:
            pass
        else:
            raise Exception('year parameter error!')

        self.year = year

        # 注意start_time和valid_time没有统一规定，要看具体情况
        self.start_time = datetime.datetime(int(year), int(self.month), int(self.day), int(self.hour))
        self.valid_time = self.start_time + datetime.timedelta(hours=int(self.period))

        (self.longitude_grid_space,  # 经度（x方向）格距, 一般为正
         self.latitude_grid_space,  # 纬度（y方向）格距，有正负号
         self.start_longitude,  # 起始经度
         self.end_longitude,  # 终止经度
         self.start_latitude,  # 起始纬度
         self.end_latitude  # 终止纬度
         ) = (float(i) for i in data_raw[9:15])

        (self.latitude_grid_number,  # 纬向(x方向)格点数目，即列数
         self.longitude_grid_number  # 经向(y方向)格点数目，即行数
         ) = (int(i) for i in data_raw[15:17])

        (self.isoline_space,  # 等值线间隔
         self.isoline_start_value,  # 等值线起始值
         self.isoline_end_value,  # 等值线终止值
         self.smooth_factor,  # 平滑系数
         self.bold_line  # 加粗线值
         ) = (float(i) for i in data_raw[17:22])

        # 数据部分，以一维数组表示
        self.data = [float(i) for i in data_raw[22:]]

        del data_raw

    def __getitem__(self, index):
        if isinstance(index, tuple) and len(index) == 2:
            return self.value(index[0], index[1])
        else:
            raise Exception('indexing error!')

    def __sub__(self, other):
        if self.rows == other.rows and self.cols == other.cols:
            return [x - y for x, y in zip(self.data, other.data)]

    def value(self, row, col):
        '''将格点数据看成self.latitude_grid_number*self.nums_lat的二维数组，返回第row行，第col列的值，
        row和col必须为整数，从0开始计数，坐标原点在左上角'''
        if row < 0 or row >= self.rows or col < 0 or col >= self.cols:
            raise Exception('out of data spatial range')
        return self.data[row * self.cols + col]

    def IDW(self, lon_lat_s, power=2):
        """
        反距离加权法提取站点数据
        :param lon_lat_s: 以[(lon1,lat1）,(lon2,lat2),……]形式传入的一系列站点位置,经纬度必须是弧度形式
        :param power:
        :return: 对应站点位置的插值结果列表
        """
        extracted_values = []
        for lon, lat in lon_lat_s:
            # 根据目标位置经纬度计算其周围四个格点在二维数组中的起始和终止行列号
            col_beg = int(fabs((lon - self.x_start) / self.dx))
            row_beg = int(fabs((lat - self.y_start) / self.dy))
            col_end = col_beg + 1
            row_end = row_beg + 1

            # 计算包围目标位置的经纬度范围,即起始和终止行列号的对应经纬度，行号与纬度对应，列号与经度对应
            lon_beg = self.x_start + self.dx * col_beg
            end_longitude = self.x_start + self.dx * col_end
            lat_beg = self.y_start + self.dy * row_beg
            end_latitude = self.y_start + self.dy * row_end

            # 根据目标位置与周围四个格点的经纬度距离计算权重
            w1 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w2 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (end_latitude - lat) ** 2)) ** power
            w3 = 1.0 / (sqrt((end_longitude - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w4 = 1.0 / (sqrt((end_longitude - lon) ** 2 + (end_latitude - lat) ** 2)) ** power

            # 目标位置周围四个格点的值
            d1 = self.value(row_beg, col_beg)
            d2 = self.value(row_end, col_beg)
            d3 = self.value(row_beg, col_end)
            d4 = self.value(row_end, col_end)

            # 根据反距离加权计算最终值，注意权重与格点要一一对应
            z = (d1 * w1 + d2 * w2 + d3 * w3 + d4 * w4) / (w1 + w2 + w3 + w4)

            extracted_values.append(z)

        return extracted_values

    def to_esri_ascii(self, out_name):
        with open(out_name, 'w') as f:
            y_start = self.y_end if self.dy < 0 else self.y_start
            header = 'NCOLS %d\nNROWS %d\nXLLCENTER %f\nYLLCENTER %f\nCELLSIZE %f\nNODATA_VALUE 9999.0\n' % (
                self.cols, self.rows, self.x_start, y_start, self.dx)
            f.write(header)

            if self.dy < 0:
                f.write(' '.join(map(str, self.data)))
            else:
                for i in range(self.rows - 1, -1, -1):
                    f.write(' '.join(
                        map(str, self.data[i * self.cols:(i + 1) * self.cols])))
                    f.write('\n')  # 必须加换行符，因为' '.join最后还多了一个空格，arcgis不能根据列数自动计算
        try:
            import arcpy
        except ImportError:
            print("""warning: you have no Esri's arcpy module, using to_esri_ascii method,
                    you can still get the result, but without the associate coordinate information.
                    you can use Esri's software like Arcmap to import the result and add the coordinate which is
                    WGS1984""")
        else:
            # 定义坐标系//define the coordinate
            sr = arcpy.SpatialReference('WGS 1984')
            arcpy.DefineProjection_management(out_name, sr)

    def to_file(self, out_name, formatted=False):
        with open(out_name, 'w') as f:
            f.write('diamond 4 ' + self.doc + '\n')
            f.write(' '.join([self.year, self.month, self.day, self.hour, self.period, self.level, '\n']))

            f.write(
                ' '.join(
                      ['%.2f' % i for i in [self.dx, self.dy, self.x_start, self.x_end, self.y_start, self.y_end]]
                    + ['%d' % i for i in [self.cols, self.rows]]
                    + ['%.2f' % i for i in [self.isoline_space, self.isoline_start_value,
                                            self.isoline_end_value, self.smooth_factor,self.bold_line]] +['\n']
                    )
            )

            if not formatted:
                f.write(' '.join(['%.2f' % i for i in self.data]))

    def to_numpy(self):
        return np.array(self.data).reshape(self.rows, self.cols)

    def max(self):
        return max(self.data)

    def min(self):
        return min(self.data)

    def calc_stats(self):
        pass

    def extract_station_value(self, lon_lat_s, method):
        '提取站点数据'
        pass

class Diamond4(object):
    diamond = 4

    def __init__(self, data_frame, data_parameters=None):

        if data_parameters is None and os.path.isfile(data_frame):
            with open(data_frame, 'r') as f:
                data_raw = [word for line in f.readlines() if line[:-1].strip()
                            for word in line.split()]  # 去除空行读入,将原文件分割成一维字符串数组

                self.doc = data_raw[2]  # .decode('gbk')  # 说明字符串

                # 日期时间处理
                (year,  # 年
                 self.month,  # 月
                 self.day,  # 日
                 self.hour,  # 时
                 self.period,  # 时效
                 self.level) = data_raw[3:9]  # 层次

                if len(year) == 2:
                    year = ('20' + year) if int(year) < 49 else ('19' + year)
                elif len(year) == 4:
                    pass
                else:
                    raise Exception('year parameter error!')

                self.year = year

                # 注意start_time和valid_time没有统一规定，要看具体情况
                self.start_time = datetime.datetime(int(year), int(self.month), int(self.day), int(self.hour))
                self.valid_time = self.start_time + datetime.timedelta(hours=int(self.period))

                (self.longitude_grid_space,  # 经度（x方向）格距, 一般为正
                 self.latitude_grid_space,  # 纬度（y方向）格距，有正负号
                 self.start_longitude,  # 起始经度
                 self.end_longitude,  # 终止经度
                 self.start_latitude,  # 起始纬度
                 self.end_latitude) = (float(i) for i in data_raw[9:15])  # 终止纬度

                (self.latitude_grid_number,  # 纬向(x方向)格点数目，即列数
                 self.longitude_grid_number) = (int(i) for i in data_raw[15:17])  # 经向(y方向)格点数目，即行数

                (self.isoline_space,  # 等值线间隔
                 self.isoline_start_value,  # 等值线起始值
                 self.isoline_end_value,  # 等值线终止值
                 self.smooth_factor,  # 平滑系数
                 self.bold_line) = (float(i) for i in data_raw[17:22])  # 加粗线值

                # 数据部分，以一维数组表示
                self.data = [float(i) for i in data_raw[22:]]

                # 将数据的一些属性参数集合到一个字典中
                self.parameters = {'doc': self.doc, 'year': self.year, 'month': self.month, 'day': self.day,
                                   'hour': self.hour, 'period': self.period, 'level': self.level,
                                   'start_time': self.start_time, 'valid_time': self.valid_time,
                                   'longitude_grid_space': self.longitude_grid_space,
                                   'latitude_grid_space': self.latitude_grid_space,
                                   'start_longitude': self.start_longitude, 'end_longitude': self.end_longitude,
                                   'start_latitude': self.start_latitude, 'end_latitude': self.end_latitude,
                                   'latitude_grid_number': self.latitude_grid_number,
                                   'longitude_grid_number': self.longitude_grid_number,
                                   'isoline_space': self.isoline_space, 'isoline_start_value': self.isoline_start_value,
                                   'isoline_end_value': self.isoline_end_value, 'smooth_factor': self.smooth_factor,
                                   'bold_line': self.bold_line}
                del data_raw

                # 为一些常用字段起简单易用的别名，以x，y，row，col来标记相对更容易理解
                self.x_start = self.start_longitude  # 起始x，即起始经度
                self.x_end = self.end_longitude  # 终止x，即终止经度
                self.dx = self.longitude_grid_space  # 经度（x方向）格距, 一般为正
                self.y_start = self.start_latitude  # 起始y，即起始纬度
                self.y_end = self.end_latitude  # 终止y，即终止纬度
                self.dy = self.latitude_grid_space  # 纬度（y方向）格距，有正负号
                self.cols = self.latitude_grid_number  # 列数，即纬向(x方向)格点数目
                self.rows = self.longitude_grid_number  # 行数，即经向(y方向)格点数目

        elif data_parameters is not None and isinstance(data_frame, (np.ndarray, list)):

            self.data = data_frame if isinstance(data_frame, list) else data_frame.flatten().tolist()
            self.parameters = data_parameters
            for key in data_parameters:
                exec('self.' + key + '=' + repr(data_parameters[key]))

        else:
            raise Exception('input parameters error!')

    def __getitem__(self, index):
        if isinstance(index, tuple) and len(index) == 2:
            return self.value(index[0], index[1])
        else:
            raise Exception('indexing error!')

    def __sub__(self, other):
        if self.rows == other.rows and self.cols == other.cols:
            return [x - y for x, y in zip(self.data, other.data)]

    def value(self, row, col):
        '''将格点数据看成self.latitude_grid_number*self.nums_lat的二维数组，返回第row行，第col列的值，
        row和col必须为整数，从0开始计数，坐标原点在左上角'''
        if row < 0 or row >= self.rows or col < 0 or col >= self.cols:
            raise Exception('out of data spatial range')
        return self.data[row * self.cols + col]

    def IDW(self, lon_lat_s, power=2):
        """
        反距离加权法提取站点数据
        :param lon_lat_s: 以[(lon1,lat1）,(lon2,lat2),……]形式传入的一系列站点位置,经纬度必须是弧度形式
        :param power:
        :return: 对应站点位置的插值结果列表
        """
        extracted_values = []
        for lon, lat in lon_lat_s:
            # 根据目标位置经纬度计算其周围四个格点在二维数组中的起始和终止行列号
            col_beg = int(fabs((lon - self.x_start) / self.dx))
            row_beg = int(fabs((lat - self.y_start) / self.dy))
            col_end = col_beg + 1
            row_end = row_beg + 1

            # 计算包围目标位置的经纬度范围,即起始和终止行列号的对应经纬度，行号与纬度对应，列号与经度对应
            lon_beg = self.x_start + self.dx * col_beg
            end_longitude = self.x_start + self.dx * col_end
            lat_beg = self.y_start + self.dy * row_beg
            end_latitude = self.y_start + self.dy * row_end

            # 根据目标位置与周围四个格点的经纬度距离计算权重
            w1 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w2 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (end_latitude - lat) ** 2)) ** power
            w3 = 1.0 / (sqrt((end_longitude - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w4 = 1.0 / (sqrt((end_longitude - lon) ** 2 + (end_latitude - lat) ** 2)) ** power

            # 目标位置周围四个格点的值
            d1 = self.value(row_beg, col_beg)
            d2 = self.value(row_end, col_beg)
            d3 = self.value(row_beg, col_end)
            d4 = self.value(row_end, col_end)

            # 根据反距离加权计算最终值，注意权重与格点要一一对应
            z = (d1 * w1 + d2 * w2 + d3 * w3 + d4 * w4) / (w1 + w2 + w3 + w4)

            extracted_values.append(z)

        return extracted_values

    def to_esri_ascii(self, out_name):
        with open(out_name, 'w') as f:
            y_start = self.y_end if self.dy < 0 else self.y_start
            header = 'NCOLS %d\nNROWS %d\nXLLCENTER %f\nYLLCENTER %f\nCELLSIZE %f\nNODATA_VALUE 9999.0\n' % (
                self.cols, self.rows, self.x_start, y_start, self.dx)
            f.write(header)

            if self.dy < 0:
                f.write(' '.join(map(str, self.data)))
            else:
                for i in range(self.rows - 1, -1, -1):
                    f.write(' '.join(
                        map(str, self.data[i * self.cols:(i + 1) * self.cols])))
                    f.write('\n')  # 必须加换行符，因为' '.join最后还多了一个空格，arcgis不能根据列数自动计算
        try:
            import arcpy
        except ImportError:
            print("""warning: you have no Esri's arcpy module, using to_esri_ascii method,
                    you can still get the result, but without the associate coordinate information.
                    you can use Esri's software like Arcmap to import the result and add the coordinate which is
                    WGS1984""")
        else:
            # 定义坐标系//define the coordinate
            sr = arcpy.SpatialReference('WGS 1984')
            arcpy.DefineProjection_management(out_name, sr)

    def to_file(self, out_name, formatted=False):
        with open(out_name, 'w') as f:
            f.write('diamond 4 ' + self.doc + '\n')
            f.write(' '.join([self.year, self.month, self.day, self.hour, self.period, self.level, '\n']))

            f.write(
                ' '.join(
                      ['%.2f' % i for i in [self.dx, self.dy, self.x_start, self.x_end, self.y_start, self.y_end]]
                    + ['%d' % i for i in [self.cols, self.rows]]
                    + ['%.2f' % i for i in [self.isoline_space, self.isoline_start_value,
                                            self.isoline_end_value, self.smooth_factor,self.bold_line]] +['\n']
                    )
            )

            if not formatted:
                f.write(' '.join(['%.2f' % i for i in self.data]))

    def to_numpy(self):
        return np.array(self.data).reshape(self.rows, self.cols)

    def max(self):
        return max(self.data)

    def min(self):
        return min(self.data)

    def calc_stats(self):
        pass

    def extract_station_value(self, lon_lat_s, method):
        '提取站点数据'
        pass


if __name__ == "__main__":
    # d0 = Diamond4('D:/000')
    # d1 = Diamond4('D:/024')
    # d1_0 = Diamond4(d1-d0, d1.parameters)
    # d1_0.isoline_start_value = floor(d1_0.min())
    # d1_0.isoline_end_value = ceil(d1_0.max())
    # d1_0.to_file('D:/d1_0.txt')
    #d = Grid('D:/Desktop/18042720.003')
    #d = Grid(r'Y:\ECMWF_HR\2T\999\18042720.000')
    d = Grid('ECMWF_HR/TMP_2M/18043008.000')
    print(d.description,
          d.dx,
          d.dy,
          d.cols,
          d.rows,
          d[0,0])
