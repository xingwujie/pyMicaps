# -*- coding:utf-8 -*-

import datetime
from math import sqrt, fabs
import os
import numpy as np


class Diamond4(object):
    """

    """
    diamond = 4

    def __init__(self, data_frame, data_parameters=None):

        if data_parameters is None and os.path.isfile(data_frame):
            with open(data_frame, 'r') as f:
                data_raw = [word for line in f.readlines() if line[:-1].strip()
                            for word in line.split()]  # 去除空行读入,将原文件分割成一维字符串数组

                self.doc = data_raw[2]#.decode('gbk')  # 说明字符串

                # 日期时间处理
                (year,  # 年
                 self.month,  # 月
                 self.day,  # 日
                 self.hour,  # 时
                 self.valid_period,  # 时效
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
                self.valid_time = self.start_time + datetime.timedelta(hours=int(self.valid_period))

                (self.size_lon,  # 经度（x方向）格距, 一般为正
                 self.size_lat,  # 纬度（y方向）格距，有正负号
                 self.lon_start,  # 起始经度
                 self.lon_end,  # 终止经度
                 self.lat_start,  # 起始纬度
                 self.lat_end) = (float(i) for i in data_raw[9:15])  # 终止纬度

                (self.cols,  # 纬向(x方向)格点数目，即列数
                 self.rows) = (int(i) for i in data_raw[15:17])  # 经向(y方向)格点数目，即行数

                (self.contour_interval,  # 等值线间隔
                 self.contour_start,  # 等值线起始值
                 self.contour_end,  # 等值线终止值
                 self.smooth,  # 平滑系数
                 self.bold_line) = (float(i) for i in data_raw[17:22])  # 加粗线值

                # 数据部分，以一维数组表示
                self.data = [float(i) for i in data_raw[22:]]

                # 将数据的一些属性参数集合到一个字典中
                self.parameters = {'doc': self.doc, 'year': self.year, 'month': self.month, 'day': self.day,
                                   'hour': self.hour,'valid_period': self.valid_period,'start_time': self.start_time,
                                   'valid_time': self.valid_time,'size_lon': self.size_lon, 'size_lat': self.size_lat,
                                   'lon_start': self.lon_start, 'lon_end': self.lon_end, 'lat_start': self.lat_start,
                                   'lat_end': self.lat_end, 'cols': self.cols, 'rows': self.rows,
                                   'contour_interval': self.contour_interval, 'contour_start': self.contour_start,
                                   'contour_end': self.contour_end, 'smooth': self.smooth, 'bold_line': self.bold_line}
                del data_raw

        elif data_parameters is not None and isinstance(data_frame, (np.ndarray, list)):

            self.data = data_frame if isinstance(data_frame, list) else data_frame.flatten().tolist()
            self.parameters = data_parameters
            for key in data_parameters:
                exec('self.'+ key + '=' + repr(data_parameters[key]))

        else:
            raise Exception('input parameters error!')

    def __getitem__(self, index):
        if isinstance(index, tuple) and len(index) == 2:
            return self.value(index[0], index[1])
        else:
            raise Exception('indexing error!')

    def __sub__(self, other):
        if self.rows == other.rows and self.cols == self.cols:
            return [x - y for x, y in zip(self.data, other.data)]


    def value(self, row, col):
        '''将格点数据看成self.cols*self.nums_lat的二维数组，返回第row行，第col列的值，
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
            col_beg = int(fabs((lon - self.lon_start) / self.size_lon))
            row_beg = int(fabs((lat - self.lat_start) / self.size_lat))
            col_end = col_beg + 1
            row_end = row_beg + 1

            # 计算包围目标位置的经纬度范围,即起始和终止行列号的对应经纬度，行号与纬度对应，列号与经度对应
            lon_beg = self.lon_start + self.size_lon * col_beg
            lon_end = self.lon_start + self.size_lon * col_end
            lat_beg = self.lat_start + self.size_lat * row_beg
            lat_end = self.lat_start + self.size_lat * row_end

            # 根据目标位置与周围四个格点的经纬度距离计算权重
            w1 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w2 = 1.0 / (sqrt((lon_beg - lon) ** 2 + (lat_end - lat) ** 2)) ** power
            w3 = 1.0 / (sqrt((lon_end - lon) ** 2 + (lat_beg - lat) ** 2)) ** power
            w4 = 1.0 / (sqrt((lon_end - lon) ** 2 + (lat_end - lat) ** 2)) ** power

            # 目标位置周围四个格点的值
            d1 = self.value(row_beg, col_beg)
            d2 = self.value(row_end, col_beg)
            d3 = self.value(row_beg, col_end)
            d4 = self.value(row_end, col_end)

            # 根据反距离加权计算最终值，注意权重与格点要一一对应
            z = (d1 * w1 + d2 * w2 + d3 * w3 + d4 * w4) / (w1 + w2 + w3 + w4)

            extracted_values.append(z)

        return extracted_values

    def extract_station_value(self, lon_lat_s, method):
        '提取站点数据'
        pass

    def to_esri_ascii(self, out_name):
        with open(out_name, 'w') as f:
            y_start = self.lat_end if self.size_lat < 0  else self.lat_start
            header = 'NCOLS %d\nNROWS %d\nXLLCENTER %f\nYLLCENTER %f\nCELLSIZE %f\nNODATA_VALUE 9999.0\n' % (
                self.cols, self.rows, self.lon_start, y_start, self.size_lon)
            f.write(header)

            if self.size_lat < 0:
                f.write(' '.join(map(str, self.data)))
            else:
                for i in xrange(self.rows - 1, -1, -1):
                    f.write(' '.join(map(str, self.data[i * self.cols:(i + 1) * self.cols])))
                    f.write('\n')  # 必须加换行符，因为' '.join最后还多了一个空格，arcgis不能根据列数自动计算
        try:
            import arcpy
        except ImportError:
            print("""warning: you have no Esri's arcpy module, using to_esri_ascii function,
                    you can still get the result, but without the associate coordinate information.
                    you can use Esri's software like Arcmap to import the result and add the coordinate which is
                    WGS1984""")
        else:
            # 定义坐标系//define the coordinate
            sr = arcpy.SpatialReference('WGS 1984')
            arcpy.DefineProjection_management(out_name, sr)

    def write_to_diamond4_txt(self, out_name):
        # with open(out_name) as f:
        #     # year = self.start_time.strftime('%y')
        #     # month =
        #     f.write('diamond4 ' + self.doc + '\n')
        #     f.write(' '.join([self.]))
        pass

    def calc_stats(self):
        pass

    def to_numpy(self):
        return np.array(self.data).reshape(self.rows, self.cols)


if __name__ == "__main__":
    d = Diamond4('D:/000')
    print(d.doc)
    d.doc = '换句话'
    print(d.parameters['doc'])
    #todo 属性改变需要引起self.parameter改变
    print(d.doc)