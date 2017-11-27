# -*- coding:utf-8 -*-

from datetime import datetime, timedelta
from math import sqrt, fabs


class Diamond4(object):
    """

    """
    diamond = 4

    def __init__(self, file_path):
        with open(file_path, 'r') as f:
            data_raw = [word for line in f.readlines() if line[:-1].strip()
                        for word in line.split()]  # 去除空行读入,将原文件分割成一维字符串数组

            self.doc = data_raw[2].decode('gbk')  # 说明字符串

            (self.size_lon,  # 经度（x方向）格距, 一般为正
             self.size_lat,  # 纬度（y方向）格距，有正负号
             self.lon_start,  # 起始经度
             self.lon_end,  # 终止经度
             self.lat_start,  # 起始纬度
             self.lat_end) = (float(i) for i in data_raw[9:15])  # 终止纬度

            (self.cols,  # 纬向(x方向)格点数目，即列数
             self.rows) = (int(i) for i in data_raw[15:17])  # 经向(y方向)格点数目，即行数

            # 日期时间处理
            (month, day, hour, interval) = data_raw[4:8]
            year = data_raw[3]
            if len(year) == 2:
                year = ('20' + year) if int(year) < 49 else ('19' + year)
            elif len(year) == 4:
                pass
            else:
                raise Exception('year parameter error!')

            # 注意start_time和valid_time没有统一规定，要看具体情况
            self.start_time = datetime(int(year), int(month), int(day), int(hour))
            self.valid_time = self.start_time + timedelta(hours=int(interval))

            # 数据部分，以一维数组表示
            self.data = [float(i) for i in data_raw[22:]]

            del data_raw

    def __getitem__(self, index):
        if isinstance(index, tuple) and len(index) == 2:
            return self.value(index[0], index[1])
        else:
            raise Exception('indexing error!')

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

    def convert_to_EsriAscii(self, out_name):
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
            print("""warning: you have no Esri's arcpy module, using convert_to_EsriAsii function,
                    you can still get the result, but without the associate coordinate information.
                    you can use Esri's software like Arcmap to import the result and add the coordinate which is
                    WGS1984""")
        else:
            # 定义坐标系//define the coordinate
            sr = arcpy.SpatialReference('WGS 1984')
            arcpy.DefineProjection_management(out_name, sr)

    def write_to_diamond4_txt(self, out_name):
        pass

    def calc_stats(self):
        pass

    def to_numpy(self):
        try:
            import numpy as np
        except ImportError:
            'you have no numpy module'
        else:
            return np.array(self.data).reshape(self.rows, self.cols)

if __name__ == "__main__":
    d = Diamond4('D:/000')
