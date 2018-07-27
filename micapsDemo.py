# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

import pandas as pd
from collections import namedtuple
import time
from micaps import Grid


if __name__ == "__main__":
    start = time.clock()
    # ***********************测试程序*********************************"

    Station = namedtuple('Station',['name', 'id', 'lon_lat'])
    stations = (
            (u'封丘', '53983', (114.4166667, 35.03333333)),
            (u'辉县', '53985', (113.8166667, 35.45)),
            (u'新乡', '53986', (113.8833333, 35.31666667)),
            (u'获嘉', '53988', (113.6666667, 35.26666667)),

            (u'原阳', '53989', (113.95, 35.05)),
            (u'卫辉', '53994', (114.0666667, 35.38333333)),
            (u'延津', '53997', (114.1833333, 35.15)),
            (u'长垣', '53998', (114.6666667,35.2)),
    )
    stations = [Station._make(i) for i in stations]

    # d0 = Diamond4('D:/000')
    # d1 = Diamond4('D:/024')
    # d1_0 = Diamond4(d1-d0, d1.parameters)
    # d1_0.isoline_start_value = floor(d1_0.min())
    # d1_0.isoline_end_value = ceil(d1_0.max())
    # d1_0.to_file('D:/d1_0.txt')
    # d = Grid('D:/Desktop/18042720.003')
    #data_source = ['Y:/GRAPES_MESO/T2M_4']
    # d = Grid(r'Y:\ECMWF_HR\2T\999\18050620.000')
    # d = Grid('ECMWF_HR/TMP_2M/18043008.012')
    data_source = ['GRAPES_MESO/T2M_4/18052720.012']#'Y:/ECMWF_HR/2T/999',
    #for i in get_file_list('18050708', data_source):
    #    print(Grid(i).timezone)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width',1000)
    pd.set_option('display.float_format', lambda x: '%7.1f'%x)



    # lon_lat_s = [i.lon_lat for i in stations]
    # d1 = Grid('ECMWF_HR/TMP_2M/18051008.030')
    # r1 = d1.nearest_neighbor(lon_lat_s)
    # d2 = Grid('ECMWF_HR/TMP_2M/18051008.033')
    # r2 = d2.nearest_neighbor(lon_lat_s)
    #
    # r = []
    # for i,j in zip(r1,r2):
    #     if i>j:
    #         r.append(i)
    #     else:
    #         r.append(j)
    #
    # for i, j in zip(stations, r):
    #     print(i.name,': ', '%7.1f'%j)


    d = Grid('Y:\\GRAPES_MESO\\T2M_4\\18052808.066')
    #d = Grid(r'Y:\GRAPES_GFS\GLW_4\18052520.000')
    #d = Grid('ECMWF_HR/TMP_2M/18052708.000')
    #d = Grid('GRAPES_MESO_HR/TMP/2M_ABOVE_GROUND/18052708.012')
    print(d.model_name)
    print(d.description)
    print(d.period)

     # ***********************测试程序*********************************"
    end = time.clock()
    elapsed = end - start
    print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))