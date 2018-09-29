# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from manager.manager import *
from datetime import datetime

if __name__ == '__main__':
    # print(latest_start_predict_time())
    # print(len(list(parse_time_resolution("24 72 3,78 240 6"))))
    # for i in parse_time_resolution("24 72 3,78 240 6"):
    #     print(i)
    print(mdfs2mds("ECMWF_HR/10_METRE_WIND_GUST_IN_THE_LAST_3_HOURS"))