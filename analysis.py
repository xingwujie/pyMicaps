# -*- coding:utf-8 -*-
# @author: ZHU Feng

from ios import Grid, extract_time_series
from datetime import datetime, timedelta
import time, json
from collections import namedtuple
import pandas as pd
from cma.cimiss.DataQueryClient import DataQuery


def max_min_model_temp(start_time, data_source, stations, time_span, interpolation='IDW', sql=None):
    """
    start_time:模式数据的起报时间
    data_source:
    stations:
    time_span:(start_hour, span_hours)以起始时刻和跨度小时数给出的统计间隔
    interpolation:
    sql:
    :return:返回的是一个列表，列表中的每一项是一个pandas的df，表示一个时间间隔上的各站点的最高、最低气温
    这个df有两行，第一行时间间隔内的是最低温度，第二行是最高温度，
    列数跟statons的个数相同，列名即站名，df的索引是时间间隔的起始、终止值
    """
    series_data = extract_time_series(start_time,data_source, stations)
    start_hour, span_hours = time_span
    start_time = datetime.strptime(start_time, '%y%m%d%H')

    delta_hours = start_hour-start_time.hour
    delta_hours = delta_hours if delta_hours > 0 else 0
    t0 = start_time + timedelta(hours = delta_hours)

    max_min_list = []
    while t0 < series_data.index[-1]:
        t1 = t0 + timedelta(hours=span_hours)

        span_data = series_data[t0: t1]

        min = span_data.min()
        max = span_data.max()
        df_max_min = pd.DataFrame([min,max])
        df_max_min.index=[t0,t1]

        max_min_list.append(df_max_min)

        t0 = t1

    return max_min_list


def max_min_real_temp_24h(end_time, admin_code='410700'):

    end_time = datetime.strptime(end_time, '%y%m%d%H')-timedelta(hours=8)
    end_time = datetime.strftime(end_time, '%Y%m%d%H%M%S')
    client = DataQuery()

    # 接口ID
    interface_id = "getSurfEleInRegionByTime"

    #参数列表
    params = {'dataCode': 'SURF_CHN_MUL_HOR_N', #资料代码（单个）
              'times': end_time,  # 时间
              'adminCodes': admin_code, # 国内行政编码
              'elements': 'Station_Name,Station_Id_C,TEM_Max_24h,TEM_Min_24h', #要素字段代码；统计接口分组字段
              }

    result_json = client.callAPI_to_serializedStr(interface_id, params, dataFormat='json')
    result_pyobj = json.loads(result_json)
    result_dict= {i['Station_Name']: [float(i['TEM_Min_24h']), float(i['TEM_Max_24h'])]
                  for i in result_pyobj['DS']}
    result = pd.DataFrame(result_dict)

    client.destroy()

    return result


def model_error_max_min_temp(now_time, model_max_min_list):

    now_time = datetime.strptime(now_time, '%y%m%d%H')
    result = []
    for span in model_max_min_list:

        if span.index[1] <= now_time:
            real = max_min_real_temp_24h(datetime.strftime(span.index[1], '%y%m%d%H'))
            span = span.reset_index(drop=True)
            result.append(span-real)

    return result


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
    data_source = ['ECMWF_HR/TMP_2M']

    series = max_min_model_temp('18050920',data_source, stations, (20,24))

    r = model_error_max_min_temp('18051020', series)

    pd.set_option('display.float_format', lambda x: '%7.1f'%x)

    station_names = [s.name for s in stations]
    r = [i[station_names] for i in r]
    min = pd.concat([i[0:1] for i in r])
    max = pd.concat([i[1:] for i in r])

    print('Tmin:\n', min)
    print('Tmax:\n', max)


    # df1 = max_min_real_temp_24h('18050920')
    # df2 = max_min_real_temp_24h('18050820')
    # print(df1)
    # print(df2)
    # print(pd.concat([df1[0:1], df2[0:1]]))
    # print(pd.concat([df1[1:], df2[1:]]))