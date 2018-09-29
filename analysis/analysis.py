# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from micaps import Grid
from datetime import datetime, timedelta
import os, time, json
from collections import namedtuple
import pandas as pd
from cma.cimiss import DataQuery
from cma.gds import GDSDataService


def get_file_list(start_time, data_source):
    """
    获取data_source下所有以start_time为起报时的文件，data_source可以包含多个不同类型的数据源列表，
    start_time: 起报时间
    data_source: 数据源列表（也可是元组形式），其中每项必须是包含数据的最终目录，
        可以包含不同种类的数据源，可以是本地数据，如m3文件，mdfs文件，也可以是上述两种混合的
        也可以是gds类型的数据目录。如果数据源中包含同名文件，优先选取在data_source中靠前的数据源
    :return:
    """
    # todo 考虑改写成生成器形式

    file_list = []
    for dir in data_source:
        if os.path.isabs(dir):
            files = os.listdir(dir)
            file_list.extend(
                [os.path.join(dir, f) for f in files
                 if f.startswith(start_time) and f not in [os.path.basename(each) for each in file_list]]
            )  # 前面目录中的模式产品文件优先
        else:
            with GDSDataService() as gds:
                files = list(gds.get_file_list(dir, start_time))
                file_list.extend(
                    [os.path.join(dir, f) for f in files
                     if f not in [os.path.basename(each) for each in file_list]]
                )
    return file_list


def search_files_by_valid_time(valid_time, data_source, condition=''):
    pass


def extract_time_series(start_time, data_source, stations, interpolation='IDW', save=False):
    """
    start_time: 起报时间,必须以'yymmddhh'的形式给出
    param station: 插值站点位置
    data_source: 数据源列表，列表中的每项必须是包含数据的最终目录，
        可以包含不同种类的数据源，可以是本地数据，如m3文件，mdfs文件，也可以是上述两种混合的
        也可以是gds类型的数据目录
    interpolation: 插值方法
    return: 一个pandas的dataframe, 索引是时间，列是根据stations中站点信息提取的各个站点上的时间序列值，以站名为列名
    """

    records = []
    lon_lat_s = [i.lon_lat for i in stations]
    file_list = get_file_list(start_time, data_source)
    for f_path in file_list:
        data = Grid(f_path)
        record = data.grid_to_station(lon_lat_s, interpolation)
        record.append(data.valid_time)
        records.append(record)

    series = pd.DataFrame.from_records(records, index=len(stations))
    series.columns = [i.name for i in stations]
    series.index.name = 'valid_time'
    series = series.sort_index()

    if save:
        model_name = data.model_name
        element = data.element
        save_path = '-'.join(['data/series',start_time, model_name, element, interpolation])
        series.to_pickle(save_path)

    return series


def calculate_span_series(series_data, start_hour, span_hours, stat_option='min max', skip_incomplete_span=False):
    """
    在时间序列数据基础上计算固定时间间隔上的统计信息
    series_data: 时间序列数据
    start_hour: 时间间隔的开始时刻，范围是[0: 23]
    span_hours: 时间间隔的小时数,
    stats_method: 时间间隔的统计方法, 不同方法之间以空格分割
    skip_incomplete_span:是否跳过不完整的时间间隔
    return:返回的是一个列表，列表中的每一项是一个pandas的df，表示一个时间间隔上的各站点的统计信息，统计信息与stat_option参数对应，
    默认这个df有两行，第一行时间间隔内的是最低值，第二行是最高值，列数跟statons的个数相同，列名即站名，df有一个额外的列表类型的
    time_span属性，表示时间间隔的起始、终止值
    """
    # 统计方法字典
    stat_methods = {'max': pd.DataFrame.max,
                    'min': pd.DataFrame.min,
                    'mean': pd.DataFrame.mean,
                    'median': pd.DataFrame.median,
                    'std': pd.DataFrame.std,
                    'var': pd.DataFrame.var,
                     }

    start_time = series_data.index[0]   # 时间序列的开始时间

    t0 = datetime(start_time.year, start_time.month, start_time.day, start_hour)  #时间间隔初始起始时间

    span_list = []  # 存储最终结果的列表
    while t0 < series_data.index[-1]:           # 当时间间隔的起始时间小于时间序列数据的最后一个时间，则循环计算
        t1 = t0 + timedelta(hours=span_hours)   # 时间间隔的终止时间

        span_data = series_data[t0: t1]         # 时间序列数据上一个时间间隔内的数据

        # 对时间间隔数据分别做不同方法的统计
        stats_list = []                         # 临时存储单个统计信息的列表
        for method in stat_option.split():
            r = stat_methods[method](span_data)
            stats_list.append(r)

        span_stats = pd.DataFrame(stats_list)   # 将所有统计组合到一个dataframe中

        # 为这个时间间隔的统计增加时间段信息,注意不能赋予t0,t1  todo 给df添加额外属性引起警告，考虑另一种方法
        span_stats.time_span = [span_data.index[0], span_data.index[-1]]

        span_list.append(span_stats)

        t0 = t1

    if skip_incomplete_span:  # 判断首尾的两个span的时间间隔是否等于span_hours参数，不等于说明时间间隔不完整，舍去这个span
        for i in [0, -1]:
            if int((span_list[i].time_span[1] - span_list[i].time_span[0]).seconds/3600) != span_hours:
                span_list.pop(i)

    return span_list


def max_min_real_temp_24h(end_time, admin_code='410700'):

    """
    获取各站点过去24小时的最高、最低气温，以行政区划为单元
    end_time: 终止时间
    admin_code: 行政区域代码，默认是新乡
    return: 一个pandas的dataframe，第一行是最低温度，第二行是最高温度，以站名为列名
    """
    if isinstance(end_time, str):
        end_time = (datetime.strptime(end_time, '%y%m%d%H')-timedelta(hours=8)).strftime('%Y%m%d%H%M%S')  # 转换成世界时
    elif isinstance(end_time, datetime):
        end_time = (end_time - timedelta(hours=8)).strftime('%Y%m%d%H%M%S')
    else:
        raise TypeError

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


def model_error_max_min_temp(now_time, max_min_temp_span_series):

    now_time = datetime.strptime(now_time, '%y%m%d%H')
    result = []
    for span in max_min_temp_span_series:
        t_end = span.time_span[1]
        if t_end <= now_time:
            real = max_min_real_temp_24h(t_end)
            #span = span.reset_index(drop=True)
            error = span - real
            error.time_span = span.time_span
            result.append(error)

    return result


if __name__ == "__main__":

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width',1000)
    pd.set_option('display.float_format', lambda x: '%7.1f'%x)

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
    data_source = ['Y:/ECMWF_HR/2T/999']

    # series = max_min_model_temp('18050920',data_source, stations, (20,24))
    #
    # r = model_error_max_min_temp('18051020', series)
    #
    # pd.set_option('display.float_format', lambda x: '%7.1f'%x)
    #
    # station_names = [s.name for s in stations]
    # r = [i[station_names] for i in r]
    # min = pd.concat([i[0:1] for i in r])
    # max = pd.concat([i[1:] for i in r])
    #
    # print('Tmin:\n', min)
    # print('Tmax:\n', max)


    # df1 = max_min_real_temp_24h('18050920')
    # df2 = max_min_real_temp_24h('18050820')
    # print(df1)
    # print(df2)
    # print(pd.concat([df1[0:1], df2[0:1]]))
    # print(pd.concat([df1[1:], df2[1:]]))


    #time_series = extract_time_series('18050820',data_source, stations) #save=True)
    #span_series = calculate_span_series(time_series, 20, 24, 'min max')
    #error = model_error_max_min_temp('18051020',span_series)
    station_names = [s.name for s in stations]
    r = max_min_real_temp_24h(datetime(2018,5,15,8))

    print(r[station_names])



    # ***********************测试程序*********************************"
    end = time.clock()
    elapsed = end - start
    print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))