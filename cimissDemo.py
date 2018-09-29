# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from cma.cimiss import DataQuery
import json
import pandas as pd


if __name__ == '__main__':
    # 定义client对象,指定数据服务ip和port
    # client = DataQuery(serverIp="10.96.89.55", serverPort=1888)
    # 定义client对象,默认使用client.config指定服务连接配置,需根据自己的地址进行修改
    client = DataQuery()



    # 调用接口
    # 1.获取数据返回结构体/类
    # retArray2D = client.callAPI_to_array2D(userName, password, interfaceId, params)

    # 2.获取数据返回序列化字符串
    # dataFormat 序列化的数据格式，可取：xml、json、csv、text、spaceText、commaText、tabText等。
    #            其中，spaceText、commaText、tabText表示保存为文本，记录间换行，要素值间分别用空格、逗号和TAB分割；text同spaceText。
    # retArray2D = client.callAPI_to_serializedStr(userName, password, interfaceId, params, dataFormat="csv")

    # 3.获取数据写入本地文件
    # dataFormat 序列化的数据格式，可取：xml、json、csv、text、spaceText、commaText、tabText等。
    #            其中，spaceText、commaText、tabText表示保存为文本，记录间换行，要素值间分别用空格、逗号和TAB分割；text同spaceText。
    # savePath 保持的本地文件路径（全路径，含文件名）

    # 接口ID
    interface_id = "getSurfEleInRegionByTime"

    # 参数列表
    params = {'dataCode': 'SURF_CHN_MUL_HOR_N', #资料代码（单个）
              'times': '20180508000000' ,  # 时间
              'adminCodes': '410700', # 国内行政编码
              'elements': 'Station_Name,Station_Id_C,TEM_Max_24h,TEM_Min_24h', #要素字段代码；统计接口分组字段
              }


    result = client.callAPI_to_serializedStr(interface_id, params, dataFormat='json')
    t = json.loads(result)
    p = {i['Station_Name']:[i['TEM_Min_24h'], i['TEM_Max_24h']]for i in t['DS']}
    print(pd.DataFrame(p))

    client.destroy()
