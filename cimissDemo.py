import datetime
from cma.cimiss.DataQueryClient import DataQuery

if __name__ == '__main__':
    # 定义client对象,指定数据服务ip和port
    # client = DataQuery(serverIp="10.96.89.55", serverPort=1888)
    # 定义client对象,默认使用client.config指定服务连接配置,需根据自己的地址进行修改
    client = DataQuery()

    # 接口ID
    interfaceId = "getSurfEleInRegionByTime"

    # 当前时间减一天
    now_time = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime('%Y%m%d%H') + "0000"
    print('now_time:', now_time)

    # 接口参数
    params = {'dataCode': "SURF_CHN_MUL_HOR_N",
              'elements': "Station_ID_C,PRE_12h",
              'times': now_time,
              'adminCodes': "230100",
              'orderby': "Station_ID_C:ASC",
              'limitCnt': "100000"}

    retArray2D = ""

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

    # 保存文件的路径及文件名
    _saveDir_ = '.\data'
    # 一种字符串拼接方法
    _saveFile_ = '%s.txt' % now_time
    # 又一种字符串拼接方法
    _savePath_ = _saveDir_ + _saveFile_
    print('savePath:', _savePath_)

    result = client.callAPI_to_saveAsFile(interfaceId, params, dataFormat='csv', savePath=_savePath_)
    # 输出结果预览
    print(retArray2D)

    interfaceId = "getNafpEleGridInRectByTimeAndLevelAndValidtime"

    # 2.3  接口参数，多个参数间无顺序
    # 必选参数    (1)资料:T639高精度格点产品(东北半球); (2)起报时间; (3)起始、终止预报时效;
    #          (4)经纬度点,北京(纬度39.8，经度116.4667)、上海(纬度31.2,经度121.4333);
    #          (5)预报要素（单个):气温; (6)预报层次(单个):1000hpa。
    params = {'dataCode': "NAFP_FOR_FTM_HIGH_T639_NEHE",
              'time': "20180308000000",
              'validTime': "3",
              'minLat': "39", 'maxLat': "42",
              'minLon': "115", 'maxLon': "117",
              'fcstEle': "TPE", 'fcstLevel': "0"}

    # 3. 调用接口
    result = client.callAPI_to_gridArray2D(interfaceId, params)

    # 4. 输出接口
    print(result)

    # 销毁client对象
    client.destroy()
