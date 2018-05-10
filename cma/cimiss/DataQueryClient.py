import sys, os, urllib, socket
import traceback
import urllib.request

import Ice, cma.cimiss


class DataQuery(Ice.Application):
    '''
    encapsulate ice interfaces
    '''

    def __init__(self, serverIp="10.69.89.55", serverPort=1888,
                       userId="BEZZ_BFXX_XXOBS", pwd="Micaps53986", configFile="client.config"):
        '''
        Constructor
        '''
        super(DataQuery, self).__init__()


        iceFilePath = os.path.dirname(cma.cimiss.__file__) + os.sep + "apiinterface.ice"
        Ice.loadSlice(iceFilePath)
        initData = Ice.InitializationData()
        config = ''
        if sys.argv.__contains__('configFile'):
            if not os.path.exists(configFile):
                raise RuntimeError("config file is not exist.")
            else:
                config = configFile
        else:
            defaultConfig = "client.config"
            if os.path.exists(defaultConfig):
                config = defaultConfig

        initData.properties = Ice.createProperties()

        if config != "":
            initData.properties.load(config)
            ic = Ice.initialize(initData)
            base = ic.stringToProxy(initData.properties.getProperty("DataApi.Proxy"))
        else:
            initData.properties.setProperty("Ice.Warn.Connections", "1")
            initData.properties.setProperty("Ice.Trace.Protocol", "0")
            initData.properties.setProperty("Ice.MessageSizeMax", "20971520")
            initData.properties.setProperty("Ice.ThreadPool.Client.Size", "10")
            initData.properties.setProperty("Ice.Default.EncodingVersion", "1.0")
            ic = Ice.initialize(initData)
            base = ic.stringToProxy("DataApi:tcp -h " + serverIp + " -p " + str(serverPort))

        self.userId = userId
        self.pwd = pwd
        self.clientIp = socket.gethostbyname(socket.gethostname())
        self.language = "python"
        self.version = "1.3"
        self.ic = ic


        ok = False
        count = 0
        while not ok and count <= 5:
            try:
                self.api = cma.cimiss.DataAPIAccessPrx.checkedCast(base)
                if not self.api:
                    count = count + 1
                    ok = False
                    print("Ice UnChecked, Retry..{%s}" % count)
                    raise RuntimeError("Invalid proxy")
                else:
                    ok = True
                    #print("Ice Checked!")
            except:
                count = count + 1
                ok = False
                print("Ice UnChecked, Retry..{%s}" % count)

        if not ok and count > 5:
            print("Ice Server is Invalid!")

    def destroy(self):
        if self.ic:
            #print("Ice Destory!")
            try:
                self.ic.destroy()
            except Exception as e:
                traceback.print_exc()
                status = 1

    def callAPI_to_array2D(self, interfaceId, params):
        if self.api:
            retArray2D = self.api.callAPItoarray2D(self.userId, self.pwd,
                                                   interfaceId, self.clientIp, self.language, self.version,
                                                   params)
            return retArray2D

    def callAPI_to_gridArray2D(self, interfaceId, params):
        if self.api:
            retGridArray2D = self.api.callAPItogridArray2D(self.userId, self.pwd,
                                                           interfaceId, self.clientIp, self.language, self.version,
                                                           params)
            return retGridArray2D

    def callAPI_to_saveAsFile(self, interfaceId, params, dataFormat, savePath):
        if self.api:
            result = self.api.callAPItosaveAsFile(self.userId, self.pwd,
                                                  interfaceId, self.clientIp, self.language, self.version,
                                                  params, dataFormat, savePath)
            if (result):
                if (result.request.errorCode == 0):
                    urllib.request.urlretrieve(result.fileInfos[0].fileUrl, savePath)
            return result

    def callAPI_to_fileList(self, interfaceId, params):
        if self.api:
            result = self.api.callAPItofileList(self.userId, self.pwd,
                                                interfaceId, self.clientIp, self.language, self.version,
                                                params)
            return result

    def callAPI_to_downFile(self, interfaceId, params, fileDir):
        fileDir = fileDir if fileDir.endswith(os.sep) else fileDir + os.sep
        if self.api:
            result = self.api.callAPItofileList(self.userId, self.pwd,
                                                interfaceId, self.clientIp, self.language, self.version,
                                                params)
            if (result):
                if (result.request.errorCode == 0):
                    for info in result.fileInfos:
                        urllib.urlretrieve(info.fileUrl, fileDir + info.fileName)
                return result

    def callAPI_to_serializedStr(self, interfaceId, params, dataFormat):
        if self.api:
            result = self.api.callAPItoserializedStr(self.userId, self.pwd,
                                                     interfaceId, self.clientIp, self.language, self.version,
                                                     params, dataFormat)
            return result

    # store
    def callAPI_to_storeArray2D(self, interfaceId, params, inArray2D):
        if self.api:
            result = self.api.callAPItostoreArray2D(self.userId, self.pwd,
                                                    interfaceId, self.clientIp, self.language, self.version,
                                                    params, inArray2D)
            return result

    def callAPI_to_storeFileByFtp(self, interfaceId, params, inArray2D, ftpfiles):
        if self.api:
            result = self.api.callAPItostoreFileByFtp(self.userId, self.pwd,
                                                      interfaceId, self.clientIp, self.language, self.version,
                                                      params, inArray2D, ftpfiles)
            return result

    def callAPI_to_storeSerializedStr(self, interfaceId, params, inString):
        if self.api:
            result = self.api.callAPItostoreSerializedStr(self.userId, self.pwd,
                                                          interfaceId, self.clientIp, self.language, self.version,
                                                          params, inString)
            return result


if __name__ == "__main__":
    client = DataQuery()
    #    retArray2D = client.callAPI_to_array2D("user_nordb", "user_nordb_pwd1", "getSurfEleByTime",\
    #                                    {'dataCode':"SURF_CHN_MUL_HOR",\
    #                                     'elements':"Station_ID_C,PRE_1h,PRS,RHU,VIS,WIN_S_Avg_2mi,WIN_D_Avg_2mi,Q_PRS",\
    #                                     'times':"20141224000000",\
    #                                     'orderby': "Station_ID_C:ASC",\
    #                                     'limitCnt': "20000" })
    #    print retArray2D

    #    retGridArray2D = client.callAPI_to_gridArray2D("user_nordb", "user_nordb_pwd1", "getNafpTimeSerialByPoint",\
    #                                    {'dataCode':'NAFP_T639_FOR_FTM_LOW_NEHE',\
    #                                     'fcstEle':'TEM',\
    #                                     'time':'20141017000000',\
    #                                     'fcstLevel': '1000',\
    #                                     'latLons':'0/0,10/10',\
    #                                     'minVT' : '0',\
    #                                     'maxVT' : '240'})
    #    print retGridArray2D

    #    result = client.callAPI_to_saveAsFile("user_nordb","user_nordb_pwd1", "getSurfEleByTime",\
    #                                    {'dataCode':"SURF_CHN_MUL_HOR",\
    #                                     'elements':"Station_ID_C,PRE_1h,PRS,RHU,VIS,WIN_S_Avg_2mi,WIN_D_Avg_2mi,Q_PRS",\
    #                                     'times':"20141224000000",\
    #                                     'orderby': "Station_ID_C:ASC",\
    #                                     'limitCnt': "20000" },'json','data.txt')
    #    print result

    #    result = client.callAPI_to_fileList("user_nordb", "user_nordb_pwd1", "getSevpFileByTime",\
    #                                    {'dataCode':"SEVP_WEFC_PRE_LOCT",\
    #                                     'times':"20141210000000,20141210010000"})
    #    print result

    #    result = client.callAPI_to_downFile("user_nordb", "user_nordb_pwd1", "getSevpFileByTime",\
    #                                    {'dataCode':"SEVP_WEFC_PRE_LOCT",\
    #                                     'times':"20141220000000,20141210010000"}, "./")
    #    print result


    # store
    result = client.callAPI_to_storeArray2D("user_nordb", "user_nordb_pwd1", "deleteStationData", \
                                            {'dataCode': "SEVP_WEFC_ACPP_STORE", \
                                             'KeyEles': "Datetime,Station_Id_C"}, \
                                            [['20150114060000', '54323']])
    print(result)

    # python二维字符数组例子
    inArray2D1 = [
        ['54511', '2015', '09', '21', '10', '999.8', '1056', '56', '20150921100000'], \
        ['54512', '2015', '09', '21', '10', '999.8', '1056', '44', '20150921100000'], \
        ['54513', '2015', '09', '21', '10', '999.8', '1056', '77', '20150921100000']
    ]
    result = client.callAPI_to_storeSerializedStr("user_nordb", "user_nordb_pwd1", "deleteStationData", \
                                                  {'dataCode': "SEVP_WEFC_ACPP_STORE", \
                                                   'KeyEles': "Datetime,Station_Id_C"}, \
                                                  "20150114060000,54323;20150114060000,54326")
    print(result)
