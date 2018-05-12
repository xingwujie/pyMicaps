import sys, os, urllib, socket
import traceback
import urllib.request
import  configparser
import Ice, cma.cimiss


#获取CIMISS地址信息和账号信息
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR,'config/config.ini'))
ip = config['CIMISS_MUSIC']['ip']
port = config['CIMISS_MUSIC'].getint('port')
user_id = config['CIMISS_MUSIC']['user_id']
password = config['CIMISS_MUSIC']['password']


class DataQuery(Ice.Application):
    '''
    encapsulate ice interfaces
    '''

    def __init__(self, serverIp=ip, serverPort=port, userId=user_id, pwd=password, configFile="client.config"):
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

    pass