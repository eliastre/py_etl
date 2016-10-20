# -*- coding:utf-8 -*- 
import bitools
import ftplib
import socket
import os
import json
import urllib
import httplib
import ConfigParser
import logging
import sys
import cPickle
import time
import platform
from bitools import getVariable

'''
@author: MUHE
'''
global cwd;
cwd = sys.path[0]
#global current;
#current = time.time()

class MyLogger:
    def __init__(self, loggerName):
        logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M',
                        filename=cwd + os.sep + 'export.log',
                        filemode='a')
        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger(loggerName).addHandler(console)
        self.logger = logging.getLogger(loggerName);
    def info(self, output):
        self.logger.info(MyLogger.parseString(output))
    def error(self, output):
        self.logger.error(MyLogger.parseString(output))
    def debug(self, output):
        self.logger.debug(MyLogger.parseString(output))
        
    @staticmethod
    def parseString(output):
        _type = sys.getfilesystemencoding()
        return str(output).decode(_type).encode('UTF-8')
    
class FTPBean(object):
    '''
    FTP :send files to dist host.
    '''
    params = ['FTP_HOST', 'FTP_PATH', 'FTP_USER', 'FTP_PASSWORD']
    logger = MyLogger("FTPBean")
    FTP_HOST = None
    FTP_PATH = None
    FTP_USER = None
    FTP_PASSWORD = None
    _ftp = None
    CONFIG_INI = None

    
    @staticmethod
    def initFTP(ini):
        config = open(ini)
        FTPBean.CONFIG_INI = ini
        dic = {}
        for line in config:
            tmp = line.strip('\n').split("=",1)
            dic[tmp[0]] = tmp[1]
        config.close()
        for param in FTPBean.params:
            exec('FTPBean.' + param + '=' + 'dic["' + param + '"]')
        
        logger = FTPBean.logger
        logger.info('**********************BEGIN initialize FTPBEAN**********************')
        try:
            ftplib.socket.setdefaulttimeout(3600)
            _ftp = ftplib.FTP(FTPBean.FTP_HOST)
        except (socket.error,socket.gaierror):
            logger.error((' |--------error:connect to FTP host', FTPBean.FTP_HOST, 'Failed!!!'))
            return False
        logger.info((' |--------connect to FTP host', FTPBean.FTP_HOST, 'Success!!!'))
        try:  
            _ftp.login(user=FTPBean.FTP_USER,passwd=FTPBean.FTP_PASSWORD)  
            _ftp.cwd(FTPBean.FTP_PATH)
        except ftplib.error_perm:  
            logger.error((' |--------error:Wrong ftp_user:', FTPBean.FTP_USER, ',ftp_password:', FTPBean.FTP_PASSWORD, 'login failed!!!'))
            _ftp.quit()  
            return False 
        logger.info((' |--------Success to login ftp_user:', FTPBean.FTP_USER, ',ftp_password:', FTPBean.FTP_PASSWORD, ' login Success!!!'))
        FTPBean._ftp = _ftp
        logger.info('**********************END initialize FTPBEAN**********************')
        return True
    
    @staticmethod
    def sendFile(filePath):
        logger = FTPBean.logger
        logger.info('**********************BEGIN sendFile**********************')
        if FTPBean._ftp == None:
            logger.info('ERROR: |--------error: You have to call the function of "FTPBean.initFTP" before "sendFile"!')
            logger.info('**********************END sendFile**********************')
            return False
        try:
            FTPBean._ftp.voidcmd("NOOP")
        except:
            try:
                FTPBean._ftp.quit();
            except:
                pass
            del FTPBean._ftp
            try:
                if not FTPBean.initFTP(FTPBean.CONFIG_INI):
                    logger.info('ERROR: |--------error: You called the function of "FTPBean.initFTP" and throws Exception')
                    return False;
            except:
                logger.info('ERROR: |--------error: You called the function of "FTPBean.initFTP" and throws Exception')
                return False
            
        logger.info((' |--------filePath', filePath, 'connected Success!!!'))

        if(filePath == ""):
            logger.error(' |--------error:filePath can not be null or empty String!')
            logger.info('**********************END sendFile**********************')
            return False
        try:
            fileName = filePath[filePath.rfind(os.sep)+1: len(filePath)]
            print filePath
            fileStream = open(filePath,'rb')
            FTPBean._ftp.storbinary('STOR ' + fileName, fileStream)
# _ftp.storbinary('STOR %s' % fileName,open(self.filePath,'rb'))  
        except:
            logger.error((' |--------error:Unload File:', filePath, 'ERROR!!!'))
            logger.info('**********************END sendFile**********************')
            return False
        finally:
            fileStream.close() 
        logger.info((' |--------Success to upload file:', filePath,'!!!'))
        logger.info('**********************END sendFile**********************')
        return True
        
    @staticmethod
    def destroyFTP():
        logger = FTPBean.logger
        logger.info('**********************BEGIN DESTROY FTP**********************')
        try:
            FTPBean._ftp.quit();
        except:
            logger.info((' |--------Fail to destroy ftp connection !!!'))
        else:
            logger.info((' |--------SUCCESS to destroy ftp connection !!!'))
        finally:
            logger.info('**********************END DESTROY FTP**********************')
            
class HTTPBean(object):
    '''
    http 服务Bean
    '''
    logger = MyLogger("HTTPBean")
    params = ['MESSAGE_SERVER', 'MESSAGE_PORT', 'MESSAGE_URL', 'FTP_HOST','FTP_PORT', 'FTP_PATH', 'FTP_BASE', 'GAME_NAME','fromBI']
    def __init__(self, ini, filePath, iniFilePath, paramsMap):
        '''
        Constructor
        '''
        self.ini = ini
        self.filePath = filePath
        self.iniFilePath = iniFilePath
        self.paramsMap = paramsMap
        #initlogger
        #initFtpParams
        bitools.initParamsWithIni(self)
 
    def senMessage(self):
        logger = self.logger
        
        logger.info('**********************BEGIN senMessage**********************')
        logger.info(' |--------begin to send HTTP message,filePath:'+ self.filePath) 
        dic = {}
        dic['Fip'] = self.FTP_HOST
        dic['Fport'] = self.FTP_PORT
        fname = self.filePath[self.filePath.rfind(os.sep) + 1: len(self.filePath)]
        dic['FfileName'] = fname
        iniFileName = self.iniFilePath[self.iniFilePath.rfind(os.sep) + 1: len(self.iniFilePath)]
        dic['iniFileName'] = iniFileName
        dic['iniMD5File'] = bitools.fileMd5(self.iniFilePath)
        dic['Fpath'] = self.FTP_BASE + self.FTP_PATH
        dic['gameName'] = self.GAME_NAME
        dic['fromBI'] = self.fromBI
        dic['MD5file'] = bitools.fileMd5(self.filePath)
        for obj in self.paramsMap:
            dic[obj] = self.paramsMap.get(obj)
        dic['key'] = bitools.md5('EWy_cHJ6m3qWTR'+dic['FfileName'])
        
        jsonParam = json.dumps(dic)
        logger.info(" |--------jsonParam = " + jsonParam)
        try:
            params = urllib.urlencode({"jsonParam":jsonParam})
            headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
            conn = httplib.HTTPConnection(self.MESSAGE_SERVER,self.MESSAGE_PORT, timeout=20)
            conn.request(method="POST",url=self.MESSAGE_URL,body=params,headers=headers)
            response = conn.getresponse()
            responseString = response.read()
            logger.info((" |--------response = ", responseString))
            logger.info((" |--------response.status = " , response.status))
            if(responseString != 'OK' and responseString != 'REDUNDANCY'):
                logger.error((' |--------error:BIServer Server Exception !! Please connect to the BI Server Manager!! Wrong Code:', responseString))
                return False

            #remove files to save disk space
            os.remove(self.filePath)
            os.remove(self.iniFilePath)
        except socket.error:
            logger.error((' |--------error:Can not connect to BIServer!!Please connect to the BI Server Manager!! Wrong Code:', socket.error.message))
            logger.info('**********************END senMessage**********************')
            return False
        finally:
            conn.close()
        logger.info('**********************END senMessage' + self.FTP_PATH + '**********************')
        return True      
        
class DBDumpBean:
    params=['DB_SID','DB_PWD','DB_HOST','DB_USER','DUMP_PATH','DUMP_DIRECTORY','SQL_PATH','GZ_SUFFIX','MYSQL_PATH']
    logger = MyLogger("DBDumpBean")
    def __init__(self, ini):
        '''
        Constructor
        '''
        self.ini = ini
        #initlogger
        #initFtpParams
        bitools.initParamsWithIni(self)
        

class ExportBean:
    logger = MyLogger("ExportBean")
    selfParams = ['hasHeader','fileNameReg','dataSign','command','selectSQL','fileSort','databases','tables','DB','T_TYPE','T_LENGTH']
    params = ['DUMP_PATH','target_timezone', 'need_transform_time', "SERVER_HOST", 'STR_TO_DATE','DATE_TO_STR','DATE_TO_TIMESTAMP','TIMESTAMP_TO_DATE','DATE_STR_FORMAT', 'GZ_SUFFIX']
    
# initProcessRemainBeans()
    path = cwd + os.sep + 'successMap.data'
    successMap = {}
    
    #get the privilege to execute
    if(not os.path.exists(path)):
        successMap["status"] = 1;
        successMap["maps"] = {}
        _file = file(path,'w') 
        cPickle.dump(successMap, _file)
        _file.close() 
    else:
        _file=file(path)
        successMap = cPickle.load(_file)
        _file.close()
        while(successMap["status"] == 1):
            time.sleep(10)
            if(not os.path.exists(path)):
                successMap["status"] = 1;
                successMap["maps"] = {}
                _file = file(path,'w') 
                cPickle.dump(successMap, _file)
                _file.close()
                break
            _file=file(path)
            successMap = cPickle.load(_file)
            _file.close()
        successMap["status"] = 1;
        _file = file(path,'w') 
        cPickle.dump(successMap, _file)
        _file.close()
    
    #init requestBeans 
    
    @staticmethod
    def persistRemainBeans():
        path = cwd + os.sep + 'successMap.data'
        _file = file(path,'w') 
        cPickle.dump(ExportBean.successMap, _file)
        _file.close() 
    
    @staticmethod
    def processRemainBeans():
        try:
            tempMap = {};
            for _key in ExportBean.successMap["maps"]:
                if(time.time() - ExportBean.successMap["maps"][_key] <= 24 * 7 * 3600):
                    tempMap[_key] = ExportBean.successMap["maps"][_key]
    # ExportBean.successMap["maps"].pop(_key)
            del ExportBean.successMap["maps"]
            ExportBean.successMap["maps"] = tempMap
        except:
            pass;
        finally:
            ExportBean.successMap["status"] = 0
            ExportBean.persistRemainBeans()
        pass
    
    
    def __init__(self, ini):
        self.T_TYPE = 'day'
        self.T_LENGTH = '1';
        self.ini = ini
        
    @staticmethod
    def readExportBeansFromIni(configIni, ini, end_date, start_date, requestBeans, isBeijing=False):
        beans = [];
        config = ConfigParser.ConfigParser()
        _file = open(ini)
        config.readfp(_file)
        bean = ExportBean(configIni)
        for obj in config.sections():
            if obj == 'config': #read configure add
                for o in config.options(obj):
                    for pp in bean.selfParams:
                        if(pp.lower() == o):
                            exec('bean.' + pp +'="""' + config.get(obj, o) + '"""')
                break; # add
        _file.close()
        try:
            if None != bean.databases:
                bean.databases = eval(bean.databases)
                for sss in range(0, len(bean.databases)):
                    dbs = bean.databases[sss];
                    tempDBs = [];
                    for v_str in dbs:
                        if ':' in v_str:
                            v_sum = int(v_str.split(":")[1])
                            v_value = v_str.split(":")[0].strip()
                            for var in range(0, v_sum):
                                tempDBs.append(v_value)
                        else :
                            tempDBs.append(v_str)
                    bean.databases[sss] = tempDBs
                
                bean.tables = eval(bean.tables)
        except:
            bean.databases = [];
            
        filePath = ExportBean.getFilePath(bean, end_date, start_date, isBeijing)
        isContains = False
        for _key in ExportBean.successMap["maps"]:
            if(_key[0: _key.rfind(".")] == filePath):
                isContains = True
        result = True;
        mm = None
        try:
            mm = sys.argv[1].strip()
        except:
            if isContains:
                result = False;

        if isContains and (mm != "-f"):
            result = False;
        
        if(requestBeans != None):
            i = 0;
            while i < len(requestBeans):
                rBean = requestBeans[i]
                if(rBean != None and rBean.strip()[0 : rBean.strip().rfind(".")] == filePath[filePath.rfind(os.sep) + 1 : len(filePath)]):
                    result = True
                    i = i + 1
                    continue
    #warning        
                if(rBean != None):
                    rBean = rBean.strip()
                    srcSerTabName = filePath[filePath.rfind(os.sep) + 1 : filePath[0 : filePath.rfind("-")].rfind("-")]
                    print "srcSerName", srcSerTabName
                    srcSerBegin = filePath[filePath.rfind(srcSerTabName) + 1 + len(srcSerTabName): filePath.rfind("-")]
                    print "srcSerBegin", srcSerBegin
                    srcSerEnd = filePath[filePath.rfind("-") + 1 : filePath.find(".", filePath.rfind("-"))]
                    print "srcSerEnd", srcSerEnd
                    
                    desSerTabName = rBean[0 : rBean[0 : rBean.rfind("-")].rfind("-")]
                    print "desSerTabName", desSerTabName
                    desSerBegin = rBean[rBean.rfind(desSerTabName) + 1 + len(desSerTabName) : rBean.rfind("-")]
                    print "desSerBegin", desSerBegin
                    desSerEnd = rBean[rBean.rfind("-") + 1 : rBean.find(".", rBean.rfind("-"))]
                    print "desSerEnd", desSerEnd
                    
                    if(srcSerTabName == desSerTabName and (srcSerBegin != desSerBegin or srcSerEnd != desSerEnd)):
                        #requestBeans.remove(rBean)
                        beans = beans + ExportBean.readExportBeansFromIni(configIni, ini, time.mktime(time.strptime(desSerEnd, '%Y_%m_%d_%H_%M')), time.mktime(time.strptime(desSerBegin, '%Y_%m_%d_%H_%M')), requestBeans[i: i+1], True)
                i = i + 1
        if result:
            bean.filePath = filePath;
            bean.iniFilePath = filePath[0 : filePath.rfind('.')] + ini[ini.rfind('.') : len(ini)]
            _out = file(bean.iniFilePath,'w')
            _in = open(ini, 'r')
            _out.truncate()
            _out.write(_in.read())
            _out.close()
            _in.close()
            beans.append(bean)
        else:
            pass
        return beans
    
# def get
    
    
    @staticmethod
    def getFilePath(obj, end_date, start_date = 0, isBeijing=False):
        length = int(obj.T_LENGTH)
        bitools.initParamsWithIni(obj)
        fileNameReg = obj.fileNameReg.lower()
        
        if obj.SERVER_HOST is not None and str.lower(obj.SERVER_HOST).strip() != "auto":
            hostname = obj.SERVER_HOST.strip()
        else:
            hostname = socket.gethostname()
        
        # isBeijing :True(bucai)
        if(isBeijing):
            now = time.localtime(end_date)
            yesterday = time.localtime(start_date)
        else:
            #Does it need to transform to target_timezone
            if obj.need_transform_time is None or str.lower(obj.need_transform_time).strip() == "true":
                end_date = round(end_date + time.timezone - time.localtime().tm_isdst * 3600 + eval(obj.target_timezone)*3600, 0)
                now = time.localtime(end_date)
            else:
                now = time.localtime(end_date)
                pass
            yesterday = time.localtime(end_date-24*60*60) 
            pass
        obj.now = now
        obj.yesterday = yesterday
            
        if obj.need_transform_time is None or str.lower(obj.need_transform_time).strip() == "true":
            ExportBean.logger.info("Beijing Time now: " + time.strftime('%Y_%m_%d_%H_%M', now))
            ExportBean.logger.info("Beijing Time yesterday: " + time.strftime('%Y_%m_%d_%H_%M', yesterday))
            diff = str(round(time.timezone - time.localtime().tm_isdst * 3600 + eval(obj.target_timezone)*3600, 0))
            ExportBean.logger.info("Local Time to Beijing Time diff: " + str(diff))
        else:
            ExportBean.logger.info("User Local Time to BI Servcer now: " + time.strftime('%Y_%m_%d_%H_%M', now))
            diff = "0"
        
        
        ExportBean.logger.info("time.timezone: " + str(time.timezone))
        
        ExportBean.logger.info("time.localtime().tm_isdst: " + str(time.localtime().tm_isdst))
        
# current_day = int(time.strftime("%d", current))
        current_hour = int(time.strftime("%H", now))
        current_minute = int(time.strftime("%M",now)) 
        
        if obj.T_TYPE=='day':
            edate = time.strftime("%Y_%m_%d_00_00", now)
        elif obj.T_TYPE=='hour':
            edate = time.strftime("%Y_%m_%d_%H_00", time.localtime(end_date - current_hour % length * 3600))
            pass;
        elif obj.T_TYPE=='minute':
            edate = time.strftime("%Y_%m_%d_%H_%M", time.localtime(end_date - current_minute % length * 60))
        if(obj.dataSign == "increase" or obj.dataSign == "server_total"):
# sdate = time.strftime("%Y_%m_%d_00_00", yesterday)
            if obj.T_TYPE=='day':
                sdate = time.strftime("%Y_%m_%d_00_00", time.localtime(end_date - length * 24 * 3600))
            elif obj.T_TYPE=='hour':
                sdate = time.strftime("%Y_%m_%d_%H_00", time.localtime(end_date - current_hour % length * 3600 - length * 3600))
            elif obj.T_TYPE=='minute':
                sdate = time.strftime("%Y_%m_%d_%H_%M", time.localtime(end_date - current_minute % length * 60 - length * 60))
        else:
            sdate = '1970_01_01_00_00'
        dump_path = obj.DUMP_PATH
        fileSort = obj.fileSort
        #initFilePath
        for oo in ['hostname','sdate','edate','dump_path','fileSort']:
            exec('fileNameReg = fileNameReg.replace("${' + oo.lower() + '}", ' + oo + ')');

        #initselectSQL
        selectSQL = obj.selectSQL
        selectSQL = selectSQL.replace('${begin_date}', obj.TIMESTAMP_TO_DATE + "(" + obj.DATE_TO_TIMESTAMP + "(" + obj.STR_TO_DATE + "('" + time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(sdate,'%Y_%m_%d_%H_%M')) + "', '" + obj.DATE_STR_FORMAT + "')) - (${diff}))")
        selectSQL = selectSQL.replace('${end_date}', obj.TIMESTAMP_TO_DATE + "(" + obj.DATE_TO_TIMESTAMP + "(" + obj.STR_TO_DATE + "('" + time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(edate,'%Y_%m_%d_%H_%M')) + "', '" + obj.DATE_STR_FORMAT + "')) - (${diff}))")
        
        first, second = getVariable(selectSQL, "timestamp_to_date");
        while(first != None):
            second = obj.TIMESTAMP_TO_DATE + "(" + second + ")"
            selectSQL = selectSQL.replace(first, second)
            first, second = getVariable(selectSQL, "timestamp_to_date");
            
        
        first, second = getVariable(selectSQL, "date");
        while(first != None):
            second = obj.DATE_TO_STR + '(' + obj.TIMESTAMP_TO_DATE + '(' + obj.DATE_TO_TIMESTAMP + '(' + second + ") +(${diff})), '" + obj.DATE_STR_FORMAT + "') "
            selectSQL = selectSQL.replace(first, second)
            first, second = getVariable(selectSQL, "date");
        
        first, second = getVariable(selectSQL, "string");
        while(first != None):
            if platform.system() == 'Windows':
                second = '''concat('""',replace(''' + second + ''','""','""""'),'""')''' 
            else:
                second = """concat('\\"',replace(""" + second + """,'\\"','\\"\\"'),'\\"')""" 
            selectSQL = selectSQL.replace(first, second)
            first, second = getVariable(selectSQL, "string");
        
        selectSQL = selectSQL.replace('${diff}', diff)
        obj.selectSQL = selectSQL
        return fileNameReg
    
    @staticmethod
    def writeExportBeansToIni(ini, beans):
        config = ConfigParser.ConfigParser()
        _file = open(ini)
        config.readfp(_file)
        _file.close()


class RequestBean:
    def __init__(self):
        pass
    logger = MyLogger("RequestBean")
    @staticmethod
    def getRequestBeans(ini, message_url):
        config = open(ini)
        dic = {}
        for line in config:
            tmp = line.strip('\n').split("=",1)
            dic[tmp[0]] = tmp[1]
        config.close()
        message_server = dic["MESSAGE_SERVER"]
        message_port = dic["MESSAGE_PORT"]
        gameName = dic["GAME_NAME"]
        logger = RequestBean.logger
        try:
            logger.info('**********************BEGIN senMessage to initial RequestBean**********************')
            
            params = urllib.urlencode({"gameName":gameName})

            headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
            conn = httplib.HTTPConnection(message_server, message_port, timeout=20)
            conn.request(method="POST",url=message_url, body=params, headers=headers) 
            response = conn.getresponse()
            responseString = response.read()
            logger.info((" |--------response = ", responseString))
            logger.info((" |--------response.status = " , response.status))
            requestBeans = None
            requestBeans = eval(responseString);

        except socket.error:
            logger.error((' |--------error:Can not connect to BIServer to get RequestBeans From BIServer !!Please connect to the BI Server Manager!! Wrong Code:', socket.error.message))
            logger.info('**********************END senMessage to initial RequestBean**********************')
            return None
        finally:
            conn.close()
        logger.info('**********************END senMessage to initial RequestBean**********************')
        
        return requestBeans  
