# -*- coding:utf-8 -*- 
'''

@author: MUHE
'''
#import time
import os
import re
import datetime
from bi2 import ExportBean, DBDumpBean, FTPBean, HTTPBean, MyLogger, RequestBean
import random
import cPickle
import bitools
import sys
import time
import platform

class Export:
    
    def __init__(self, ini, EXPORT_PREFIX, EXPORT_SUFFIX):
        self.ini = ini
        self.EXPORT_SUFFIX = EXPORT_SUFFIX
        self.EXPORT_PREFIX = EXPORT_PREFIX
        self.dumpBean = DBDumpBean(ini)
        self.logger = MyLogger('MAIN')
    def __start__(self):
        logger = self.logger;
        global cwd
        logger.info('#########################################EXPORT MAIN BEGIN#########################################')
        path = cwd + os.sep + 'httprequest.data'
        
        logger.info('*******************************to initial remain HTTPBean BEGIN*******************************') 
        self.initRemainBeans(path)
        logger.info('*******************************to initial remain HTTPBean END*******************************\n') 
        
# logger.info('*******************************to initial remain HTTPBean BEGIN*******************************') 
# self.initRemainBeans(path)
# logger.info('*******************************to initial remain HTTPBean END*******************************\n') 
        
        logger.info('*******************************to initial self.exportBeans with export_*.ini file BEGIN*******************************') 
        self.initExportBeans()
        logger.info('*******************************to initial self.exportBeans with export_*.ini file END*******************************\n') 
        
        logger.info('*******************************to service self.exportBeans BEGIN*******************************') 
        if(FTPBean.initFTP(ini)):
            for oo in self.exportBeans:
                logger.info('+++++++++++++++++++++++exportBeans' + oo.fileNameReg + ' BEGIN+++++++++++++++++++++++') 
                self.service(oo)
                logger.info('+++++++++++++++++++++++exportBeans' + oo.fileNameReg + ' END+++++++++++++++++++++++') 
            FTPBean.destroyFTP()
        logger.info('*******************************to service self.exportBeans END*******************************\n') 
        
        logger.info('*******************************to process remainBeans BEGIN*******************************\n') 
        self.processRemainBeans()
        logger.info('*******************************to process remainBeans END*******************************\n') 
        
        logger.info('*******************************to persist failed remainBeans BEGIN*******************************\n') 
        self.persistRemainBeans(path)
        logger.info('*******************************to persist failed remainBeans END*******************************\n') 
        
        logger.info('#########################################EXPORT MAIN END#########################################')

    def processRemainBeans(self):
        logger = self.logger
        logger.info('+++++++++++++++++++++++REMAINBEANS.LENGTH :' + str(len(self.remainBeans)) + ' +++++++++++++++++++++++') 
        tempArray = [];
        for index in range(len(self.remainBeans)):
            obj = self.remainBeans[index]
            logger.info('+++++++++++++++++++++++REMAINBEANS.NO.' + str(index) + ' +++++++++++++++++++++++') 
            if not obj.senMessage():
                tempArray.append(obj)
        del self.remainBeans
        self.remainBeans = tempArray
        
    def initRemainBeans(self, path):
        self.remainBeans = []
        if(not os.path.exists(path)):
            return
        _file=file(path)
        self.remainBeans = cPickle.load(_file)
        _file.close()
    
    def persistRemainBeans(self, path):
        _file = file(path,'w') 
        cPickle.dump(self.remainBeans, _file)
        _file.close()        
    
    def isset(self, v):  
        try:  
            type (eval(v))  
        except:  
            return 0  
        else:
            return 1  
    def initExportBeans(self):
        self.exportBeans = []
        for obj in os.listdir(os.curdir):
            if obj.startswith(EXPORT_PREFIX) and obj.endswith(EXPORT_SUFFIX) :
                _obj = ExportBean.readExportBeansFromIni(self.ini, obj, current, 0, requestBeans)
                if _obj != None:
                    self.exportBeans = self.exportBeans + _obj
    
    def processSuccessMap(self, filePath):
        ExportBean.successMap["maps"][filePath] = time.time()
        

    def service(self, obj):
        logger = self.logger;
        # EXCEPTION : this code for Oracle should be modified into the type which is similar to MySQL.
        filePath = obj.filePath
        if obj.DB.upper() == "ORACLE" :
            iniFilePath = obj.iniFilePath
            fileName = filePath[filePath.rfind(os.sep)+1: len(filePath)]
            ######init shell command#######
            command = obj.command
            command = command.replace('${DB_USER}', self.dumpBean.DB_USER)
            command = command.replace('${DB_PWD}', self.dumpBean.DB_PWD)
            command = command.replace('${DB_SID}', self.dumpBean.DB_SID)
            try:
                ######init sql.file command#######
                sqlStream = open(self.dumpBean.SQL_PATH, 'r')
                sqlText = sqlStream.read()
                sqlText = sqlText.replace('${DUMP_DIRECTORY}', self.dumpBean.DUMP_DIRECTORY)
                sqlText = sqlText.replace('${selectSQL}', obj.selectSQL.replace("'", "''"))
                sqlText = sqlText.replace('${fileName}', fileName)
                sqlStream.close()
                tempFile = "/tmp/export_sql_temp" + str(random.randint(0,65535)) + ".sql"
                sqlStream = file(tempFile, 'w')
                sqlStream.truncate()
                print sqlText
                sqlStream.write(sqlText)
                sqlStream.close()
                command = command.replace('${SQL_PATH}', tempFile)
                os.system(command)
                print fileName
                filePath = bitools.gzipFile(filePath, self.dumpBean.GZ_SUFFIX) 
                #send csv
                FTPBean.sendFile(filePath)
                FTPBean.sendFile(iniFilePath)
               
                httpBean = HTTPBean(ini, filePath, iniFilePath, {'dataSign': obj.dataSign, 'hasHeader':'true'})
                
                self.remainBeans.append(httpBean)
            except os.error:
                pass
            finally:
                pass
            os.remove(tempFile)
        elif obj.DB.upper()=='MYSQL':
            iniFilePath = obj.iniFilePath
            fileName = filePath[filePath.rfind(os.sep)+1: len(filePath)]
            ######init shell command#######
# command = ${mysql_path} -u${mysql_user} -p${mysql_pwd}<<EOF
# ${selectSQL}
# quit;
# exit;
            command = obj.command
            command = command.replace('${MYSQL_PATH}', self.dumpBean.MYSQL_PATH)
            DB_PWDs = eval(self.dumpBean.DB_PWD)
            DB_USERs = eval(self.dumpBean.DB_USER)
            DB_HOSTs = eval(self.dumpBean.DB_HOST)
            DBIndex = len(DB_HOSTs)
            try:
                #清空文件
                fileStream = file(filePath, 'w')
                fileStream.truncate()
                fileStream.close()
                for k in range(0,DBIndex):
                    if(obj.databases == None or len(obj.databases) == 0):
                        continue
                    index = len(obj.databases[k]);
                    if index == 0:
                        continue
                    
                    t_command = command;
                    t_command = t_command.replace('${DB_PWD}', DB_PWDs[k])
                    t_command = t_command.replace('${DB_USER}', DB_USERs[k])
                    t_command = t_command.replace('${DB_HOST}', DB_HOSTs[k])
                    
                    for i in range(0,index) :
                        temp_command = t_command;
                        #init SELECT_SQL
                        selectSQL = obj.selectSQL
                        selectSQL = selectSQL.replace("${database}", obj.databases[k][i])
                        while(selectSQL.find("$table{") != -1):
                            ind = selectSQL.find('$table{')
                            mm = selectSQL[ind : selectSQL.index('}', ind) + 1]
                            _str = mm[mm.index('{') + 1 : len(mm)-1].strip()
                            selectSQL = selectSQL.replace(mm, obj.tables[k][i][eval(_str)-1])
    # selectSQL += "\n into outfile '" + tempPath + """'   
    # fields terminated by ',' optionally enclosed by '"' escaped by '"'   
    # lines terminated by '\\r\\n'; """
                        temp_command = temp_command.replace("${selectSQL}", selectSQL)
                        temp_command = temp_command.replace("${fileName}", filePath)
                        temp_command = temp_command.replace("\n"," ");
                        logger.info( temp_command)
                        if(platform.system()=='Windows'):
                            temp_cwd = os.getcwd()
                            os.chdir(self.dumpBean.MYSQL_PATH[0:self.dumpBean.MYSQL_PATH.rfind(os.sep)]);
                            temp_command = temp_command.replace(self.dumpBean.MYSQL_PATH, 'mysql')
# logger.info('****************************** status: ' + str(os.system(temp_command)) + ' ************************')
# status, resultstr = commands.getstatusoutput(temp_command)
# print resultstr
                        status = os.system(temp_command)
# resultstr = os.system(temp_command)
                        logger.info('****************************** status: ' + str(status) + ' ************************')
# if status <> 0:
# logger.error("$$$$$$$$$$$$$$$$$$$$$$$ MESSAGE: " + str(resultstr) + '$$$$$$$$$$$$$$$$$$$$$$')
                        if(platform.system()=='Windows'):
                            os.chdir(temp_cwd)
                print fileName
# if(platform.system()=='Windows'):
                logger.info('*******************************to process2CSV begin*******************************\n') 
                del fileStream
                fileStream = open(filePath, 'r')
                dataText = fileStream.read();
                dataText = dataText.replace('\t', ',')
                dataText = dataText.replace('\r', ' ')
                fileStream.close()
                del fileStream
                fileStream = open(filePath, 'w')
                fileStream.truncate()
                fileStream.write(dataText);
                fileStream.close()
                del dataText
                del fileStream
                logger.info('*******************************to process2CSV end*******************************\n') 
                
                filePath = bitools.gzipFile(filePath, self.dumpBean.GZ_SUFFIX) 
                #send csv
                FTPBean.sendFile(filePath)
                FTPBean.sendFile(iniFilePath)
               
                httpBean = HTTPBean(ini, filePath, iniFilePath, {'dataSign': obj.dataSign, 'hasHeader':'false'})
                sBean = None
                for hBean in self.remainBeans:
                    if(hBean.filePath == httpBean.filePath):
                        sBean = hBean
                        break
                if(sBean != None):
                    self.remainBeans.remove(sBean)
                self.remainBeans.append(httpBean)
            except os.error:
                pass
        else:
            pass
        self.processSuccessMap(filePath);
    

if __name__ == "__main__":
    try:
 print 1
        filedir='/data2/biftp/dump/meizisanguo_yanyi/';
        print 2
        today_file=datetime.datetime.now().strftime('%Y_%m_%d_00_00')+'.csv.gz';
        find_file=re.compile(today_file)

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_consume_log')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'increase', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1


 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_card')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'increase', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_payment_log')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'increase', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_login_log')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'increase', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_basic')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'server_total', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_map')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'increase', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('user_role_info')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'server_total', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1

 for file in os.listdir(filedir):
                if os.path.isdir(file):
                        print "bang"
                else :
                        if (file.find('m_card')!=-1) and (file.find("csv.gz")!=-1) and (file.find(today_file)!=-1) :
                                print file
                                inifile=file.replace(".csv.gz",".ini")
                                print inifile
    try:
                                 httpBean = HTTPBean('config.ini', filedir+file, filedir+inifile, {'dataSign': 'server_total', 'hasHeader':'false'})
                                 httpBean.senMessage()
    except Exception as e1:
     print e1


    except Exception as e:
        print e



