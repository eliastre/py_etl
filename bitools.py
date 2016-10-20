import logging
import hashlib
import gzip
import os
'''
@author: MUHE
'''
def initParamsWithIni(bean):
    config = open(bean.ini)
    dic = {}
    for line in config:
        tmp = line.strip('\n').split("=",1)
        dic[tmp[0].strip()] = tmp[1].strip()
    config.close()
    for param in bean.params:
        try:
            exec('bean.' + param + '=' + 'dic["' + param + '"]')
        except:
            exec('bean.' + param + '=None')
            continue;

def fileMd5(filePath):
    files = open(filePath, 'rb')
    md5str = hashlib.md5(files.read()).hexdigest()
    files.close()
    return md5str

def md5(sourceString):
    return hashlib.md5(sourceString).hexdigest()

def gzipFile(filePath, GZ_SUFFIX):
    logging.info("start gzip file "+filePath)
    f_in = open(filePath, 'rb')
    f_out = gzip.open(filePath+GZ_SUFFIX, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    logging.info("finish gzip file "+filePath)
    os.remove(filePath)   
    return filePath+GZ_SUFFIX

def getVariable(command, key):
    key = "$" + key + "{";
    key = key.upper()
    temp = command;
    command = command.upper()
    if(command.find(key) != -1):
        index = command.find(key)
        rIndex = command.index('}', index)
        mm = command[index : rIndex + 1]
        lIndex = command.find("{", index)
        tmm = mm[mm.find("{") + 1 : mm.find("}")];
        while tmm.find("{") != -1:
            lIndex = command.index("{", lIndex + 1)
            rIndex = command.index('}', rIndex + 1)
            if rIndex == -1:
                return None,None
            tmm = command[lIndex + 1 : rIndex]
        command = temp
        _str = command[index : rIndex +1].strip()
        return (_str, _str[len(key) : len(_str)-1]) 
    else:
        return None,None
    
def removeFile(filePath):
    try:
        os.remove(filePath);
    except:
        pass
#mm='''
# select u_id,
# $strding{task_id} as task_id,
# state,
# $string{$date{$TIMESTAMP_TO_DATE{time}}$Date_to_timestamp{}} as time
# from ${database}.$table{1} 
#'''
#print getVariable(mm,'string')
