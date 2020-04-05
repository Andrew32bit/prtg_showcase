# -*- coding: utf-8 -*-
import logging
import datetime
import os
import json
import pysftp
import yaml
from snakebite.client import Client
import paramiko
try:
    logging.getLogger("paramiko").setLevel(logging.ERROR)
except:
    None

Log = None

with open('creds.yaml') as f:
    doc=yaml.load(f)

UNIXusername=doc['credentials']['username']
UNIXpassword=doc['credentials']['password']


def GetJSONValue(sJSON,key,noError=False):
    try:
        json_str = json.loads(sJSON, encoding="windows-1251")
        return json_str[key]
    except Exception as e:
        if noError!=True:
            Log.AddLog('ERROR: GetJSONValue: The key ['+key+'] is invalid! ('+str(e)+')')
        return None

def saveList(LogList,FileName):
    try:
        fo = open(FileName, "w+")
        for item in LogList:
            fo.write("%s\n" % item)
        fo.close()
    except Exception as inst:
        print ("ERROR: saveList ["+FileName+"]")
        print (type(inst))     # the exception instance
        print (inst.args)
        print (inst)          # __str__ allows args to be printed directly



class dpmiLog(object):
    def __init__(self, aLogPath=None):
        try:
            self.LogPath=aLogPath
            now = datetime.datetime.now()
            self.logFile = logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = logging.INFO, filename=aLogPath+'log'+now.strftime("%Y-%m")+'.txt')
            self.ClearLog()
        except:
            print("Error open log!")

    def StopLog(self):
        self.logFile = None

    def AddLog(self,msg):
        try:
            now = datetime.datetime.now()
            logging.info('['+now.strftime("%Y-%m-%d %H:%M")+'] '+msg)
            logging.info(msg.decode('cp1251'))
            logging.info(msg)
        except:
            pass

        return

    def ClearLog(self):
        return
        now = datetime.datetime.now()
        files = os.listdir(self.LogPath)
        for filename in files:
            if len(filename)==14:
                filename_dt=filename.replace('log','')
                filename_dt=filename_dt.replace('.txt','')
                if len(filename_dt)==7:
                    #dt=datetime.strptime(filename_dt[0:4]+'-'+filename_dt[5:7]+'-01', '%Y-%m-%d')
                    dt=datetime.datetime.strptime(filename_dt[0:4]+'-'+filename_dt[5:7]+'-01', '%Y-%m-%d')
                    #AddLog(filename+' dt='+dt.strftime("%Y-%m-%d %H:%M")+' dif='+str((now.year - dt.year)*12 + now.month - dt.month))
                    diff=(now.year - dt.year)*12 + now.month - dt.month
                    if diff>=3:
                        self.AddLog('delete log: '+filename)
                        os.remove(self.LogPath+filename)

#############################################################
# Заголовок по умолчанию дял SQL комманды
#############################################################
def hive_sql_prefix(job_name='Analytics JOB'):
    s=  'SET mapred.job.name='+job_name+';\n'\
        'SET mapred.job.priority=VERY_HIGH;\n' \
        'set mapreduce.job.queuename=A1;\n'
    return s

#############################################################
# Запускает SQL скрипт на hive - возвращаем весь stdout
#############################################################
def execute_hive_command(SQL,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22,quote=1):
    res = ''

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport)
        #hadoop fs -put metro_msbol.csv

        if quote==2:
            command = '''hive -e "''' + SQL + '''"'''
        else:
            command = "hive -e '"+SQL+"'"
        # zcat /opt/openam/logs/ru.org.openam.smpp.2016-12-22*.gz | zgrep -c SENT
        stdin, stdout, stderr = client.exec_command(command)

        data = stdout.read() + stderr.read()

        client.close()
        res = str(data)
    except Exception as e:
        try:
            print('execute_hive_command ERROR: ' + str(e))
            logging.error('execute_hive_command ERROR: ' + str(e))
        except:
            None

    return res

#############################################################
# Копируем файл на unix машину
#############################################################
def put_to_sftp(filename,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22):
    res = False

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        ftp = pysftp.Connection(host=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport,
                                cnopts=cnopts)
        logging.info('put_to_sftp: start')
        ftp.put(filename)
        ftp.close()
        logging.info('put_to_sftp: end')
        res = True
    except Exception as e:
        try:
            print('put_to_sftp ERROR: ' + str(e))
            logging.error('ERROR: put_to_sftp e=' + str(e))
        except:
            None
    return res

#############################################################
# Удаляем файл с unix машины
#############################################################
def remove_from_sftp(filename,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22):
    res = False

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        ftp = pysftp.Connection(host=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport,
                                cnopts=cnopts)
        logging.info('remove_from_sftp: start')
        ftp.remove(filename)
        ftp.close()
        logging.info('remove_from_sftp: end')
        res = True
    except Exception as e:
        try:
            print('remove_from_sftp ERROR: ' + str(e))
            logging.error('ERROR: remove_from_sftp e=' + str(e))
        except:
            None

    return res


    return res

#############################################################
# Проверка/Создание папка на HDFS
#############################################################
def CheckHDFSPath(HDFSPath,hdfs_host='10.1.4.14',hdfs_user=UNIXusername):
    res = False
    try:
        client = Client(hdfs_host, effective_user=hdfs_user)

        if not client.test(HDFSPath, exists=True, directory=True):
            for y in client.mkdir([HDFSPath], create_parent=True):
                res=True
        else:
            res=True
    except Exception as e:
        logging.error('ERROR: CheckHDFSPath: '+str(e))
        res=False

    return res

#############################################################
# Копируем файл на HDFS
#############################################################
def put_to_hdfs(filename,HDFSPath,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22):
    res = False

    # Проверка/Создание папка на HDFS
    if not CheckHDFSPath(HDFSPath):
        return res

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport)
        #hadoop fs -put metro_msbol.csv
        command = "hadoop fs -put "+filename+" "+HDFSPath
        # zcat /opt/openam/logs/ru.org.openam.smpp.2016-12-22*.gz | zgrep -c SENT
        stdin, stdout, stderr = client.exec_command(command)
        data = stdout.read() + stderr.read()
        client.close()
        res = True
    except Exception as e:
        logging.error('ERROR: put_to_hdfs e=' + str(e))

    return res

#############################################################
# Удаляем файл с HDFS
#############################################################
def del_file_hdfs(filename,HDFSPath,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22):
    res = False

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport)
        #hadoop fs -put metro_msbol.csv

        command = "hadoop fs -rm "+HDFSPath+filename
        # zcat /opt/openam/logs/ru.org.openam.smpp.2016-12-22*.gz | zgrep -c SENT
        stdin, stdout, stderr = client.exec_command(command)

        data = stdout.read() + stderr.read()

        client.close()
        res = True
    except Exception as e:
        logging.error('ERROR: del_file_hdfs e=' + str(e))

    return res


#############################################################
# Выполняем ssh комманду
#############################################################
def ssh_cmd_exec(cmd,UNIXhost='10.1.4.16',UNIXusername=UNIXusername,UNIXpassword=UNIXpassword,UNIXport=22):
    res = ''

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=UNIXhost, username=UNIXusername, password=UNIXpassword, port=UNIXport)
        stdin, stdout, stderr = client.exec_command(cmd)

        data = stdout.read() + stderr.read()

        client.close()
        res = str(data)
    except Exception as e:
        logging.error('ERROR: ssh_cmd_exec e=' + str(e))

    return res

def list_to_str(lst):
    res=''
    for it in lst:
        if res!='':
            res+=','
        res=res+str(it).replace("'",'"')
    return '['+res+']'