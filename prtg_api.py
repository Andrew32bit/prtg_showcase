# -*- coding: utf-8 -*-
##########################################################################################################################################################
##
## 28.05.2018
## По списку sennsors - берет данные из PRTG API и кладет в таблицу default.prtg_sensors
##
## Следит за полями, струкутрой, партициями
##
## Нужны доступы: PRTG, UNIX host с утилитой hadoop - через ssh и ftp
## На HDFS работает и хранит файлы от имени stcuscol
## Вынесео в dpmi_utils: execute_hive_command, put_to_sftp, remove_from_sftpm, CheckHDFSPath, put_to_hdfs, del_file_hdfs
## где IP, username и проч прописано по умолчнию
##
## Empty table:
## CREATE EXTERNAL TABLE default.prtg_sensors
## (dt TIMESTAMP COMMENT 'Hive formated prtg Date Time',
## id BIGINT COMMENT 'sensor id')
## COMMENT 'PRTG sensors data, created by ibm@MT'
## PARTITIONED BY (   year INT,    month INT,    day INT )
## ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
## STORED AS TEXTFILE LOCATION '/user/stcuscol/published/prtg_sensors/'
## TBLPROPERTIES ('skip.header.line.count'='1')
##
## http://10.1.4.14:50070/explorer.html#/user/stcuscol/published/prtg_sensors/year=2018/month=5/day=27
##
## PRTG Server (10.1.1.248) [Core]
## https://10.1.1.248/device.htm?id=40&tabid=1
##
##########################################################################################################################################################
prtg_login='ibm'
prtg_password='es5ZiEwD'
##########################################################################################################################################################
copyRightTableString="PRTG sensors data, created by ibm@MT" # Не менять! Используется для парсинга текущей структу таблицы
##########################################################################################################################################################
#default_sennsors=[129511]
#default_sennsors=[31310]
# ================ INFO ===================
# 93455 начало данных с 2018-04-06 10:32:00
# 115515 начало данных 2017-12-07 03:40:00
# 93457 2017-12-13 00:20:00
# 98481 2017-12-07 04:48:00
# 98483 2017-12-07 03:30:00
# 115529 2017-11-16 20:14:00
# 122054 2017-11-20 11:24:00
# 129511, 129512 2017-09-19 14:01:00
# Все состальные 58526,44047,31310,36352,34972,32705,33681,34948,34990,63664,32427 c 2017-06-07 !!!!
# ================ INFO ===================
#'''
default_sennsors=[129511,129512,58526,44047,31310,36352,34972,32705,33681,34948,34990,63664,32427,93455,93457,122054,115515,98481,98483,115529]
#'''

##########################################################################################################################################################
import sys
# sys.path.append('C:\\MAXI\\Projects\\dpmi\\')
sys.path.append('/Users/a.konstantinov/Desktop/prtg/')
import requests
import dpmi_utils as du
# import dpmi_db
import datetime as dt
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from subprocess import check_output
from io import StringIO

##########################################################################################################################################################
import logging
# root_path='C:\\MAXI\\Projects\\PRTG_API\\'
root_path='/Users/a.konstantinov/Desktop/prtg/'
app_name='prtg_api'

# if sys.version_info[0] < 3:
#     from StringIO import StringIO
# else:
#     from io import StringIO

try_log_part=''
log_ok=False
for i in range(0,10):
    logger = None
    s_logger = None
    try:
        logger = logging.getLogger("prtg_api")
        logger.setLevel(logging.INFO)

        fh = logging.FileHandler(root_path+'log_'+app_name+'_'+dt.datetime.now().strftime("%Y-%m")+try_log_part+'.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        logger.addHandler(fh)

        #Краткий
        s_logger = logging.getLogger("prtg_api_small")
        s_logger.setLevel(logging.INFO)

        fh = logging.FileHandler(root_path+'s_log_'+app_name+'_'+dt.datetime.now().strftime("%Y-%m")+try_log_part+'.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        s_logger.addHandler(fh)

        log_ok = True
        break
    except:
        try_log_part = try_log_part+'_'

##########################################################################################################################################################
##########################################################################################################################################################

def AddLog(msg):
    print(msg)
    try:
        if 'ERROR' in str(msg).upper():
            logger.error(msg)
        else:
            logger.info(msg)
    except Exception as e:
        print('AddLog: '+str(e))

def s_AddLog(msg):
    if not s_logger is None:
        s_logger.info(msg)

##########################################################################################################################################################
# Получаем текущую структуру полей таблицы default.prtg_sensors
##########################################################################################################################################################

def get_fields():
    AddLog('START: get_fields')

    current_columns=[]
    current_type=[]
    current_comment = []
    txt=''
    res=du.execute_hive_command(du.hive_sql_prefix('PRTG Sensors import')+'show create table default.prtg_sensors')
    find_str1 = "CREATE EXTERNAL TABLE `default.prtg_sensors`("
    # find_str2 = ")COMMENT '"+copyRightTableString+"'" TODO find_str2
    find_str2 = "COMMENT '" + copyRightTableString + "'"
    res=res.replace('\n','')
    pos1=res.find(find_str1,0)
    if pos1>=0:
        pos2 = res.find(find_str2, pos1)
        if pos2>=0:
            txt=res[pos1+len(find_str1):pos2]

    for item in txt.split(','):
        columns_content1=item.replace('` ',',').replace('`','').split(',')
        try:
            current_columns.append(columns_content1[0].replace(' ','').strip(' \t\n\r'))
            if 'COMMENT' in columns_content1[1].upper():
                columns_content2 = columns_content1[1].split('COMMENT')

                current_type.append(columns_content2[0].replace('`','').replace('"','').replace("'",'').replace(' ','').strip(' \t\n\r'))
                current_comment.append(columns_content2[1].replace('`','').replace('"','').replace("'",'').strip(' \t\n\r'))
            else:
                current_type.append(columns_content1[1].replace(' ','').strip(' \t\n\r'))
                current_comment.append('')
        except Exception as e:
            current_columns = []
            current_type = []
            current_comment = []
            AddLog('ERROR: get_fields '+ e)
            return current_columns, current_type, current_comment

    AddLog('END: get_fields: '+str(current_columns)+' / '+str(current_type))
    return current_columns, current_type, current_comment

##########################################################################################################################################################
# Пересоздаем таблицу default.prtg_sensors
# columns - список все колонок таблицы - для пересоздания
# ColumnsName - словарь имена колонок и названия в prtg - для поля COMMENT
##########################################################################################################################################################
def recreate_table_prtg_sensors(columns,ColumnsName):
    AddLog('START: recreate_table_prtg_sensors')
    fields=""
    for field in columns:
        if fields!='':
            fields +=', '
        type_str = 'BIGINT'
        if field == 'dt':
            type_str = 'TIMESTAMP'
        comment=''
        if field == 'dt':
            comment=' COMMENT "Hive formated prtg Date Time"'
        elif field == 'id':
            comment = ' COMMENT "sensor id"'
        elif field in ColumnsName.keys():
            comment = ' COMMENT "'+ColumnsName[field].replace('"','_')+'"'

        fields += field + ' ' + type_str + comment +'\n'

        print(fields)

    sql=du.hive_sql_prefix('PRTG Sensors import') + \
        'drop table if exists default.prtg_sensors;\n' \
        'CREATE EXTERNAL TABLE default.prtg_sensors\n' \
        '(' + fields + ')\n' \
        'COMMENT "'+copyRightTableString+'"\n' \
        'PARTITIONED BY (year INT,month INT, day INT)\n' \
        'ROW FORMAT DELIMITED FIELDS TERMINATED BY "\\t"\n' \
        'STORED AS TEXTFILE LOCATION "/user/stcuscol/published/prtg_sensors/"\n' \
        'TBLPROPERTIES ("skip.header.line.count"="1");'
    print(sql)


    text_res = du.execute_hive_command(sql)
    res=True
    if 'FAILED' in text_res:
        res=False
        AddLog('ERROR: Executins Hive SQL: ' + text_res)
    AddLog('END: recreate_table_prtg_sensors: SQL=' + sql)
    return res


##########################################################################################################################################################
# Считываем данные по сенсору за день sdate: 2018-05-23
# возвращает DataFrame
##########################################################################################################################################################
def get_prtg_df(id,sdate):
    AddLog('STRAT: get_prtg_df: id='+str(id)+' sdate='+str(sdate))

    try:
        url_str_1 = 'https://10.1.1.248/public/checklogin.htm'
        # PROD - полный ответ с API
        url_str = 'https://10.1.1.248/api/historicdata.csv?id='+str(id)+'&avg=0&sdate='+sdate+'-00-00-00&edate='+sdate+'-23-59-59&username=ibm&passhash=1619417265'
        #url_str = 'https://prtg.mt.ru/api/historicdata.csv?id=' + str(id) + '&avg=0&sdate=' + sdate + '-00-00-00&edate=' + sdate + '-23-59-59&username=ibm&passhash=1619417265'
        # TEST - короткий ответ с API
        # url_str = 'http://10.1.1.248/api/historicdata.csv?id=' + str(id) + '&avg=0&sdate=' + sdate + '23-50-00&edate=' + sdate + '-23-59-59'
        ddd = {
            "loginurl": "",
            "username": prtg_login,
            "password": prtg_password
        }
        session = requests.Session()
        # сначала дергаем Login
        #r = session.request('POST', url_str_1, data=ddd, verify=False)
        #print(r.status_code)
        # print(r.headers['content-type'])

        # теперь API
        print(url_str)
        data = session.request('GET',url_str,verify=False)
        data = data.content.decode("utf-8") # bug fix
        #print(data.status_code)
        #print(data.headers['content-type'])

        # Преобразуем в DataFrame
        df = pd.read_csv(StringIO(data), sep=",")

        #print(df.columns)

        #Sums(of 719 values), , "21 282 957 MByte",, "",, "2 142 965 MByte",, "",, "19 139 991 MByte",, "",, "",
        #Averages(of 719 values), , "29 601 MByte",, "2 066 Mbit/s",, "2 980 MByte",, "208 Mbit/s",, "26 620 MByte",, "1 858 Mbit/s",, "100 %",

        # Проверяем последнюю строку - НЕ ДАТА, то дропаем
        while len(df)>0:
            try:
                dt.datetime.strptime(df['Date Time'][len(df)-1], '%d.%m.%Y %H:%M:%S')
                break
            except:
                df.drop(df.index[len(df)-1], inplace=True)
        #df = df[(df['Date Time'] != 'Sums') & (df['Date Time'] != 'Averages')]

        # Удаляем колонки
        for col in df.columns:
            if (not 'RAW' in col or col=='Date Time(RAW)' or col=='Coverage(RAW)') and col!='Date Time':
                df.drop(str(col), axis=1, inplace=True)
        # Переименовываем колонки
        NewColumns=[]
        ColumnsName={}
        for col in df.columns:
            AdaptedColName=col.replace('(RAW)','').replace('(#)','').replace('(','').replace(')','').strip(' \t\n\r').replace('Date Time','dt').replace(' ','_').replace('.','_').replace(',','_').lower()
            # Сохраняем изначальные имена - пойдут в структуру таблицы как COMMENT
            ColumnsName[AdaptedColName]=col
            NewColumns.append(AdaptedColName)
        df.columns=NewColumns


        # Null
        df = df.fillna(0)

        # Округляем
        desred_decimals = 0
        for col in df.columns:
            if col!='dt':
                df[col] = df[col].apply(lambda x: round(x, desred_decimals))
                df[col] = df[col].astype(np.int64)


        # Дата
        df['dt'] = df['dt'].apply(lambda x: dt.datetime.strptime(x,'%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'))

        # Id
        df['id'] = id
    except Exception as e:
        AddLog('ERRROR: get_prtg_df STOPED e=' + str(e))
        return None, 'ERRROR: get_prtg_df STOPED e=' + str(e)

    AddLog('END: get_prtg_df STRAT: len(df)='+str(len(df)))
    return df,ColumnsName

#############################################################
# Получение папки HDFS из названия файла
#############################################################
def get_HDFSPath(filename):
    rootPath='/user/stcuscol/published/prtg_sensors/'

    # 01234567890
    # 2016-10-01-IdSensor
    year = filename[0:4]
    month = filename[5:7]
    day = filename[8:10]

    if month[0] == '0':
        month = month[1]
    if day[0] == '0':
        day = day[1]

    return rootPath + 'year=' + year + '/month=' + month + '/day=' + day + '/'

##########################################################################################################################################################
#
##########################################################################################################################################################
def Start_PRTG_API(DayReduce,sennsors):
    ok_list=[]
    error_list=[]
    res = False
    # Определяем день
    sdate = dt.datetime.now().date() - dt.timedelta(days=DayReduce)
    sdate = sdate.strftime('%Y-%m-%d')
    AddLog('START: prtg_api: sdate='+str(sdate))

    s_AddLog('DayReduce = '+str(DayReduce))
    s_AddLog(sdate)

    prtg_files=[]

    for sensor_id in sennsors:
        AddLog('>>>>>>>>>> sensor_id=' + str(sensor_id))
        # Тело данных
        prtg_df,ColumnsName = get_prtg_df(sensor_id, sdate)
        if prtg_df is None:
            error_list.append(str(sensor_id)+' / '+ColumnsName)
            continue
        # Текущий спсиок полей в Hive default.prtg_sensors

        current_columns, current_type, current_comment = get_fields() # TODO get_fields()

        # Насытить словарь COMMENT ColumnsName Значениями из списка current_comment
        for row in range(0,len(current_columns)):
            if not current_columns[row] in ['dt','id']:
                if not current_columns[row] in ColumnsName.keys() and len(current_comment[row])!=0:
                    ColumnsName[current_columns[row]]=current_comment[row]

        # Порядок колонок + Ананлиз новых и пропущеных
        AddLog('В наборе: '+str(prtg_df.columns))
        AddLog('В таблице: '+str(current_columns))
        print(prtg_df.columns)

        existing_columns=[]
        absent_columns = []


        for cur_col in current_columns:
            if cur_col in prtg_df.columns:
                existing_columns.append(cur_col)
            else:
                existing_columns.append('')
                # Долбавляем пустую колонку вместо несуществующих полейTODO
                prtg_df[cur_col]='null'

        # dt и id сделать первыми
        if not 'dt' in current_columns:
            absent_columns.append('dt')
        if not 'id' in current_columns:
            absent_columns.append('id')

        for df_col in prtg_df.columns:
            if not df_col in ['dt','id']:
                if not df_col in current_columns:
                    absent_columns.append(df_col)

        AddLog('Существуют: ' + str(existing_columns))
        AddLog('Отсутствуют: ' + str(absent_columns))


        # # # Пересоздать табицу: Добавить absent_columns
        # if len(absent_columns)>0:
        #     res=recreate_table_prtg_sensors(current_columns + absent_columns,ColumnsName)
        # # Сохранить файл по шаблону: current_columns + absent_columns
        # prtg_df = prtg_df[current_columns + absent_columns]

        # Сохраняем в файл
        fn=sdate+'_'+str(sensor_id)
        prtg_files.append(fn)
        prtg_df.to_csv(root_path + fn, sep='\t', header=True, encoding='utf-8', index=False)

        ok_list.append(str(sensor_id))
        print(ok_list)

        # Перекидываем файл на UNIX машину

        for fn in prtg_files:
            du.put_to_sftp(root_path + fn)

        # Перекидываем файл на HDFS
        for fn in prtg_files:
            print(get_HDFSPath(fn))
            du.del_file_hdfs(fn,get_HDFSPath(fn))
            du.put_to_hdfs(fn,get_HDFSPath(fn))

        # Удаляем файлы локально и на UNIX машине
        for fn in prtg_files:
            du.remove_from_sftp(fn)
            # check_output("del " + root_path + fn, shell=True)
            check_output("rm " + root_path + fn, shell=True)

        prtg_files=[]

    # Пересоздаем партицию
    sql = du.hive_sql_prefix('PRTG Sensors import') + \
          'MSCK REPAIR TABLE default.prtg_sensors;'
    AddLog(du.execute_hive_command(sql))

    res=True
    return res,ok_list,error_list


##########################################################################################################################################################
##########################################################################################################################################################
##########################################################################################################################################################

def main(DayReduce,sennsors,email=True):


    s_AddLog('START at '+dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
    s_AddLog('Sensors count: '+str(len(sennsors)))

    res=True
    ok_list=[]
    error_list=[]
    try:
        res,ok_list,error_list=Start_PRTG_API(DayReduce,sennsors) # TODO надо только досюда
    except:
        return None

    sdate = dt.datetime.now().date() - dt.timedelta(days=DayReduce)
    sdate = sdate.strftime('%Y-%m-%d')
    mail_to = 'ibm@maximatelecom.ru'
    if DayReduce==0 and sennsors==[129511,129512]:
        # mail_to = 'ibm@maximatelecom.ru;a.korolkova@maximatelecom.ru'
        mail_to = 'andreaseuro@yandex.ru'
#    mail_to = 'ibm@maximatelecom.ru;p.bulavin@maximatelecom.ru'
    subject = u'PRTG API сбор данных'
    body = u'<h2>за ' + sdate + u'</h2><BR>'
#    body += u'Успешно отработано ТОЛЬКО ' + str(cnt_ok) + u' из ' + str(cnt_all) + u' скриптов ' + '<BR>'
#    body += status_body


    # Нотификации и и т.д.
    if len(error_list)==0 and res:
        # dpmi_db.mssql().UpdateTaskProperty(348, str(len(sennsors)), dt.datetime.now().date() - dt.timedelta(days=1))

        s_AddLog('>>>>>>>>>>>>>>>>>>>>> everything OK')

        subject = u'OK ' + subject
        body += u'Завершился успешно !!!!' + '<BR>'

        body += u'Список отработанных сенсоров:' + '<BR>'
        body += du.list_to_str(ok_list)
        s_AddLog('Список отработанных сенсоров:')
        s_AddLog(str(ok_list))
    else:
        email = True
        # mail_to = 'ibm@maximatelecom.ru;p.bulavin@maximatelecom.ru'
        mail_to='andreaseuro@yandex.ru'
        if DayReduce == 0 and sennsors == [129511, 129512, 131299]:
            mail_to='a.konstantinov@maximatelecom.ru'
            # mail_to = 'ibm@maximatelecom.ru;a.korolkova@maximatelecom.ru'

        subject=u'ОШИБКА '+subject
        if not res:
            s_AddLog('>>>>>>>>>>>>>>>>>>>>> Завершился с EXCEPTION ')
            body += u'Завершился с EXCEPTION !!!!' + '<BR>'

        body += u'Список отработанных сенсоров:' + '<BR>'
        body += du.list_to_str(ok_list)
        s_AddLog('Список отработанных сенсоров:')
        s_AddLog(str(ok_list))

        body += u'Список сенсоров с ошибкой:' + '<BR>'
        body += du.list_to_str(error_list)
        s_AddLog('Список сенсоров с ошибкой:')
        s_AddLog(str(error_list))

    # if email:
    #     dpmi_db.mssql().execute("insert into send_mail(mail_to,subject,body) values('" + mail_to + "','" + subject + "','" + body + "')")

    s_AddLog('END at ' + dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
    s_AddLog('')


if __name__ == "__main__":
    if len(sys.argv)==1:
        # ================================================================
        print ('Default start')

        try:
            '''
            for dr in range(342,372):
                sdate = dt.datetime.now().date() - dt.timedelta(days=dr)
                sdate = sdate.strftime('%Y-%m-%d')
                print(dr)
                print(sdate)
                # БЕЗ 93455, 115515 , 93457, 98481, 98483, 115529, 122054, 129511, 129512
                #
                #main(dr, [58526,44047,31310,36352,34972,32705,33681,34948,34990,63664,32427],email=False)
            '''
            #main(0, [129511])
            main(0, [129511,129512,58526,44047,31310,36352,34972,32705,33681,34948,34990,63664,32427,93455,93457,122054,115515,98481,98483,115529])
            # ПРОВАЛЫ
            #22-03-2018 28-03-2018
        except:
            s_AddLog('Ошибка запуска: main(1,default_sennsors)')

    elif len(sys.argv)==2 and sys.argv[1]=='-regular':
        # ================================================================
        print ('Dayly regular start')
        try:
            main(1,default_sennsors,False)
        except:
            s_AddLog('Ошибка запуска: main(1,default_sennsors)')
    elif len(sys.argv)==2 and sys.argv[1]=='-fifa':
        # ================================================================
        print ('Toodays fifa sensors')
        try:
            main(0,[129511,129512,131299])
        except:
            s_AddLog('Ошибка запуска: main(0,[129511,129512,131299])')
    else:
        # ================================================================
        print ('sys.argv list:')
        for arg in sys.argv:
            print(arg)
