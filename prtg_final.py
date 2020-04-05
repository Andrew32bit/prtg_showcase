# -*- coding: utf-8 -*-

import requests
import dpmi_utils as du
import datetime as dt
import pandas as pd
import numpy as np
import warnings
import os
import yaml

from impala import dbapi
from impala.util import as_pandas
from subprocess import check_output
from tqdm import tqdm
from io import StringIO
warnings.filterwarnings("ignore")

with open('creds.yaml') as f:
    doc=yaml.load(f)

user=doc['credentials']['userimpala']
password=doc['credentials']['passwordimpala']
prtg_login=doc['credentials']['prtg_login']
passhash=doc['credentials']['passhash']

def get_HDFSPath(filename):
    rootPath='/user/stcuscol/published/prtg_sensors_v2/'

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

def get_data(sensors_id,sdate,DayReduce):
    url_str = 'https://10.1.1.248/api/historicdata.csv?id='+str(sensors_id)+'&avg=0&sdate='+sdate+'-00-00-00&edate='+sdate+'-23-59-59&username={}&passhash={}'.format(prtg_login,passhash)
    session = requests.Session()
    data = session.request('GET',url_str,verify=False)
    data = data.content.decode("utf-8") # bug fix
    df = pd.read_csv(StringIO(data), sep=",")
    df['id']=sensors_id
    df.columns = [i.lower().replace(" ", '').replace('(#)', '') for i in df.columns]
    while len(df) > 0:
        try:
            dt.datetime.strptime(df['datetime'][len(df) - 1], '%d.%m.%Y %H:%M:%S')
            df['datetime'] = pd.to_datetime(df['datetime'], format='%d.%m.%Y %H:%M:%S').astype(int) / 10 ** 9
            df['datetime'] = df['datetime'].astype(int)
            break
        except:
            df.drop(df.index[len(df) - 1], inplace=True)
    return df


def data_for_kudu(df,default_list):
    dataframe_columns = df.columns
    list_exists = [i for i in dataframe_columns if i in default_list]  # присутствующие колонки
    absent_columns = [i for i in default_list if i not in list_exists]  # отсуствующие колонки
    df = df[list_exists].copy()
    for i in absent_columns:
        df[i] = np.nan
    df=df[default_list]
    return df


def processing_for_hdfs(sensor_id,sdate):
    root_path = './'
    fn=sdate+'_'+str(sensor_id)
    get_data(sensor_id,sdate,DayReduce).to_csv(root_path + fn, sep='\t', header=True, encoding='utf-8', index=False)

    # Перекидываем файл на UNIX машину
    du.put_to_sftp(root_path + fn)

    # Перекидываем файл на HDFS
    du.del_file_hdfs(fn,get_HDFSPath(fn))
    du.put_to_hdfs(fn,get_HDFSPath(fn))

    # Удаляем файлы локально и на UNIX машине
    du.remove_from_sftp(fn)
    check_output("rm " + root_path + fn, shell=True)

def put_all_sensors_to_hdfs(sensors_list):
    for i in tqdm(sensors_list):
        processing_for_hdfs(i, sdate)


def data_to_kudu(sensors_list):
    result = pd.DataFrame()
    for i in tqdm(sensors_list):
        df=get_data(i, sdate, DayReduce)
        df=data_for_kudu(df, default_list)
        result = result.append(df)
    return result

def insertDataToDB(D, user, password):
    D['id'] = D['id'].astype(int)
    D['traffictotal(volume)(raw)'] = D['traffictotal(volume)(raw)'].astype(str)
    D['traffictotal(speed)(raw)'] = D['traffictotal(speed)(raw)'].astype(str)

    D['trafficin(volume)(raw)'] = D['trafficin(volume)(raw)'].astype(str)
    D['trafficin(speed)(raw)'] = D['trafficin(speed)(raw)'].astype(str)
    D['trafficout(volume)(raw)'] = D['trafficout(volume)(raw)'].astype(str)

    D['trafficout(speed)(raw)'] = D['trafficout(speed)(raw)'].astype(str)
    D['fromlines(volume)(raw)'] = D['fromlines(volume)(raw)'].astype(str)
    D['tolines(volume)(raw)'] = D['tolines(volume)(raw)'].astype(str)
    D['coverage(raw)'] = D['coverage(raw)'].astype(str)

    D = D.fillna('')
    D = D[['datetime', 'id', 'traffictotal(volume)(raw)', 'traffictotal(speed)(raw)',
                    'trafficin(volume)(raw)', 'trafficin(speed)(raw)', 'trafficout(volume)(raw)',
                    'trafficout(speed)(raw)', 'fromlines(volume)(raw)', 'tolines(volume)(raw)', 'coverage(raw)']]

    conn_impala = dbapi.connect(host='10.1.4.48', port=21050, auth_mechanism='PLAIN',
                                user=user, password=password, kerberos_service_name=None)
    cursor = conn_impala.cursor()
    for i, row in tqdm(D.iterrows(),total=D.shape[0]):
        query = "INSERT INTO stg.prtg_sensors_v2 (`timestamp`, id, traffictotal_volume_raw, traffictotal_speed_raw, trafficin_volume_raw, trafficin_speed_raw, " \
                "trafficout_volume_raw, trafficout_speed_raw, fromlines_volume_raw, tolines_volume_raw,coverage_raw) VALUES ({})".format(tuple(row))
        cursor.execute(query)




if __name__ == "__main__":
    sdate = dt.datetime.now().date() - dt.timedelta(days=1) #дата  прошлых суток
    sdate = sdate.strftime('%Y-%m-%d')
    sensors_list = [129511, 129512, 58526, 44047, 31310, 36352, 34972, 32705, 33681, 34948, 34990, 63664, 32427, 93455,
                    93457, 122054, 115515, 98481, 98483, 115529] # список дефолтных сенсоров
    default_list = ['datetime', 'id', 'traffictotal(volume)(raw)', 'traffictotal(speed)(raw)',
                    'trafficin(volume)(raw)', 'trafficin(speed)(raw)', 'trafficout(volume)(raw)',
                    'trafficout(speed)(raw)', 'fromlines(volume)(raw)', 'tolines(volume)(raw)', 'coverage(raw)'] #список дефолтных колонок для инсерта в куду

    put_all_sensors_to_hdfs(sensors_list) #кладем данные в HDFS
    df=data_to_kudu(sensors_list)
    insertDataToDB(df, user=user, password=password) # инсерт данных в куду

# TODO вынести в переменные окружения Dockerfile когда будет AF.Также проверить рабочие креды на инсерт данных в Impala