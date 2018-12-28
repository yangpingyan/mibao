#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 15:41 
# @Author : yangpingyan@gmail.com

import pandas as pd
import os, json
from sql.sql import sql_connect

def sql_connect_mibao_rds():
    workdir = r'./sql'
    sql_file = os.path.join(workdir, 'sql_mibao.json')
    ssh_pkey = os.path.join(workdir, 'sql_pkey')
    sql_conn = sql_connect('mibao_rds', sql_file, ssh_pkey)
    # sql = '''SELECT * FROM `order` o WHERE o.deleted != 1 ORDER BY id DESC LIMIT 100;'''
    # df = pd.read_sql(sql, sql_conn)
    # print(df)
    return sql_conn

sql_conn = sql_connect_mibao_rds()
is_sql = True

def read_data(filename, features, is_sql=False):
    data_path = r'./data'
    # starttime = time.clock()
    if is_sql:
        sql = '''SELECT {} FROM `{}` o LIMIT 100;'''.format(",".join(features), filename)
        df = pd.read_sql(sql, sql_conn)
        df.to_csv(os.path.join(data_path, filename + '.csv'), index=False)
    else:
        df = pd.read_csv(os.path.join(r'./data', filename + '.csv'), encoding='utf-8', engine='python')
        df = df[features]
    # print(filename, time.clock() - starttime)
    return df

features = ['id', 'create_time']
df = read_data('order', features, is_sql=is_sql)
