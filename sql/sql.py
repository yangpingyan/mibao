#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 15:06 
# @Author : yangpingyan@gmail.com

from sshtunnel import SSHTunnelForwarder
import os
import pymysql
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def sql_connect(database_name, sql_file, ssh_pkey=None):
    '''连接数据库'''
    with open(sql_file, encoding='utf-8') as f:
        sql_info = json.load(f)

    ssh_host = sql_info['ssh_host']
    ssh_user = sql_info['ssh_user']
    sql_address = sql_info['sql_address']
    sql_user = sql_info['sql_user']
    sql_password = sql_info['sql_password']
    if ssh_pkey == None:
        sql_engine = create_engine(
            'mysql+pymysql://{}:{}@{}:3306/{}'.format(sql_user, sql_password, sql_address, database_name))
        print("Access MySQL directly")
    else:
        server = SSHTunnelForwarder((ssh_host, 22),  # ssh的配置
                                    ssh_username=ssh_user,
                                    ssh_pkey=ssh_pkey,
                                    remote_bind_address=(sql_address, 3306))
        server.start()
        sql_engine = create_engine(
            'mysql+pymysql://{}:{}@127.0.0.1:{}/{}'.format(sql_user, sql_password, server.local_bind_port, database_name))
        print("Access MySQL with SSH tunnel forward")

    return sql_engine

workdir = r'./'
sql_file = os.path.join(workdir, 'sql_mibao.json')
ssh_pkey = os.path.join(workdir, 'sql_pkey')
sql_engine = get_sql_engine()

def read_sql_query(sql):
    global sql_engine

    try:
        df = pd.read_sql_query(sql, sql_engine)
    except:
        sql_engine = get_sql_engine()
        df = pd.read_sql_query(sql, sql_engine)

    return df


