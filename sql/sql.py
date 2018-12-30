#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 15:06 
# @Author : yangpingyan@gmail.com

import os
import json
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder
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
        sql_conn = create_engine(
            'mysql+pymysql://{}:{}@{}:3306/{}'.format(sql_user, sql_password, sql_address, database_name))
        print("Access MySQL directly")
    else:
        server = SSHTunnelForwarder((ssh_host, 22),  # ssh的配置
                                    ssh_username=ssh_user,
                                    ssh_pkey=ssh_pkey,
                                    remote_bind_address=(sql_address, 3306))
        server.start()
        sql_conn = create_engine(
            'mysql+pymysql://{}:{}@127.0.0.1:{}/{}'.format(sql_user, sql_password, server.local_bind_port, database_name))
        print("Access MySQL with SSH tunnel forward")
        if True:
            sql_conn = pymysql.Connect(
                host="127.0.0.1",
                port=server.local_bind_port,
                user=sql_user,
                password=sql_password,
                db=database_name,
                charset='utf8'
            )
        else:
            sql_conn = create_engine(
                'mysql+pymysql://{}:{}@127.0.0.1:{}/{}'.format(sql_user, sql_password, server.local_bind_port,
                                                               database_name))
    return sql_conn

if __name__ == '__main__':

    workdir = r'./sql'
    sql_file = os.path.join(workdir, 'sql_mibao.json')
    ssh_pkey = os.path.join(workdir, 'sql_pkey')
    sql_conn = sql_connect('mibao_rds', sql_file, ssh_pkey)
    sql = '''SELECT * FROM `order` o WHERE o.deleted != 1 ORDER BY id DESC LIMIT 100;'''
    df = pd.read_sql(sql, sql_conn)
    print(df)




