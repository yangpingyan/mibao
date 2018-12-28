#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 15:41 
# @Author : yangpingyan@gmail.com

import pandas as pd
import os, json
from sql.sql import sql_connect
import numpy as np

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
is_sql = False

def read_data(filename, features, is_sql=False):
    data_path = r'./data'
    # starttime = time.clock()
    if is_sql:
        sql = '''SELECT {} FROM `{}` o ;'''.format(",".join(features), filename)
        # sql = '''SELECT {} FROM `{}` o ORDER BY o.id DESC LIMIT 100;'''.format(",".join(features), filename)
        print(sql)
        df = pd.read_sql(sql, sql_conn)
        df.to_csv(os.path.join(data_path, filename + '.csv'), index=False)
    else:
        df = pd.read_csv(os.path.join(r'./data', filename + '.csv'), encoding='utf-8', engine='python')
        df = df[features]
    # print(filename, time.clock() - starttime)
    return df

def save_data(df, filename):
    data_path = r'./data'
    df.to_csv(os.path.join(data_path, filename), index=False)

# 保存注释内容
sql = '''SELECT table_name, column_name, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns; '''
df = pd.read_sql(sql, sql_conn)
save_data(df, "mibao_comment.csv")

order_features = ['id', 'create_time', 'deleted',
      'lease_start_time',
       'lease_expire_time', 'finished_time', 'canceled_time',
       'received_time', 'delivery_time', 'last_pay_time', 'buyout_time',
       'order_number', 'merchant_id', 'merchant_name', 'user_id',
       'user_name', 'goods_name', 'goods_sn', 'state', 'cost', 'discount',
       'installment', 'next_pay_time', 'rem_pay_num',
       'pay_num', 'added_service',
       'first_pay', 'first_pay_time', 'full', 'billing_method',
       'liquidated_damages_percent', 'buffer_days', 'channel', 'pay_type',
       'user_receive_time', 'reminded', 'bounds_example_id',
       'bounds_example_name', 'bounds_example_no', 'goods_type',
       'cash_pledge', 'cancel_reason',
       'cancel_mode', 'clearance_time', 'freight', 'paid_amount',
       'credit_check_author', 'reminder_time', 'lease_term', 'commented',
       'daily_rent', 'accident_insurance',
       'description', 'type', 'freeze_money', 'sign_state',
       'best_sign_channel', 'doc_id', 'handheld_photo', 'ip', 'pid',
       'releted', 'service_enable', 'exchange_enable',
       'relet_appliable', 'order_type', 'delivery_way', 'buyouted',
       'buyout_appliable', 'mac_address',
       'imei', 'device_type', 'joke', 'hand_id_card', 'id_card_pros',
       'id_card_cons', 'stages', 'source', 'distance',
       'disposable_payment_discount', 'disposable_payment_enabled',
       'custom_lease', 'activity_id', 'lease_num',
       'credit_check_result', 'user_remark', 'original_daily_rent',
       'merchant_store_id', 'deposit', 'deposit_type',
       'hit_merchant_white_list', 'pick_up_merchant_store_id',
       'receive_merchant_store_id', 'api_version', 'fingerprint',
       'finished_state', 'hit_goods_white_list', 'buyout_coefficient',
       'merchant_credit_check_result', 'disposable_payment_limit_day',
       'instalment_pay_enable', 'select_disposable_payment_enabled',
       'settlement', 'settlement_transaction_no']
df = read_data('order', order_features, is_sql=is_sql)
all_data_df = df.copy()
# 特殊字符串的列预先处理下：
df = all_data_df
features = ['deleted', 'installment', 'commented', 'disposable_payment_enabled', 'joke']
for feature in features:
    # print(all_data_df[feature].value_counts())
    df[feature] = df[feature].astype(str)
    df[feature].fillna('0', inplace=True)
    df[feature] = np.where(df[feature].str.contains('1'), 1, 0)
df = df[df['deleted'] != 1]
df['create_time'] = pd.to_datetime(df['create_time'])
save_data(df, 'mibao.csv')

df['create_time'] = pd.to_datetime(df['create_time'])
df['year'] = df['create_time'].map(lambda x: x.year)
df['year'].value_counts()
# df = pd.read_sql('''SELECT * FROM `order` o LIMIT 100;''', sql_conn)
# df.columns.values