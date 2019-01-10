#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 15:41 
# @Author : yangpingyan@gmail.com

import pandas as pd
import os, json
from sql.sql import sql_connect
import numpy as np
import time
import re
from ds_utils import *

# to make output display better
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 1000)

# 全局变量定义
is_sql = False
data_path = r'./data'


def sql_connect_mibao_rds():
    workdir = r'./sql'
    sql_file = os.path.join(workdir, 'sql_mibao.json')
    ssh_pkey = os.path.join(workdir, 'sql_pkey')
    sql_conn = sql_connect('mibao_rds', sql_file, ssh_pkey)
    # sql = '''SELECT * FROM `order` o WHERE o.deleted != 1 ORDER BY id DESC LIMIT 100;'''
    # df = pd.read_sql(sql, sql_conn)
    # print(df)
    return sql_conn


global sql_conn
sql_conn = sql_connect_mibao_rds()


def read_sql_query(sql):
    global sql_conn
    try:
        df = pd.read_sql(sql, sql_conn)
    except:
        sql_conn = sql_connect_mibao_rds()
        df = pd.read_sql(sql, sql_conn)
    return df


# def read_data(filename, features, is_sql=False):
#     # starttime = time.clock()
#     if is_sql:
#         # sql = '''SELECT {} FROM `{}` o ;'''.format(",".join(features), filename)
#         sql = '''SELECT {} FROM `{}` o ORDER BY o.id DESC LIMIT 1000;'''.format(",".join(features), filename)
#         print(sql)
#         df = pd.read_sql(sql, sql_conn)
#         df.to_csv(os.path.join(data_path, filename + '.csv'), index=False)
#     else:
#         df = pd.read_csv(os.path.join(r'./data', filename + '.csv'), encoding='utf-8', engine='python')
#         df = df[features]
#     # print(filename, time.clock() - starttime)
#     return df


# 需要读取的数据库表
sql_tables = ['face_id', 'face_id_liveness', 'jimi_order_check_result', 'order', 'order_detail',
              'order_express', 'order_goods', 'order_phone_book', 'risk_order', 'tongdun', 'user', 'user_credit',
              'user_device', 'user_third_party_account', 'user_zhima_cert', 'credit_audit_order', 'risk_white_list']

# 数据库表中的相关字段
order_features = ['id', 'create_time', 'finished_time', 'canceled_time', 'received_time',
                  'delivery_time', 'order_number', 'merchant_id', 'merchant_name', 'user_id', 'goods_name',
                  'state', 'cost', 'discount', 'installment', 'rem_pay_num', 'pay_num', 'added_service', 'first_pay',
                  'first_pay_time', 'full', 'billing_method', 'pay_type', 'user_receive_time', 'bounds_example_id',
                  'bounds_example_name', 'goods_type', 'cancel_reason', 'cancel_mode', 'paid_amount',

                  'credit_check_author', 'lease_term', 'commented',
                  'daily_rent', 'accident_insurance',
                  'type', 'freeze_money',
                  'ip',
                  'releted', 'service_enable', 'exchange_enable',
                  'relet_appliable', 'order_type', 'delivery_way', 'buyouted',
                  'buyout_appliable', 'mac_address',
                  'device_type',
                  'stages', 'source', 'distance',
                  'disposable_payment_discount', 'disposable_payment_enabled',
                  'custom_lease', 'activity_id',
                  'credit_check_result', 'user_remark', 'original_daily_rent',
                  'merchant_store_id', 'deposit', 'deposit_type',
                  'hit_merchant_white_list', 'pick_up_merchant_store_id',
                  'fingerprint',
                  'hit_goods_white_list', 'buyout_coefficient',
                  'merchant_credit_check_result', 'disposable_payment_limit_day',
                  'instalment_pay_enable', 'select_disposable_payment_enabled',
                  'settlement']

user_features = ['id', 'create_time', 'head_image_url','code', 'recommend_code', 'regist_channel_type', 'share_callback',
                 'tag', 'phone']
face_id_features = ['user_id', 'status', 'liveness_status']
user_credit_features = ['user_id', 'cert_no', 'workplace', 'idcard_pros', 'occupational_identity_type',
                        'company_phone', 'cert_no_expiry_date', 'cert_no_json', ]
user_device_features = ['user_id', 'device_type', 'regist_device_info', 'regist_useragent', 'ingress_type']
order_express_features = ['order_id', 'zmxy_score', 'card_id', 'phone', 'company']
order_detail_features = ['order_id', 'order_detail']
order_goods_features = ['order_id', 'price', 'category', 'old_level']
risk_order_features = ['order_id', 'type', 'result', 'detail_json', 'remark']
tongdun_features = ['order_number', 'final_score', 'final_decision']
user_zhima_cert_features = ['user_id', 'status']
jimi_order_check_result_features = ['order_id', 'check_remark']
credit_audit_order_features = ['order_id', 'create_time', 'admin_name', 'state', 'remark', 'manual_check_start_time',
                               'manual_check_end_time', 'order_cancle_time']
order_xinyongzu_features = []
user_bonus_features = []
user_login_log_features = []
user_login_record_features = []
user_longitude_latitude_feature = []
user_wx_account_features = []
xiaobai_features = []
user_third_party_account_features = ['user_id', 'create_time']
face_id_liveness_features = ['order_id', 'status']
order_phone_book_features = ['order_id', 'phone_book']
risk_white_list_features = ['user_id']

# order中的state 分类
pass_state_values = ['pending_receive_goods', 'running', 'lease_finished', 'pending_send_goods',
                     'merchant_not_yet_send_canceled', 'buyout_finished', 'pending_user_compensate', 'repairing',
                     'express_rejection_canceled', 'pending_return', 'returning', 'return_goods', 'returned_received',
                     'relet_finished', 'pending_refund_deposit', 'exchange_goods', 'pending_relet_pay',
                     'pending_compensate_check', 'pending_buyout_pay']
failure_state_values = ['artificial_credit_check_unpass_canceled', 'return_overdue', 'running_overdue',
                        'system_credit_check_unpass_canceled',
                        'merchant_relet_check_unpass_canceled', 'merchant_credit_check_unpass_canceled']
unknow_state_values = ['pending_order_receiving', 'pending_jimi_credit_check', 'order_payment_overtime_canceled',
                       'pending_artificial_credit_check', 'user_canceled', 'pending_relet_start',
                       'pending_relet_check', 'pending_pay']

# print(list(set(state_values).difference(set(pass_state_values + failure_state_values))))

# 机器学习中使用到的特征
mibao_ml_features = ['merchant_id', 'pay_num', 'added_service',
                     'goods_type', 'lease_term', 'commented', 'accident_insurance', 'order_type', 'device_type',
                     'source', 'distance', 'disposable_payment_discount', 'disposable_payment_enabled',
                     'merchant_store_id', 'fingerprint', 'delivery_way', 'head_image_url', 'recommend_code',
                     'regist_channel_type', 'share_callback', 'tag', 'face_check', 'phone',
                     'company', 'company_phone', 'category', 'old_level', 'tongdun_result', 'guanzhu_result',
                     'bai_qi_shi_result', 'workplace', 'idcard_pros', 'occupational_identity_type', 'device_type_os',
                     'regist_device_info', 'ingress_type', 'baiqishi_score', 'zhima_cert_result', 'age', 'sex', 'zmf',
                     'xbf', 'final_score', 'final_decision', 'weekday', 'hour',
                     'price', 'cost',
                     'phone_book', 'face_live_check',
                     'bounds_example_id',
                     # 'account_num' #用到了事后数据，需处理下
                     # 'deposit', 'type',  #有押金的审核肯定通过
                     ]


def save_data(df, filename):
    df.to_csv(os.path.join(data_path, filename), index=False)


def read_data(table_name, features, field='order_id', field_value=None):
    starttime = time.clock()
    if field_value == None:
        if table_name in ['user_device', 'order_express', 'order_detail', 'order_phone_book',
                          'user_third_party_account']:
            sql = "SELECT {} FROM `{}` s ;".format(",".join(features), table_name, field)
        else:
            sql = "SELECT {} FROM `{}` s WHERE s.deleted != 1 ;".format(",".join(features), table_name, field)
    else:
        sql = "SELECT {} FROM `{}` o WHERE o.{} = {};".format(",".join(features), table_name, field, field_value)

    print(sql)
    df = read_sql_query(sql)
    print(table_name, time.clock() - starttime)
    return df


def get_all_data_mibao():
    df = get_order_data()
    save_data(df, "mibao_alldata.csv")
    # 保存注释内容
    sql = '''SELECT table_name, column_name, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = 'mibao_rds'; '''
    df = read_sql_query(sql)
    save_data(df, "mibao_comment.csv")

    return df


def process_data_tableau(df):
    pd.set_option('display.max_rows', 200)
    # 特殊字符串的列预先处理下：
    features = ['installment', 'commented', 'disposable_payment_enabled', 'face_check', 'face_live_check',
                'hit_merchant_white_list', 'instalment_pay_enable', 'releted', 'hit_goods_white_list',
                'full', 'service_enable', 'relet_appliable', 'buyouted', 'buyout_appliable',
                'custom_lease', 'liveness_status', 'select_disposable_payment_enabled']
    for feature in features:
        df[feature] = df[feature].astype(str)
        df[feature] = np.where(df[feature].str.contains('1', na=False), 1, 0)

    # 标注人工审核结果于target字段
    df['target'] = np.NAN
    df.loc[df['state_cao'].isin(['manual_check_fail']), 'target'] = 0
    df.loc[df['state_cao'].isin(['manual_check_success', 'self_check_success']), 'target'] = 1
    df.loc[df['state'].isin(pass_state_values), 'target'] = 1
    df.loc[df['state'].isin(failure_state_values), 'target'] = 0

    # 订单最终壮状态标注
    df['target_state'] = np.NAN
    df.loc[df['state'].isin(['system_credit_check_unpass_canceled']), 'target_state'] = '机审拒绝'
    df.loc[df['state'].isin(['artificial_credit_check_unpass_canceled']), 'target_state'] = '人审拒绝'
    df.loc[df['state'].isin(
        ['merchant_credit_check_unpass_canceled', 'merchant_relet_check_unpass_canceled']), 'target_state'] = '商户审核拒绝'
    normal_order = ['pending_receive_goods', 'running', 'pending_send_goods',
                    'merchant_not_yet_send_canceled', 'pending_user_compensate', 'repairing',
                    'express_rejection_canceled', 'pending_return', 'returning', 'return_goods',
                    'returned_received',
                    'pending_refund_deposit', 'exchange_goods', 'pending_relet_pay',
                    'pending_compensate_check', 'pending_buyout_pay']
    df.loc[df['state'].isin(normal_order), 'target_state'] = '运行中订单'
    df.loc[df['state'].isin(['lease_finished', 'buyout_finished', 'relet_finished']), 'target_state'] = '完成'
    df.loc[df['state'].isin(['return_overdue', 'running_overdue']), 'target_state'] = '逾期'
    df.loc[df['state'].isin(['order_payment_overtime_canceled']), 'target_state'] = '付款超时'
    df.loc[df['state'].isin(['pending_artificial_credit_check']), 'target_state'] = '未处理-等待人工处理'
    df.loc[df['state'].isin(['pending_order_receiving', 'pending_jimi_credit_check', 'pending_relet_start',
                             'pending_relet_check']), 'target_state'] = '未处理'
    df.loc[df['state'].isin(['user_canceled']), 'target_state'] = '用户取消'
    df.loc[df['state'].isin(['pending_pay']), 'target_state'] = '等待支付'
    # feature_analyse(df, "cancel_reason")

    # 熟人订单类型
    df['order_acquaintance'] = '陌生用户'
    df.loc[df['cancel_reason'].str.contains('内部', na=False), 'order_acquaintance'] = '内部员工'
    df.loc[df['check_remark'].str.contains('内部', na=False), 'check_remark'] = '内部员工'
    df.loc[df['hit_merchant_white_list'] == 1, 'order_acquaintance'] = '商户白名单'

    def get_baiqishi_score(x):
        ret = 0
        if isinstance(x, type('str')):
            ret_list = re.findall(r'final\w+core.:[\'\"]?([\d]+)', x)
            ret = int(ret_list[0]) if len(ret_list) > 0 else 0
        return ret

    df['baiqishi_score'] = df['bai_qi_shi_detail_json'].map(lambda x: get_baiqishi_score(x))

    # 时间处理
    features_time = ['create_time', 'finished_time', 'canceled_time', 'received_time', 'delivery_time',
                     'manual_check_start_time', 'manual_check_end_time', 'order_cancle_time']
    for feature in features_time:
        df[feature] = pd.to_datetime(df[feature])

    # tableau 不支持64位数据
    df['canceled_time_interval'] = (df['canceled_time'] - df['create_time']) / np.timedelta64(1, 'm')
    df['canceled_time_interval'] = df['canceled_time_interval'].fillna(0).astype(int)
    df['manual_check_end_time_interval'] = (df['manual_check_end_time'] - df['create_time']) / np.timedelta64(1, 'm')
    df['manual_check_end_time_interval'] = df['manual_check_end_time_interval'].fillna(0).astype(int)
    df['delivery_time_interval'] = (df['received_time'] - df['delivery_time']) / np.timedelta64(1, 'm')
    df['delivery_time_interval'] = df['delivery_time_interval'].fillna(0).astype(int)

    # 取phone前3位
    df['phone'][df['phone'].isnull()] = df['phone_user'][df['phone'].isnull()]
    df['phone'].fillna(value='0', inplace=True)
    df['phone'][df['phone'].str.len() != 11] = '0'
    df['phone'] = df['phone'].str.slice(0, 3)

    # 数据处理


    # 只判断是否空值的特征处理
    features_cat_null = ['fingerprint', 'company', 'company_phone', 'workplace', 'idcard_pros', ]
    for feature in features_cat_null:
        df[feature].fillna(0, inplace=True)
        df[feature] = np.where(df[feature].isin(['', ' ', 0]), 0, 1)


    df['head_image_url'].fillna(value=0, inplace=True)
    df['head_image_url'] = df['head_image_url'].map(
        lambda x: 0 if x == ("headImg/20171126/ll15fap1o16y9zfr0ggl3g8xptgo80k9jbnp591d.png") or x == 0 else 1)

    # df['share_callback'] = np.where(df['share_callback'] < 1, 0, 1)
    # df['tag'] = np.where(df['tag'].str.match('new'), 1, 0)
    df['final_score'].fillna(value=0, inplace=True)

    df['cert_no'][df['cert_no'].isnull()] = df['card_id'][df['cert_no'].isnull()]
    # 有45个身份证号缺失但审核通过的订单， 舍弃不要。
    # df = df[df['cert_no'].notnull()]

    # 处理芝麻信用分 '>600' 更改成600
    df['xiaobaiScore'] = df['order_detail'].map(
        lambda x: json.loads(x).get('xiaobaiScore', 0) if isinstance(x, str) else 0)
    df['zmxyScore'] = df['order_detail'].map(lambda x: json.loads(x).get('zmxyScore', 0) if isinstance(x, str) else '0')
    df['xiaobaiScore'] = df['xiaobaiScore'].map(lambda x: float(x) if str(x) > '0' else 0)
    df['zmxyScore'] = df['zmxyScore'].map(lambda x: float(x) if str(x) > '0' else 0)

    df['zmxy_score'][df['zmxy_score'].isin(['', ' '])] = 0
    zmf = [0.0] * len(df)
    xbf = [0.0] * len(df)
    for row, detail in enumerate(df['zmxy_score'].tolist()):
        # print(row, detail)
        if isinstance(detail, type('hh')):
            if '/' in detail:
                score = detail.split('/')
                xbf[row] = 0 if score[0] == '' else (float(score[0]))
                zmf[row] = 0 if score[1] == '' else (float(score[1]))
            # print(score, row)
            elif '>' in detail:
                zmf[row] = 600
            else:
                score = float(detail)
                if score <= 200:
                    xbf[row] = score
                else:
                    zmf[row] = score

    df['zmf'] = zmf
    df['xbf'] = xbf

    df['zmf'][df['zmf'] == 0] = df['zmxyScore'][df['zmf'] == 0].astype(float)  # 26623
    df['xbf'][df['xbf'] == 0] = df['xiaobaiScore'][df['xbf'] == 0].astype(float)  # 26623
    df['zmf'].fillna(value=0, inplace=True)
    df['xbf'].fillna(value=0, inplace=True)
    # zmf_most = df['zmf'][df['zmf'] > 0].value_counts().index[0]
    # xbf_most = df['xbf'][df['xbf'] > 0].value_counts().index[0]
    df['zmf'][df['zmf'] == 0] = 600  # zmf_most
    df['xbf'][df['xbf'] == 0] = 87.6  # xbf_most

    # 根据身份证号增加性别和年龄 年龄的计算需根据订单创建日期计算
    df['age'] = df['create_time'].dt.year - df['cert_no'].str.slice(6, 10).astype(int)
    df['sex'] = df['cert_no'].str.slice(-2, -1).astype(int) % 2


    df['added_service'] = df['added_service'].str.count('insuranceName')
    df['added_service'].fillna(0, inplace=True)
    df['added_service'] = df['added_service'].astype(int)
    df['recommend_code'].value_counts()

    # 已处理的特征
    df.drop(['cancel_reason', 'check_remark', 'bai_qi_shi_detail_json', 'zmxy_score', 'xiaobaiScore', 'zmxyScore'], axis=1, inplace=True, errors='ignore')
    return df


# def bit_process(df: pd.DataFrame):
#     df = df.astype(str)
#     df.fillna('0', inplace=True)
#     df = np.where(df.str.contains('1', na=False), 1, 0)
#     return df


def get_order_data(order_id=None):
    # 变量初始化
    order_id = None
    user_id = None
    order_number = None
    print("读取order表")
    order_df = read_data('order', order_features, 'id', order_id)
    if len(order_df) == 0:
        return order_df
    order_df.rename(columns={'id': 'order_id'}, inplace=True)
    if order_id != None:
        user_id = order_df.at[0, 'user_id']
        order_number = order_df.at[0, 'order_number']
    all_data_df = order_df.copy()

    # 若state字段有新的状态产生， 抛出异常
    state_values_newest = all_data_df['state'].unique().tolist()
    state_values = pass_state_values + failure_state_values + unknow_state_values
    print("state产生新字段： ", list(set(state_values_newest).difference(set(state_values))))
    assert (len(list(set(state_values_newest).difference(set(state_values)))) == 0)

    # 读取并处理表 user
    user_df = read_data('user', user_features, 'id', user_id)
    user_df.rename(columns={'id': 'user_id', 'phone': 'phone_user', 'create_time': 'create_time_user'}, inplace=True)
    all_data_df = pd.merge(all_data_df, user_df, on='user_id', how='left')

    # 读取并处理表 face_id
    face_id_df = read_data('face_id', face_id_features, 'user_id', user_id)
    face_id_df.rename(columns={'status': 'face_check'}, inplace=True)
    all_data_df = pd.merge(all_data_df, face_id_df, on='user_id', how='left')

    # 读取并处理表 face_id_liveness
    face_id_liveness_df = read_data('face_id_liveness', ['order_id', 'status'], 'order_id', order_id)
    face_id_liveness_df.rename(columns={'status': 'face_live_check'}, inplace=True)
    all_data_df = pd.merge(all_data_df, face_id_liveness_df, on='order_id', how='left')

    # 读取并处理表 user_credit
    user_credit_df = read_data('user_credit', user_credit_features, 'user_id', user_id)
    all_data_df = pd.merge(all_data_df, user_credit_df, on='user_id', how='left')

    # 读取并处理表 user_device
    user_device_df = read_data('user_device', user_device_features, 'user_id', user_id)
    user_device_df.rename(columns={'device_type': 'device_type_os'}, inplace=True)
    all_data_df = pd.merge(all_data_df, user_device_df, on='user_id', how='left')

    # 读取并处理表 order_express
    # 未处理特征：'country', 'provice', 'city', 'regoin', 'receive_address', 'live_address'
    order_express_df = read_data('order_express', order_express_features, 'order_id', order_id)
    order_express_df.drop_duplicates(subset='order_id', inplace=True)
    all_data_df = pd.merge(all_data_df, order_express_df, on='order_id', how='left')

    # 读取并处理表 order_detail
    order_detail_df = read_data('order_detail', order_detail_features, 'order_id', order_id)
    all_data_df = pd.merge(all_data_df, order_detail_df, on='order_id', how='left')

    # 读取并处理表 order_goods
    order_goods_df = read_data('order_goods', order_goods_features, 'order_id', order_id)
    order_goods_df.drop_duplicates(subset='order_id', inplace=True)
    all_data_df = pd.merge(all_data_df, order_goods_df, on='order_id', how='left')

    # 读取并处理表 order_phone_book
    def count_name_nums(data):
        name_list = []
        if isinstance(data, str):
            data_list = json.loads(data)
            for phone_book in data_list:
                if len(phone_book.get('name')) > 0 and phone_book.get('name').isdigit() is False:
                    name_list.append(phone_book.get('name'))

        return len(set(name_list))

    order_phone_book_df = read_data('order_phone_book', ['order_id', 'phone_book'], 'order_id', order_id)
    order_phone_book_df['phone_book'] = order_phone_book_df['phone_book'].map(count_name_nums)

    all_data_df = pd.merge(all_data_df, order_phone_book_df, on='order_id', how='left')
    all_data_df['phone_book'].fillna(value=0, inplace=True)

    # 读取并处理表 risk_order
    risk_order_df = read_data('risk_order', risk_order_features, 'order_id', order_id)
    risk_order_df['result'] = risk_order_df['result'].str.lower()
    for risk_type in ['tongdun', 'mibao', 'guanzhu', 'bai_qi_shi']:
        tmp_df = risk_order_df[risk_order_df['type'].str.match(risk_type)][
            ['order_id', 'result', 'detail_json', 'remark']]
        tmp_df.rename(
            columns={'result': risk_type + '_result', 'detail_json': risk_type + '_detail_json',
                     'remark': risk_type + '_remark'},
            inplace=True)
        all_data_df = pd.merge(all_data_df, tmp_df, on='order_id', how='left')
    # 读取并处理表 tongdun
    tongdun_df = read_data('tongdun', tongdun_features, 'order_number', order_number)
    all_data_df = pd.merge(all_data_df, tongdun_df, on='order_number', how='left')

    # 读取并处理表 user_third_party_account
    user_third_party_account_df = read_data('user_third_party_account', ['user_id'], 'user_id', user_id)
    counts_df = pd.DataFrame({'account_num': user_third_party_account_df['user_id'].value_counts()})
    counts_df['user_id'] = counts_df.index
    all_data_df = pd.merge(all_data_df, counts_df, on='user_id', how='left')

    # 读取并处理表 user_zhima_cert
    df = read_data('user_zhima_cert', user_zhima_cert_features, 'user_id', user_id)
    all_data_df['zhima_cert_result'] = np.where(all_data_df['user_id'].isin(df['user_id'].tolist()), 1, 0)

    # 读取并处理表 jimi_order_check_result
    df = read_data('jimi_order_check_result', jimi_order_check_result_features, 'order_id', order_id)
    all_data_df = pd.merge(all_data_df, df, on='order_id', how='left')

    # 读取并处理表 credit_audit_order
    df = read_data('credit_audit_order', credit_audit_order_features, 'order_id', order_id)
    df.rename(columns={'state': 'state_cao', 'remark': 'remark_cao', 'create_time': 'create_time_credit_audit'},
              inplace=True)
    all_data_df = pd.merge(all_data_df, df, on='order_id', how='left')

    # 去除测试数据和内部员工数据
    all_data_df = all_data_df[all_data_df['cancel_reason'].str.contains('测试', na=False) != True]
    all_data_df = all_data_df[all_data_df['check_remark'].str.contains('测试', na=False) != True]

    all_data_df.drop(['order_number'], axis=1, inplace=True, errors='ignore')
    # df.drop(['description'], axis=1, inplace=True, errors='ignore')
    # detail_json 暂不处理
    all_data_df.drop(
        ['tongdun_detail_json', 'guanzhu_detail_json', 'mibao_detail_json', 'cert_no_json', 'mibao_remark'],
        axis=1, inplace=True, errors='ignore')

    return all_data_df


def feature_analyse(df, feature):
    print(df[df[feature].notnull()])
    print(comment_df[comment_df['column_name'] == feature])
    print("数据类型:", df[feature].dtype)
    print("空值率： {:.2f}%".format(100 * df[feature].isnull().sum() / len(df[feature])))
    print("value_counts: \r", df[feature].value_counts())

    print("数据的趋势、集中趋势、紧密程度")
    print("数据分布、密度")
    print("数据相关性、周期性")
    print("统计作图，更直观的发现数据的规律：折线图、直方图、饼形图、箱型图、对数图形、误差条形图")


comment_df = pd.read_csv(os.path.join(data_path, "mibao_comment.csv"), encoding='utf-8', engine='python')

# In[]
df = pd.read_csv(os.path.join(data_path, "mibao_alldata.csv"), encoding='utf-8', engine='python')
all_data_df = df.copy()
df = process_data_tableau(df)

save_data(df, 'mibao.csv')
feature_analyse(df, "recommend_code")
'''
df = all_data_df.copy()
'''
