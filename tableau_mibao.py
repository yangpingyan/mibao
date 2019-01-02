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


sql_conn = sql_connect_mibao_rds()


def read_sql_query(sql):
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
order_features = ['id', 'create_time', 'lease_start_time', 'lease_expire_time', 'finished_time',
                  'canceled_time', 'received_time', 'delivery_time', 'last_pay_time', 'order_number', 'merchant_id',
                  'merchant_name', 'user_id', 'user_name', 'goods_name',
                  'state', 'cost', 'discount',
                  'installment', 'next_pay_time', 'rem_pay_num',
                  'pay_num', 'added_service',
                  'first_pay', 'first_pay_time', 'full', 'billing_method',
                  'liquidated_damages_percent', 'channel', 'pay_type',
                  'user_receive_time', 'reminded', 'bounds_example_id',
                  'bounds_example_name', 'bounds_example_no', 'goods_type',
                  'cash_pledge', 'cancel_reason',
                  'cancel_mode', 'clearance_time', 'paid_amount',
                  'credit_check_author', 'lease_term', 'commented',
                  'daily_rent', 'accident_insurance',
                  'description', 'type', 'freeze_money', 'sign_state',
                  'handheld_photo', 'ip', 'pid',
                  'releted', 'service_enable', 'exchange_enable',
                  'relet_appliable', 'order_type', 'delivery_way', 'buyouted',
                  'buyout_appliable', 'mac_address',
                  'device_type', 'hand_id_card', 'id_card_pros',
                  'id_card_cons', 'stages', 'source', 'distance',
                  'disposable_payment_discount', 'disposable_payment_enabled',
                  'custom_lease', 'activity_id', 'lease_num',
                  'credit_check_result', 'user_remark', 'original_daily_rent',
                  'merchant_store_id', 'deposit', 'deposit_type',
                  'hit_merchant_white_list', 'pick_up_merchant_store_id',
                  'receive_merchant_store_id', 'fingerprint',
                  'finished_state', 'hit_goods_white_list', 'buyout_coefficient',
                  'merchant_credit_check_result', 'disposable_payment_limit_day',
                  'instalment_pay_enable', 'select_disposable_payment_enabled',
                  'settlement', 'settlement_transaction_no']

user_features = ['id', 'create_time', 'head_image_url', 'recommend_code', 'regist_channel_type', 'share_callback',
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


def process_data_mibao(df):
    # 取phone前3位
    df['phone'][df['phone'].isnull()] = df['phone_user'][df['phone'].isnull()]
    df['phone'].fillna(value='0', inplace=True)
    df['phone'][df['phone'].str.len() != 11] = '0'
    df['phone'] = df['phone'].str.slice(0, 3)

    phone_list = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '147',
                  '150', '151', '152', '153', '155', '156', '157', '158', '159', '166', '170', '171',
                  '173', '175', '176', '177', '178', '180', '181', '182', '183', '184', '185', '186',
                  '187', '188', '189', '198', '199']
    type_list = ['DEPOSIT_ORDER', 'LEASE_ORDER', 'PCREDIT_FREEZE_ORDER', 'RELET_ORDER']
    source_list = ['aliPay', 'alipayMiniProgram', 'android', 'ios', 'jd', 'saas', 'weChat', 'weChatMiniProgram']
    merchant_store_id_list = [22.0, 36.0, 40.0, 42.0, 43.0, 45.0, 46.0, 47.0, 48.0, 49.0, 52.0, 53.0, 54.0,
                              55.0, 56.0, 60.0, 62.0, 67.0, 70.0, 72.0, 73.0, 75.0, 76.0, 77.0, 81.0, 85.0, 131.0,
                              137.0, 139.0, 140.0, 142.0, 144.0, 145.0, 146.0, 149.0, 151.0, 155.0, 161.0, 162.0, 168.0,
                              170.0, 171.0, 172.0, 173.0, 176.0, 186.0, 196.0, 197.0, 199.0, 200.0, 201.0, 204.0, 207.0,
                              209.0, 214.0, 450.0, 452.0, 453.0, 455.0, 464.0, 465.0, 467.0, 468.0, 470.0, 471.0, 472.0,
                              473.0, 476.0, 478.0, 481.0, 482.0, 483.0, 485.0, 489.0, 491.0, 496.0, 19900002.0,
                              47800001.0]
    device_type_list = ['OTHER', 'android', 'h5', 'ios', 'web']
    goods_type_list = ['VR眼睛', 'VR眼镜', '一体机', '健康监测', '光碟', '其他玩具', '其他生活电器', '剃须刀', '办公配件', '医疗健康', '厨房电器', '口腔护理',
                       '台式电脑', '台球', '吸尘器/除螨器', '品质冰箱', '品质生活', '品质音响', '女神节专区', '安卓专区', '安卓手机', '平板', '平板电脑', '平衡车',
                       '户外旅行', '手机', '手机配件', '手表', '打印机', '扫地机器人', '投影仪', '新人专区', '无人机', '无人飞机', '早教益智', '时尚手表', '时尚箱包',
                       '时尚耳机', '显示器', '智力开发', '智能出行', '智能手表', '智能电视', '智能硬件', '洁面仪', '洗衣机', '洗衣神器', '游戏主机', '游戏光碟',
                       '游戏电竞', '潮流相机', '灭蚊器', '爆款推荐', '玩具', '电动摩托车', '电动汽车', '电动车', '电吹风', '电子阅读', '电视机', '相机配件',
                       '相机镜头', '积木王国', '移动硬盘', '空气净化器', '笔记本', '绿色出行', '美容仪', '翻译机', '耳机', '苹果专区', '苹果手机', '路由器',
                       '运动器材', '酷乐玩具', '音乐播放器', '音响', '鼠标键盘']
    merchant_id_list = [22, 24, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
                        55, 56, 58, 59, 60, 61, 62, 63, 65, 67, 68, 70, 71, 72, 73, 74, 75, 76, 77, 81, 85, 131, 137,
                        139, 140, 142, 144, 145, 146, 149, 150, 151, 155, 161, 162, 168, 170, 171, 172, 173, 176, 186,
                        188, 192, 193, 195, 196, 197, 199, 200, 201, 204, 207, 209, 214, 274, 299, 414, 450, 452, 453,
                        455, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 470, 471, 472, 473, 476, 478,
                        479, 481, 482, 483, 485, 489, 491, 496]
    order_type_list = ['COMMON', 'PUSHING']
    regist_channel_type_list = [0.0, 1.0, 2.0, 3.0, 4.0, 105.0, 112.0, 113.0, 117.0]
    occupational_identity_type_list = ['civil_servant', 'company_clerk', 'company_manager',
                                       'enterprises_clerk', 'other', 'public_institution', 'teacher']
    ingress_type_list = ['APP', 'WEB']
    device_type_os_list = ['ANDROID', 'APPLE', 'OTHER', 'ios']
    bai_qi_shi_result_list = ['accept', 'reject', 'review']
    guanzhu_result_list = ['命中', '未命中']
    tongdun_result_list = ['pass', 'reject', 'review']
    delivery_way_list = ['PRIVATE_STORE', 'TO_DOOR_SERVICE']
    old_level_list = ['7成新', '8成新', '9成新', '二手', '全新', '非全新']
    category_list = ['个人护理', '休闲游戏', '优享电脑', '免租专区', '出行', '办公', '办公设备', '女神节专区', '家用电器', '家电', '影音娱乐', '手机', '数码',
                     '新人专区', '时尚手机', '智能学习', '游戏', '潮流数码', '潮玩', '生活', '电脑', '益智玩具', '绿色出行', '运动户外', '高端奢侈']

    final_decision_list = ['拒绝', '通过', '需评估']

    features_cat = ['type', 'source', 'merchant_store_id',
                    'device_type', 'goods_type', 'merchant_id', 'order_type', 'regist_channel_type',
                    'occupational_identity_type', 'ingress_type', 'device_type_os',
                    'bai_qi_shi_result', 'guanzhu_result', 'tongdun_result', 'delivery_way', 'old_level', 'category',
                    'final_decision', 'phone']

    for feature in features_cat:
        feature_list = eval(feature + '_list')
        feature_dict = dict(zip(feature_list, range(1, len(feature_list) + 1)))
        df[feature] = df[feature].map(lambda x: feature_dict.get(x, 0))

    # 数据处理
    df['xiaobaiScore'] = df['order_detail'].map(
        lambda x: json.loads(x).get('xiaobaiScore', 0) if isinstance(x, str) else 0)
    df['zmxyScore'] = df['order_detail'].map(lambda x: json.loads(x).get('zmxyScore', 0) if isinstance(x, str) else '0')
    df['xiaobaiScore'] = df['xiaobaiScore'].map(lambda x: float(x) if str(x) > '0' else 0)
    df['zmxyScore'] = df['zmxyScore'].map(lambda x: float(x) if str(x) > '0' else 0)

    # 只判断是否空值的特征处理
    features_cat_null = ['bounds_example_id', 'distance', 'fingerprint', 'added_service',
                         'recommend_code', 'regist_device_info', 'company', 'company_phone', 'workplace',
                         'idcard_pros', ]
    for feature in features_cat_null:
        df[feature].fillna(0, inplace=True)
        df[feature] = np.where(df[feature].isin(['', ' ', 0]), 0, 1)

    df['deposit'] = np.where(df['deposit'] == 0, 0, 1)

    df['head_image_url'].fillna(value=0, inplace=True)
    df['head_image_url'] = df['head_image_url'].map(
        lambda x: 0 if x == ("headImg/20171126/ll15fap1o16y9zfr0ggl3g8xptgo80k9jbnp591d.png") or x == 0 else 1)

    df['share_callback'] = np.where(df['share_callback'] < 1, 0, 1)
    df['tag'] = np.where(df['tag'].str.match('new'), 1, 0)
    df['account_num'].fillna(value=0, inplace=True)
    df['final_score'].fillna(value=0, inplace=True)

    df['cert_no'][df['cert_no'].isnull()] = df['card_id'][df['cert_no'].isnull()]
    # 有45个身份证号缺失但审核通过的订单， 舍弃不要。
    df = df[df['cert_no'].notnull()]

    # 处理芝麻信用分 '>600' 更改成600
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

    # order_id =9085, 9098的crate_time 是错误的
    df = df[df['create_time'] > '2016']
    # 把createtime分成月周日小时
    df['create_time'] = pd.to_datetime(df['create_time'])
    df['year'] = df['create_time'].map(lambda x: x.year)
    # df['month'] = df['create_time'].map(lambda x: x.month)
    df['day'] = df['create_time'].map(lambda x: x.day)
    df['weekday'] = df['create_time'].map(lambda x: x.weekday())
    df['hour'] = df['create_time'].map(lambda x: x.hour)

    # 根据身份证号增加性别和年龄 年龄的计算需根据订单创建日期计算
    df['age'] = df['year'] - df['cert_no'].str.slice(6, 10).astype(int)
    df['sex'] = df['cert_no'].str.slice(-2, -1).astype(int) % 2

    def get_baiqishi_score(x):
        ret = 0
        if isinstance(x, type('str')):
            ret_list = re.findall(r'final\w+core.:[\'\"]?([\d]+)', x)
            ret = int(ret_list[0]) if len(ret_list) > 0 else 0

        return ret

    df['baiqishi_score'] = df['bai_qi_shi_detail_json'].map(lambda x: get_baiqishi_score(x))

    #
    # # 处理mibao_detail_json
    # df['tdTotalScore'] = 0
    # df['zu_lin_ren_shen_fen_zheng_yan_zheng'] = 0
    # df['zu_lin_ren_xing_wei'] = 0
    # df['shou_ji_hao_yan_zheng'] = 0
    # df['fan_qi_za'] = 0
    # df.reset_index(inplace=True)
    # for index, value in enumerate(df['mibao_detail_json']):
    #     if isinstance(value, type('str')):
    #         mb_list = json.loads(value)
    #         if (len(mb_list) == 5):
    #             for mb in mb_list:
    #                 df.at[index, mb.get('relevanceRule', 'error')] = mb.get('score', 0)
    #                 # print(mb.get('relevanceRule', 'error'))
    #                 # print(mb.get('score', 0))

    # 未处理的特征
    df.drop(['cert_no_expiry_date', 'regist_useragent', 'cert_no_json', ],
            axis=1, inplace=True, errors='ignore')
    # 已使用的特征
    df.drop(['zmxy_score', 'card_id', 'phone_user', 'xiaobaiScore', 'zmxyScore', 'cert_no',
             'bai_qi_shi_detail_json', 'guanzhu_detail_json', 'mibao_detail_json',
             'order_detail'], axis=1,
            inplace=True, errors='ignore')
    # 与其他特征关联度过高的特征
    df.drop(['lease_num', 'installment'], axis=1, inplace=True, errors='ignore')
    '''
    'tdTotalScore','zu_lin_ren_shen_fen_zheng_yan_zheng','zu_lin_ren_xing_wei','shou_ji_hao_yan_zheng','fan_qi_za',

    feature = 'fan_qi_za'
    df[feature].value_counts()
    feature_analyse(df, feature, bins=50)
    df[feature].dtype
    df[df[feature].isnull()].sort_values(by='target').shape
    df[feature].unique()
    df.columns.values
    missing_values_table(df)
    df.shape
    '''
    # merchant 违约率

    df.drop(['year', 'cancel_reason', 'check_remark', 'hit_merchant_white_list', 'mibao_result',
             'tongdun_detail_json', 'order_number', 'joke', 'mibao_remark', 'tongdun_remark', 'bai_qi_shi_remark',
             'guanzhu_remark'], axis=1, inplace=True, errors='ignore')

    return df


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
    save_data(df, "mibao.csv")
    # 保存注释内容
    sql = '''SELECT table_name, column_name, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = 'mibao_rds'; '''
    df = read_sql_query(sql)
    save_data(df, "mibao_comment.csv")

    return df


def bit_process(df: pd.DataFrame):
    df = df.astype(str)
    df.fillna('0', inplace=True)
    df = np.where(df.str.contains('1'), 1, 0)
    return df




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

    # 特殊字符串的列预先处理下：
    features = ['installment', 'commented', 'disposable_payment_enabled', 'face_check', 'face_live_check']
    for feature in features:
        all_data_df[feature] = bit_process(all_data_df[feature])

    # 去除软删除标志的数据
    all_data_df = all_data_df[all_data_df['deleted'] != 1]
    # 去除测试数据和内部员工数据
    all_data_df = all_data_df[all_data_df['cancel_reason'].str.contains('测试') != True]
    all_data_df = all_data_df[all_data_df['check_remark'].str.contains('测试') != True]

    # 标注人工审核结果于target_state字段
    df['target_state'] = None
    # 未处理订单
    # 机审拒绝
    # 机审通过但用户取消
    # 人审拒绝
    # 机审或人审拒绝后， 押金订单
    # 人审通过但用户取消
    # 正常订单
    # 逾期订单

    # df.loc[df['state_cao'].isin(['manual_check_fail']), 'target'] = 0
    # df.loc[df['state_cao'].isin(['manual_check_success']), 'target'] = 1
    # df.loc[df['state'].isin(pass_state_values), 'target'] = 1
    # df.loc[df['state'].isin(failure_state_values), 'target'] = 0
    # df = df[df['target'].notnull()]
    # df['target'].value_counts()
    # df['state'].value_counts()

    return all_data_df

# In[]
def feature_analyse(df, feature):
    feature = 'deleted'
    print("数据类型: ", df[feature].dtype)
    df[feature].value_counts()
    df[df[feature].isnull()].sort_values(by='target').shape
    df[feature].unique()
    missing_values_table(df)
    df.shape

    print("数据的趋势、集中趋势、紧密程度")
    print("数据分布、密度")
    print("数据相关性、周期性")
    print("统计作图，更直观的发现数据的规律：折线图、直方图、饼形图、箱型图、对数图形、误差条形图")


df = pd.read_csv(os.path.join(data_path, "mibao.csv"), encoding='utf-8', engine='python')

df.rename(columns={'create_time_x': 'create_time'}, inplace=True)
df.columns.tolist()
'''
feature = 'create_time'
df[feature].value_counts()
feature_analyse(df, feature, bins=50)
df[feature].dtype
df[df[feature].isnull()].sort_values(by='target').shape
df[feature].unique()
df.columns.values
missing_values_table(df)
df.shape
'''
