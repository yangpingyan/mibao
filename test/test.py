import msvcrt
import time
import random
import pandas as pd
import numpy as np
import datetime

td = pd.Timedelta('3 days 2 hours 3 seconds')
td.seconds   #小于1天的秒数 7203
td.days  # 3
td / np.timedelta64(1, 'm')  #转换成秒数 266403


express_dict = {'EMS': 127,
'中国邮政': 6,
'中通': 76,
'中通快递': 818,
'京东': 3,
'京东物流': 73,
'其他': 175,
'圆通': 37,
'圆通快递': 93,
'天天': 13,
'天天快递': 11,
'安迅物流': 4,
'德邦': 3,
'德邦物流': 139,
'新邦物流': 3,
'汇通': 6,
'申通': 49,
'申通快递': 862,
'百世汇通': 21,
'百世物流': 3,
'苏宁': 13,
'苏宁物流': 9,
'门店自提': 119,
'闪送': 10,
'韵达': 8,
'韵达快递': 410,
'顺丰': 854,
'顺丰快递': 6473,
'顺心捷达': 1}

pay_type_dict = {'alipay': 684,
'alipay_deduction': 293,
'jdpay': 601,
'wechat_mini_program': 1,
'wxpay': 198}

all_data_df = pd.read_csv('data1.csv')
all_data_df['rent_resale']  = 'N'
all_data_df['return_rate']  = None
all_data_df['create_time'] = pd.to_datetime(all_data_df['create_time'])
all_data_df['monthly_rent'] = None
all_data_df['sign_time'] = None

# all_data_df[all_data_df['delivery_time'].notnull()]['sign_time'] = pd.to_datetime(all_data_df[all_data_df['delivery_time'].notnull()])
# all_data_df[all_data_df['delivery_time'].notnull()]['sign_time'] = all_data_df[all_data_df['delivery_time'].notnull()]['sign_time'] - datetime.timedelta(1)



df = all_data_df[all_data_df['delivery_time'].isnull()]
df[~df['id'].isin([1329])]


df.iteritems()
for k,v in df.iterrows():
    print(k,v)
    break

df['delivery_time'] = df['create_time'].map(lambda x: x + datetime.timedelta(random.randint(1,3)))
df['sign_time'] = df['delivery_time'].map(lambda x: x + datetime.timedelta(random.randint(1,4)))
df['lease_start_time'] = df['sign_time'] + datetime.timedelta(1)
df['lease_term_format'] = df['lease_term'].map(lambda x: datetime.timedelta(x))
df['expiration_date'] = df['lease_start_time'] + df['lease_term_format']

all_data_df = pd.concat([all_data_df, df])

today = datetime.datetime.now()
df = df[df['lease_start_time'] < today]

express_list = []
for k,v in express_dict.items():
    l = [k]*v
    express_list.extend(l)
df['express_channel'] = df['express_channel'].map(lambda x: random.choice(express_list))

pay_type_list = []
for k,v in pay_type_dict.items():
    l = [k]*v
    pay_type_list.extend(l)
df['pay_type'] = df['pay_type'].map(lambda x: random.choice(pay_type_list))

all_data_df = pd.concat([all_data_df, df])


df = df[today-df['lease_start_time'] > datetime.timedelta(31)]
null_list = df.index.tolist()
index_list = random.sample(null_list,2859)

for index in index_list:
    leased_term = int((today - df.loc[index, 'lease_start_time']).days/30)
    df.loc[index, 'overdue_time'] = random.randint(1,leased_term)
all_data_df = pd.concat([all_data_df, df])
df = df[df['overdue_time'].notnull()]

overdue_index = df[df['overdue_time'] > 1].index.tolist()
overdue_continuous_index_list = random.sample(overdue_index,633)
for index in overdue_continuous_index_list:
    random_number = random.randint(2,df.loc[index, 'overdue_time'])
    df.loc[index, 'delay_amount_greater30'] = df.loc[index, 'price'] - random_number *  df.loc[index, 'daily_rent'] * 30
    df.loc[index, 'return_rate'] =(df.loc[index, 'overdue_time'] - random_number)/  df.loc[index, 'overdue_time']
    df.loc[index, 'rent_resale'] = 'Y'

overdue_30 = random.sample(set(index_list).difference(overdue_continuous_index_list), 245)
for index in overdue_30:
    df.loc[index, 'delay_amount_30day'] = df.loc[index, 'daily_rent'] * 30
    df.loc[index, 'return_rate'] =(df.loc[index, 'overdue_time'] - 1)/  df.loc[index, 'overdue_time']


df['monthly_rent'] = df['daily_rent']*30
all_data_df = pd.concat([all_data_df, df])

all_data_df.drop_duplicates(subset='id', keep='last', inplace=True)
all_data_df.to_csv('fixed.cvs', index=False)
# df_2 = df[['name','cardID','phone', 'pay_num', 'address','price','lease_term','daily_rent','monthly_rent','goods_name','standard','express_channel','pay_type','delivery_time','sign_time','lease_start_time','expiration_date','overdue_time','delay_amount_less30','delay_amount_greater30','return_rate','rent_resale']]


import tableausdk as tableau

