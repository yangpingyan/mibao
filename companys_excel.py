#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/28 9:49 
# @Author : yangpingyan@gmail.com

from mibao.sql import sql_engine_connect
import pandas as pd
import os

def get_excel_files(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.xlsx':
                L.append(os.path.join(root, file))
    return L

files = get_excel_files('./mibao/dataset/')
print(files)
all_df = pd.DataFrame()
for excelfile in files:
    print(excelfile)

    df_file = pd.read_excel(excelfile, header=None, names=['company_name', 1, 2, 3, 4, 5])
    all_df = pd.concat([all_df, df_file], join='outer', axis=0, ignore_index=True)

print(all_df)
all_df.shape
all_df.nunique()
all_df = all_df.drop_duplicates('company_name')
all_df.shape

conn = sql_engine_connect(1)
tyc_df = pd.read_sql('''select t.company_name, t.phone from tianyancha t''', conn)
job_df = pd.read_sql('''select t.real_name company_name from 51job t''', conn)
lagou_df = pd.read_sql('''select t.company_name from lagou t''', conn)
df = pd.concat([tyc_df, job_df, lagou_df])
df = df.drop_duplicates('company_name')
df = df.reset_index()
df = df[df['company_name'].isin(all_df['company_name'])]
df['phone']
df.shape

print("Mission complete!")