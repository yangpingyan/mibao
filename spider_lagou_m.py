#!/usr/bin/env python 
# coding: utf-8
# @Time : 2019/1/23 16:54 
# @Author : yangpingyan@gmail.com


# coding: utf-8
import uuid
import requests
import time
import json
import pandas as pd
import random
import os
from sql.sql import sql_connect
import sys
try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
except:
    pass

try:
    from urllib import parse as parse
except:
    import urllib as parse

    sys.reload()
    sys.setdefaultencoding('utf-8')

# Initial parameters
data_base = 'enterprise'
table_name = "lagou"
create_table_sql = '''CREATE TABLE if not exists `{}`(
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
            `company_name` varchar(50) UNIQUE ,
            `job_site` char(20),
            `company_id` char(20),
            `job_create_time` char(20),
            PRIMARY KEY (`id`)
            )'''.format(table_name)

# 插入信息函数，每次插入一条信息，插入信息失败会回滚
def insert_data_df(conn, df: pd.DataFrame):
    '''插入数据，不成功就回滚操作'''
    df.reset_index(inplace=True)
    sql = '''INSERT INTO `{}`(company_name, job_site, company_id, job_create_time) VALUES('{}','{}','{}','{}')'''
    try:
        for row in range(0, len(df)):
            conn.cursor().execute(
                sql.format(table_name, df.at[row, 'company_name'], df.at[row, 'job_site'],
                           df.at[row, 'company_id'], df.at[row, 'job_create_time']))

    except Exception as e:
        conn.rollback()
        print("插入信息失败，原因：", e)
    else:
        conn.commit()
        # print("成功插入一条信息")

def get_companys(conn, table_name):
    df = pd.read_sql('''SELECT `company_name` FROM `{}` ;'''.format(table_name), conn)
    company_list = df['company_name'].values.tolist()
    return company_list

def init_cookies():
    """
    return the cookies after your first visit
    """
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'Host': 'm.lagou.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
        'DNT': '1',
        'Cache-Control': 'max-age=0',
        'Referrer Policy': 'no-referrer-when-downgrade',
    }
    url = 'https://m.lagou.com/search.html'
    response = requests.get(url, headers=headers, timeout=10)

    return response.cookies

def get_max_pageNo(city, position_name):
    """
    return the max page number of a specific job
    """
    request_url = 'https://m.lagou.com/search.json?city={}&positionName={}&pageNo=1&pageSize=15'.format(parse.quote(city), parse.quote(position_name))
    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Host': 'm.lagou.com',
        'Referer': 'https://m.lagou.com/search.html',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) AppleWebKit/600.1.3 (KHTML, like Gecko) '
                      'Version/8.0 Mobile/12A4345d Safari/600.1.4',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive'
    }
    response = requests.get(request_url, headers=headers, cookies=init_cookies(), timeout=10)
    print("Getting data from %s successfully. URL: " % position_name + request_url)
    if response.status_code == 200:
        max_page_no = int(int(response.json()['content']['data']['page']['totalCount']) / 15 + 1)

        return max_page_no
    elif response.status_code == 403:
        print('request is forbidden by the server...')
        return 0
    else:
        print(response.status_code)
        return 0


try:
    workdir = os.path.dirname(os.path.realpath(__file__))
except:
    workdir = os.getcwd()

sql_file = os.path.join(workdir, 'sql', 'sql_mibao_spider.json')
ssh_pkey = os.path.join(workdir, 'sql', 'sql_pkey')
conn = sql_connect(data_base, sql_file, ssh_pkey)
# 创建表格
try:
    conn.cursor().execute(create_table_sql)
except Exception as e:
    print("创建表格失败，表格可能已经存在！", e)
else:
    conn.commit()

citys = ['杭州', '宁波', '温州', '湖州', '衢州', '台州', '金华', '绍兴', '舟山', '嘉兴',
         '上海', '南京', '苏州', '无锡', '常州', '南通', '扬州', '镇江', '昆山', '宿迁', '连云港',
         '合肥', '黄山', '深圳', '广州', '西安', '重庆', '武汉', '成都', '长沙', '厦门', '福州']

for city in citys:
    print(city)
    position_name = ' '
    max_page_number = get_max_pageNo(city, position_name)
    print("There are {} pages, approximately {} records in total.".format(max_page_number, max_page_number * 15))

    # init cookies
    cookie = init_cookies()
    companys_list = get_companys(conn, table_name)
    none_added_count = 0
    for page_no in range(1, max_page_number + 1):
        request_url = 'https://m.lagou.com/search.json?city={}&positionName={}&pageNo={}&pageSize=15'.format(
            parse.quote(city), parse.quote(position_name), page_no)

        headers = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Host': 'm.lagou.com',
            'DNT': '1',
            'Referer': 'https://m.lagou.com/search.html',
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referrer Policy': 'no-referrer-when-downgrade',
        }
        response = requests.get(request_url, headers=headers, cookies=cookie)

        # update cookies after first visit
        # cookie = response.cookies
        # cookie = dict(cookies_are='')

        if response.status_code == 200:
            try:
                items = response.json()['content']['data']['page']['result']
                if len(items) > 0:
                    df = pd.DataFrame(columns=['company_name', 'job_site', 'company_id', 'job_create_time'])
                    row = 0

                    for each_item in items:
                        df.loc[row] = [each_item['companyFullName'], each_item['city'], each_item['companyId'],
                                       each_item['createTime']]
                        row += 1
                        # print(each_item)
                    df.drop_duplicates(subset='company_name', inplace=True)
                    df = df[df['company_name'].isin(companys_list) == False]
                    if len(df) > 0:
                        insert_data_df(conn, df)
                        companys_list.extend(df['company_name'].tolist())
                        none_added_count = 0
                    else:
                        none_added_count += 1

                    print(city, len(df), " companys added")
                    print('crawling page %d done...' % page_no)
                    time.sleep(random.randint(3, 6))
                else:
                    break
            except Exception as e:
                print(e)
                none_added_count += 1
                print('Invalid request is found by Lagou...')
                pass
        elif response.status_code == 403:
            print('request is forbidden by the server...')
            none_added_count += 1
        else:
            print(response.status_code)
            none_added_count += 1

        if none_added_count > 3:
            break

    time.sleep(round(random.uniform(3, 5), 2))
print("Mission Complete")





