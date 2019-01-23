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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

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

max_page_number = get_max_pageNo()
print("There are %s pages, approximately %s records in total.", max_page_number, max_page_number * 15)

# init cookies
cookie = init_cookies()

class SpiderLagou(object):

    def crawl_lagou(city, position_name):
        """
        crawl the job info from lagou H5 web pages
        """

        for i in range(1, max_page_number + 1):
            request_url = 'https://m.lagou.com/search.json?city={}&positionName={}&pageNo=1&pageSize=15'.format(
                parse.quote(city), parse.quote(position_name))

            request_url = 'https://m.lagou.com/search.json?city=%E5%85%A8%E5%9B%BD&positionName=' + parse.quote(
                positionName) + '&pageNo=' + str(i) + '&pageSize=15'
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
                        for each_item in items:


                            JOB_DATA.append([each_item['positionId'], each_item['positionName'], each_item['city'],
                                             each_item['createTime'], each_item['salary'], each_item['companyId'],
                                             each_item['companyName'], each_item['companyFullName']])
                            print(each_item)
                        print('crawling page %d done...' % i)
                        time.sleep(random.randint(3, 6))
                    else:
                        break
                except Exception as exp:
                    print('Invalid request is found by Lagou...')
                    pass
            elif response.status_code == 403:
                print('request is forbidden by the server...')
            else:
                print(response.status_code)

        return JOB_DATA


    def get_lagou(self, page, city, kd):
        url = "https://www.lagou.com/jobs/positionAjax.json"
        querystring = {"px": "new", "city": city, "needAddtionalResult": "false", "isSchoolJob": "0"}
        payload = "first=false&pn=" + str(page) + "&kd=" + str(kd)
        cookie = "JSESSIONID=" + get_uuid() + ";" + "user_trace_token=" + get_uuid() + "; LGUID=" + get_uuid() + "; index_location_city=%E6%88%90%E9%83%BD; " + "SEARCH_ID=" + get_uuid() + '; _gid=GA1.2.717841549.1514043316; '                                                                                                                                         '_ga=GA1.2.952298646.1514043316; '                                                                                                                                          'LGSID=' + get_uuid() + "; "                                                                                                                                                                  "LGRID=" + get_uuid() + "; "
        headers = {'cookie': cookie,
                   'Host': "www.lagou.com",
                   "Upgrade-Insecure-Requests": "1",
                   'x-anit-forge-code': "0",
                   'accept-encoding': "gzip, deflate, br", 'accept-language': "zh-CN,zh;q=0.8,en;q=0.6",
                   'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
                   'content-type': "application/x-www-form-urlencoded; charset=UTF-8",
                   'accept': "application/json, text/javascript, */*; q=0.01",
                   'referer': "https://www.lagou.com/jobs/list_%20?px=new&city={}".format(city),
                   'x-requested-with': "XMLHttpRequest", 'connection': "keep-alive", 'x-anit-forge-token': "None",
                   'cache-control': "no-cache", 'postman-token': "91beb456-8dd9-0390-a3a5-64ff3936fa63"}
        response = requests.request("POST", url, data=payload.encode('utf-8'), headers=headers, params=querystring)
        # print(response.text)
        html_json = json.loads(response.text)
        # print( html_json['content']['positionResult']['result'])
        df = pd.DataFrame(columns=['company_name', 'job_site', 'company_id', 'job_create_time'])
        row = 0
        for position in html_json['content']['positionResult']['result']:
            df.loc[row] = [position['companyFullName'], position['city'], position['companyId'], position['createTime']]
            row += 1

        response.close()

        return df

    def main(self, city, keyword):
        companys_list = get_companys()
        none_added_count = 0
        start_page = 1
        for n in range(start_page, start_page + 1000):
            print("starting to crawl page ", n)
            try:
                df = self.get_lagou(n, city, keyword)
                print(df)
                if len(df) == 0:
                    break
                df.drop_duplicates(subset='company_name', inplace=True)
                df = df[df['company_name'].isin(companys_list) == False]
                if len(df) > 0:
                    self.insert_data_df(df)
                    companys_list.extend(df['company_name'].tolist())
                    none_added_count = 0
                else:
                    none_added_count += 1

                print(city, len(df), " companys added")
            except Exception as e:
                print(e)
                none_added_count += 1

            if none_added_count > 3:
                break

            time.sleep(round(random.uniform(4, 6), 2))
        conn.cursor().close()
        conn.close()


if __name__ == "__main__":
    citys = ['杭州', '上海', '南京', '苏州', '宁波', '温州', '湖州', '衢州', '台州', '金华', '丽水', '舟山', '嘉兴',
             '无锡', '常州', '徐州', '南通', '淮安', '盐城', '扬州', '镇江', '泰州', '宿迁', '连云港']
    for city in citys:
        print(city)
        crawl = SpiderLagou()
        crawl.main(city, ' ')
        time.sleep(round(random.uniform(3, 5), 2))
    print("Mission Complete")





