# coding: utf-8
import uuid
import requests
import time
import json
import pandas as pd
import random
import os
from sql.sql import sql_connect


def get_uuid():
    return str(uuid.uuid4())


class SpiderLagou(object):
    def __init__(self):
        try:
            workdir = os.path.dirname(os.path.realpath(__file__))
        except:
            workdir = os.getcwd()
        self.workdir = workdir

        sql_file = os.path.join(workdir, 'sql', 'sql_mibao_spider.json')
        ssh_pkey = os.path.join(workdir, 'sql', 'sql_pkey')
        self.conn = sql_connect('enterprise', sql_file, ssh_pkey)
        self.create_table()

    # 创建表格的函数，表格名称按照时间和关键词命名
    def create_table(self):
        # create_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
        self.table_name = "lagou"

        sql = '''CREATE TABLE if not exists `{tbname}`(
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
            `company_name` varchar(50) UNIQUE ,
            `job_site` char(20),
            `company_id` char(20),
            `job_create_time` char(20),
            PRIMARY KEY (`id`)
            )'''
        try:
            self.conn.cursor().execute(sql.format(tbname=self.table_name))
        except Exception as e:
            print("创建表格失败，表格可能已经存在！", e)
        else:
            self.conn.commit()

    # 插入信息函数，每次插入一条信息，插入信息失败会回滚
    def insert_data(self, data: dict):
        '''插入数据，不成功就回滚操作'''
        sql = '''INSERT INTO `{}`(company_name, job_site, company_id, job_create_time) VALUES('{}','{}','{}','{}')'''
        try:

            self.conn.cursor().execute(
                sql.format(self.table_name, data.get('company_name'), data.get('job_site'), data.get('company_id'),
                           data.get('job_create_time')))

        except Exception as e:
            self.conn.rollback()
            print("插入信息失败，原因：", e)
        else:
            self.conn.commit()
            # print("成功插入一条信息")

    # 插入信息函数，每次插入一条信息，插入信息失败会回滚
    def insert_data_df(self, df:pd.DataFrame):
        '''插入数据，不成功就回滚操作'''
        df.reset_index(inplace=True)
        sql = '''INSERT INTO `{}`(company_name, job_site, company_id, job_create_time) VALUES('{}','{}','{}','{}')'''
        try:
            for row in range(0, len(df)):
                self.conn.cursor().execute(
                    sql.format(self.table_name, df.at[row, 'company_name'], df.at[row, 'job_site'],
                               df.at[row, 'company_id'], df.at[row, 'job_create_time']))

        except Exception as e:
            self.conn.rollback()
            print("插入信息失败，原因：", e)
        else:
            self.conn.commit()
            # print("成功插入一条信息")

    def get_companys(self):
        df = pd.read_sql('''SELECT `company_name` FROM `{}` ;'''.format(self.table_name), self.conn)
        company_list = df['company_name'].values.tolist()
        return company_list

    def close(self):
        '''关闭游标和断开链接，数据全部插入后必须执行这个操作'''
        self.conn.cursor().close()
        self.conn.close()

    def get_lagou(self, page, city, kd):
        url = "https://www.lagou.com/jobs/positionAjax.json"
        querystring = {"px": "new", "city": city, "needAddtionalResult": "false", "isSchoolJob": "0"}
        payload = "first=false&pn=" + str(page) + "&kd=" + str(kd)
        cookie = "JSESSIONID=" + get_uuid() + ";" + "user_trace_token=" + get_uuid() + "; LGUID=" + get_uuid() + "; index_location_city=%E6%88%90%E9%83%BD; " + "SEARCH_ID=" + get_uuid() + '; _gid=GA1.2.717841549.1514043316; '                                                                                                                                         '_ga=GA1.2.952298646.1514043316; '                                                                                                                                          'LGSID=' + get_uuid() + "; "                                                                                                                                                                  "LGRID=" + get_uuid() + "; "
        headers = {'cookie': cookie, 'origin': "https://www.lagou.com", 'x-anit-forge-code': "0",
                   'accept-encoding': "gzip, deflate, br", 'accept-language': "zh-CN,zh;q=0.8,en;q=0.6",
                   'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
                   'content-type': "application/x-www-form-urlencoded; charset=UTF-8",
                   'accept': "application/json, text/javascript, */*; q=0.01",
                   'referer': "https://www.lagou.com/jobs/list_%20?px=new&city=%E6%88%90%E9%83%BD",
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
            # if position['companyFullName'] not in companys_list:
            #     data = {}
            #     data['company_name'] = position['companyFullName']
            #     data['job_site'] = position['city']
            #     data['company_id'] = position['companyId']
            #     data['job_create_time'] = position['createTime']
            #     print(data)
            #     self.insert_data(data)
            #     companys_list.append(position['companyFullName'])
        return df

    def main(self, city, keyword):
        companys_list = self.get_companys()
        none_added_count = 0
        start_page = 1
        for n in range(start_page, start_page + 1000):
            print("starting to crawl page ", n)
            df = self.get_lagou(n, city, keyword)

            if len(df) == 0:
                break
            df.drop_duplicates(subset='company_name', inplace=True)
            df = df[df['company_name'].isin(companys_list) == False]
            if len(df) > 0:
                self.insert_data_df(df)
                none_added_count = 0
            else:
                none_added_count += 1
                if none_added_count > 3:
                    break
            companys_list.extend(df['company_name'].tolist())
            print(city, len(df), " companys added")
            time.sleep(round(random.uniform(4, 6), 2))
        self.close()


if __name__ == "__main__":
    citys = ['杭州', '上海', '南京', '苏州', '宁波', '温州', '湖州', '衢州', '台州', '金华', '丽水', '舟山', '嘉兴',
             '无锡', '常州', '徐州', '南通', '淮安', '盐城', '扬州', '镇江', '泰州', '宿迁', '连云港']
    for city in citys:
        print(city)
        crawl = SpiderLagou()
        crawl.main(city, ' ')
        time.sleep(round(random.uniform(3,5), 2))
    print("Mission Complete")
