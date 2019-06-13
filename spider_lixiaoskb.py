#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/10 11:13 
# @Author : yangpingyan@gmail.com

import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
import re, json, os, sys
from datetime import datetime

from sql.sql import sql_connect


class SpiderLixiaoskb(object):
    def __init__(self):
        try:
            workdir = os.path.dirname(os.path.realpath(__file__))
        except:
            workdir = os.getcwd()
        self.workdir = workdir
        self.from_table = None

        self.sql_file = os.path.join(workdir, 'sql', 'sql_mibao_spider.json')
        self.ssh_pkey = os.path.join(workdir, 'sql', 'sql_pkey')
        self.conn = sql_connect('enterprise', self.sql_file, self.ssh_pkey)
        self.create_table()

        with open(os.path.join(workdir, 'others', "lixiaoskb_account.json"), 'r') as f:
            lixiaoskb_account = json.load(f)
        self.username = lixiaoskb_account['username']
        self.password = lixiaoskb_account['password']
        self.usernames = ['15356663955@qq.com', '17306420346@qq.com', '17746844466@qq.com', '17774004648@qq.com',
                          'wangkantao@qq.com']
        self.url_login = 'https://biz.lixiaoskb.com/login'
        self.view_count = 0
        self.companys_skb = self.get_companys_skb()
    # 创建表格的函数，表格名称按照时间和关键词命名
    def create_table(self):
        self.table_name = "lixiaoskb"
        sql = '''CREATE TABLE if not exists `{}` (
                      `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                      `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
                      `company_name` varchar(50) NOT NULL UNIQUE COMMENT '公司名称',
                      `phone` varchar(255) DEFAULT NULL COMMENT '电话',
                      `email` varchar(255) DEFAULT NULL COMMENT '邮箱',
                      `website` varchar(255) DEFAULT NULL COMMENT '网址',
                      `address` varchar(255) DEFAULT NULL COMMENT '地址',
                      `introduction` varchar(255) DEFAULT NULL COMMENT '简介',
                      `legal_person` varchar(50) DEFAULT NULL COMMENT '法人',
                      `registered_capital` varchar(50) DEFAULT NULL COMMENT '注册资本',
                      `registered_date` varchar(50) DEFAULT NULL COMMENT '注册时间',
                      `company_status` varchar(50) DEFAULT NULL COMMENT '状态',                      
                      `registered_number` varchar(50) DEFAULT NULL COMMENT '工商注册号',                      
                      `organization_code` varchar(50) DEFAULT NULL COMMENT '组织机构代码',                      
                      `social_credit_code` varchar(50) DEFAULT NULL COMMENT '统一社会信用代码',                      
                      `company_type` varchar(50) DEFAULT NULL COMMENT '公司类型',                      
                      `taxpayer_number` varchar(50) DEFAULT NULL COMMENT '纳税人识别号',                      
                      `industry` varchar(50) DEFAULT NULL COMMENT '行业',                      
                      `operating_period` varchar(50) DEFAULT NULL COMMENT '营业期限',                      
                      `approval_date` varchar(50) DEFAULT NULL COMMENT '核准日期',                      
                      `taxpayer_qualification` varchar(50) DEFAULT NULL COMMENT '纳税人资质',                      
                      `staff_size` varchar(50) DEFAULT NULL COMMENT '人员规模',                      
                      `contributed_capital` varchar(50) DEFAULT NULL COMMENT '实缴资本',
                      `registration_office` varchar(50) DEFAULT NULL COMMENT '登记机关',
                      `insurance_contributors` varchar(50) DEFAULT NULL COMMENT '参保人数',
                      `english_name` varchar(50) DEFAULT NULL COMMENT '英文名称',
                      `registered_address` varchar(50) DEFAULT NULL COMMENT '注册地址',
                      `business_scope` varchar(50) DEFAULT NULL COMMENT '经营范围',                      
                      `link` varchar(255) DEFAULT NULL COMMENT '公司信息获取的链接地址',
                      `from_table` varchar(50) DEFAULT NULL COMMENT '公司名称来源哪张表',
                      `id_related` int(10) unsigned DEFAULT NULL COMMENT '表对应的id',
                      `score` varchar(50) DEFAULT NULL COMMENT '天眼评分',
                      `risk_own_sum` varchar(50) DEFAULT NULL COMMENT '自身风险个数',
                      `risk_surrounding_sum` varchar(50) DEFAULT NULL COMMENT '周边风险个数',
                      `warning_sum` varchar(50) DEFAULT NULL COMMENT '预警提醒个数',
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8'''

        try:
            self.conn.cursor().execute(sql.format(self.table_name))
        except Exception as e:
            print("创建表格失败，表格可能已经存在！", e)
        else:
            self.conn.commit()
            print("成功创建一个表格{}".format(self.table_name))

    # 插入信息函数，每次插入一条信息，插入信息失败会回滚
    def insert_data(self, data: dict):
        try:
            self.conn.ping(True)
        except Exception as e:
            self.conn.cursor().close()
            self.conn.close()
            self.conn = sql_connect('enterprise', self.sql_file, self.ssh_pkey)

        '''插入数据，不成功就回滚操作'''
        sql = '''INSERT INTO `{}`(company_name, phone, email, website,
                               address, introduction, legal_person, registered_capital,
                               registered_date, company_status, registered_number,
                               organization_code, social_credit_code, company_type,
                               taxpayer_number, industry, operating_period, approval_date,
                               taxpayer_qualification, staff_size, contributed_capital,
                               registration_office, insurance_contributors, english_name,
                               registered_address, business_scope, link, from_table, id_related, score, risk_own_sum, risk_surrounding_sum, warning_sum ) 
                               VALUES("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",
                               "{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}") ; '''
        try:
            self.conn.cursor().execute(
                sql.format(self.table_name, data.get('公司名称'), data.get('电话'), data.get('邮箱'), data.get('网址'),
                           data.get('地址'), data.get('简介'), data.get('法人'), data.get('注册资本'), data.get('成立日期'),
                           data.get('经营状态'), data.get('工商注册号'), data.get('组织机构代码'), data.get('统一社会信用代码'),
                           data.get('公司类型'), data.get('纳税人识别号'), data.get('行业'), data.get('营业期限'), data.get('核准日期'),
                           data.get('纳税人资质'), data.get('人员规模'), data.get('实缴资本'), data.get('登记机关'), data.get('参保人数'),
                           data.get('英文名称'), data.get('注册地址'), data.get('经营范围'), data.get('link'), self.from_table,
                           data.get('id_related'), data.get('评分'), data.get('自身风险'), data.get('周边风险'),
                           data.get('预警提醒')))
        except Exception as e:
            self.conn.rollback()
            print("插入信息失败，原因：", e)
        else:
            self.conn.commit()
            # print("成功插入一条信息")

    def close(self):
        '''关闭游标和断开链接，数据全部插入后必须执行这个操作'''
        self.conn.cursor().close()
        self.conn.close()

    def login(self):
        # 关闭浏览器
        try:
            self.browser.quit()
        except:
            pass
        usernames_counts = len(self.usernames)
        for i in range(usernames_counts):
            self.username = self.usernames.pop()
            print(f"使用{self.username}用户名登陆")

            # 设置浏览器
            options = webdriver.ChromeOptions()
            prefs = {'profile.default_content_setting_values': {'images': 2}}
            options.add_experimental_option('prefs', prefs)
            executable_path = 'C:\ProgramData\Anaconda3\envs\envforall\chromedriver.exe'
            self.browser = webdriver.Chrome(executable_path=executable_path, chrome_options=options)

            self.browser.get(self.url_login)
            time.sleep(round(random.uniform(1, 2), 2))

            # 模拟登陆
            self.browser.find_element_by_css_selector(
                "#app > div.wrapper.login-wrapper > div > div > div.login-box > div:nth-child(2) > div.ms-login > form > div:nth-child(1) > div > div > div > input").send_keys(
                self.username)
            self.browser.find_element_by_css_selector(
                "#app > div.wrapper.login-wrapper > div > div > div.login-box > div:nth-child(2) > div.ms-login > form > div:nth-child(2) > div > div > div > input").send_keys(
                self.password)
            self.browser.find_element_by_css_selector(
                "#app > div.wrapper.login-wrapper > div > div > div.login-box > div:nth-child(2) > div.btn > button").click()
            view_count = WebDriverWait(self.browser, 10).until(
                lambda x: x.find_element(By.CSS_SELECTOR, "[class='viewCount']")).text
            self.view_count = int(view_count)
            print(f"登陆成功, 还可查看{self.view_count}次联系方式")
            if self.view_count > self.view_count_left:
                break
            else:
                self.browser.quit()

    def get_lixiaoskb(self, company):
        base_table = {}
        # 清空搜索栏
        # try:
        #     # self.browser.find_element(By.CSS_SELECTOR, "# searchDeInput > div.del-btn").click()
        #     self.browser.find_element(By.CLASS_NAME, "clear-label").click()
        #     time.sleep(round(random.uniform(1, 2), 2))
        # except:
        #     pass

        # self.browser.find_element(By.CSS_SELECTOR,"#userRatio > div > label:nth-child(1) > span.el-radio__input > span").click()
        self.browser.find_element(By.TAG_NAME, "input").send_keys(company)
        self.browser.find_element(By.TAG_NAME, "input").send_keys(Keys.ENTER)
        try:
            WebDriverWait(self.browser, 20).until(
                lambda x: x.find_element(By.CSS_SELECTOR, "[class='result-list'")).find_element(By.TAG_NAME, "a")
            url_company = self.browser.find_element(By.CSS_SELECTOR, "[class='result-list'").find_element(By.TAG_NAME,
                                                                                                          "a").get_attribute(
                'href')
            # self.browser.find_element(By.CSS_SELECTOR, "[class='result-list'").find_element(By.TAG_NAME, "a").click()
            # self.browser.switch_to_window(self.browser.window_handles[1])

        except Exception as e:
            print("Error message: ", e)
            print("没找到结果")
            self.browser.refresh()
            try:
                self.browser.find_element(By.CLASS_NAME, "clear-label").click()
            except:
                pass
            time.sleep(round(random.uniform(1, 2), 2))
            return None
        self.browser.get(url_company)
        WebDriverWait(self.browser, 20).until(lambda x: x.find_element(By.CLASS_NAME, "name"))
        # 统一key名称
        key_name_dict = {'所属行业': '行业', '官方网站': '网址', '通讯地址': '地址', '注册号': '统一社会信用代码'}
        # 获取公司基本信息
        base_table['公司名称'] = self.browser.find_element(By.CLASS_NAME, "name").text
        if base_table['公司名称'] in self.companys_skb:
            self.browser.back()
            return None
        # 查看联系方式按键
        try:
            self.browser.find_element(By.CLASS_NAME, "mask-box").find_element(By.CLASS_NAME, "action").click()
            WebDriverWait(self.browser, 30).until(lambda x: x.find_element(By.CSS_SELECTOR,
                                                                           "[class='report-scroll_wrap el-scrollbar__wrap']"))

        except:
            print("没找到查看联系方式按钮---")


        info_elements = self.browser.find_elements(By.CLASS_NAME, "group")
        for element in info_elements:
            label = element.find_element(By.CLASS_NAME, "label").text
            label = label.split(' ')[0]
            try:
                desc = element.find_element(By.CLASS_NAME, "desc").text
            except:
                try:
                    desc = element.find_element(By.CLASS_NAME, "website").text
                except:
                    desc = '-'
            if label in key_name_dict.keys():
                label = key_name_dict[label]
            base_table[label] = desc

        info_elements = self.browser.find_elements(By.CLASS_NAME, "gongshang-col")
        for element in info_elements:

            text = element.text.split('\n')
            label = text[0].split('/')[0]
            desc = text[-1]

            if label in key_name_dict.keys():
                label = key_name_dict[label]
            base_table[label] = desc

        # 获取联系方式
        try:
            contact_element = self.browser.find_element(By.CSS_SELECTOR,
                                                        "[class='report-scroll_wrap el-scrollbar__wrap']")
            phone_element = contact_element.find_elements(By.CSS_SELECTOR, "[class='el-tooltip hint number']")
            person_element = contact_element.find_elements(By.CSS_SELECTOR, "[class='el-carousel']")
            base_table['电话'] = ''
            for phone, name in zip(phone_element, person_element):
                base_table['电话'] = base_table['电话'] + phone.text + name.find_element(By.CSS_SELECTOR,
                                                                                     "[class='content']").text + '/'

            view_count = self.browser.find_element(By.CSS_SELECTOR, "[class='viewCount']").text
            self.view_count = int(view_count)
        except:
            base_table['电话'] = '暂无联系方式'

        self.browser.back()
        return base_table

    def get_companys_skb(self):
        skb_file = os.path.join(self.workdir, 'skb_companys.csv')

        if os.path.exists(skb_file):
            df = pd.read_csv(skb_file)
            skb_df = pd.read_sql(
                '''SELECT j.id, j.`company_name` FROM  `lixiaoskb` j WHERE j.id > {} ORDER BY id ; '''.format(max(df['id'])),
                self.conn)
            df = pd.concat([df, skb_df])
            skb_df.to_csv(skb_file, index=False,mode='a', header=False)
        else:
            df = pd.read_sql('''SELECT j.id, j.`company_name` FROM  `lixiaoskb` j ORDER BY id ; ''', self.conn)
            df.to_csv(skb_file, index=False)

        return df['company_name'].tolist()

    def get_companys_51job(self):
        start_time = time.clock()
        self.from_table = '51job'
        df = pd.read_sql('''SELECT * FROM `lixiaoskb` WHERE from_table = '51job' ORDER BY id_related DESC LIMIT 1;''',
                         self.conn)
        last_id = 0
        if len(df) > 0:
            last_id = df['id_related'][0]
        companys_51job_df = pd.read_sql(
            '''SELECT j.id, j.`real_name` FROM  `51job` j WHERE j.`id` > {} ORDER BY j.id;'''.format(last_id),
            self.conn)

        print(time.clock() - start_time)

        df = companys_51job_df[~companys_51job_df['real_name'].isin(self.companys_skb)]
        df = df.drop_duplicates(subset='real_name')
        print(len(df), "companys for crawling")

        return df

    def get_companys_lagou(self):
        start_time = time.clock()
        self.from_table = 'lagou'
        df = pd.read_sql('''SELECT * FROM `lixiaoskb` WHERE from_table = 'lagou' ORDER BY id_related DESC LIMIT 1;''',
                         self.conn)
        last_id = 0
        if len(df) > 0:
            last_id = df['id_related'][0]
        companys_lagou_df = pd.read_sql(
            '''SELECT j.id, j.`company_name` FROM  `lagou` j WHERE j.`id` > {} ORDER BY j.id;'''.format(last_id),
            self.conn)


        print(time.clock() - start_time)
        df = companys_lagou_df[~companys_lagou_df['company_name'].isin(self.companys_skb)]
        df = df.drop_duplicates(subset='company_name')
        print(len(df), "companys for crawling")

        return df

    def main_database(self, database_name):
        # 工作日16点以前，预留99个查询数
        now = datetime.now()
        if now.weekday() > 4 or now.hour > 14:
            self.view_count_left = 3
        else:
            self.view_count_left = 3
        print(f"view_count_left is {self.view_count_left}")
        # 爬取51job所有数据库中未爬取的公司
        if database_name == 'lagou':
            companys_df = self.get_companys_lagou()
        elif database_name == '51job':
            companys_df = self.get_companys_51job()
            companys_df.rename(columns={'real_name': 'company_name'}, inplace=True)
        else:
            print("未知数据库， 退出")
            return 1
        if len(companys_df) > 0:
            companys = companys_df['company_name'].values.tolist()
            for company in companys:
                if self.view_count < self.view_count_left:
                    self.login()

                if self.view_count < self.view_count_left:
                    break

                print("Start to crawl :", companys_df[companys_df['company_name'] == company])
                base_table = self.get_lixiaoskb(company)
                if base_table is None:
                    continue
                else:
                    print(base_table)
                    base_table['id_related'] = companys_df[companys_df['company_name'] == company]['id'].values[0]
                    self.insert_data(base_table)
            self.browser.quit()
        self.close()

        return 0

    def main(self, companys):

        return 0


if __name__ == '__main__':

    while(True):
        try:
            spider = SpiderLixiaoskb()
            ret = spider.main_database('lagou')
            # ret = spider.main_database('51job')
        except Exception as e:
            print(e)
            ret = -1

        if ret == 0:
            break

    print("lixiaoskb spider completed")
