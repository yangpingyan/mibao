#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/10 11:13 
# @Author : yangpingyan@gmail.com

'tianyancha_spider: scrap Tianyancha information'
import random

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import pandas as pd
import time
import re, json, os, sys
from fontTools.ttLib import TTFont

from sql.sql import sql_connect


class SpiderTianyangcha(object):
    def __init__(self):
        try:
            workdir = os.path.dirname(os.path.realpath(__file__))
        except:
            workdir = os.getcwd()
        self.workdir = workdir
        self.from_table = None

        sql_file = os.path.join(workdir, 'sql', 'sql_mibao_spider.json')
        ssh_pkey = os.path.join(workdir, 'sql', 'sql_pkey')
        self.conn = sql_connect('enterprise', sql_file, ssh_pkey)
        self.create_table()

        with open(os.path.join(workdir, 'others', "tianyancha_account.json"), 'r') as f:
            tianyancha_account = json.load(f)
        self.username = tianyancha_account['username']
        self.password = tianyancha_account['password']

        self.url_login = 'https://www.tianyancha.com/login'
        self.url_search = 'http://www.tianyancha.com/search?key={}&checkFrom=searchBox'
        self.Headers = {
            "Host": "https://www.tianyancha.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        self.font_file = 'tyc-num.woff'
        self.font_path = self.workdir
        # 设置浏览器
        options = webdriver.ChromeOptions()
        prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': self.font_path}
        options.add_experimental_option('prefs', prefs)
        self.browser = webdriver.Chrome(executable_path=os.path.join(self.workdir, 'others', 'chromedriver.exe'),
                                        chrome_options=options)
        self.create_woff_map()
        self.login()

    # 创建表格的函数，表格名称按照时间和关键词命名
    def create_table(self):
        self.table_name = "tianyancha"
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
        '''插入数据，不成功就回滚操作'''
        sql = '''REPLACE INTO `{}`(company_name, phone, email, website,
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
                           data.get('地址'), data.get('简介'), data.get('法人'), data.get('注册资本'), data.get('注册时间'),
                           data.get('状态'), data.get('工商注册号'), data.get('组织机构代码'), data.get('统一社会信用代码'),
                           data.get('公司类型'), data.get('纳税人识别号'), data.get('行业'), data.get('营业期限'), data.get('核准日期'),
                           data.get('纳税人资质'), data.get('人员规模'), data.get('实缴资本'), data.get('登记机关'), data.get('参保人数'),
                           data.get('英文名称'), data.get('注册地址'), data.get('经营范围'), data.get('link'), self.from_table,
                           data.get('id_related'), data.get('评分'), data.get('自身风险'), data.get('周边风险'), data.get('预警提醒')))
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

    def create_woff_map(self):
        # 删除font_file:
        font_file = os.path.join(self.font_path, self.font_file)
        try:
            os.remove(font_file)
        except Exception as e:
            print(e)

        # 下载font_file
        self.browser.get(self.url_login)
        woff_url = self.browser.find_element_by_xpath("/html/head/link[5]").get_attribute('href')
        woff_url = woff_url.replace("font.css", self.font_file)
        woff_url = woff_url.replace("css", "fonts")
        self.browser.get(woff_url)
        # 创建字形列表template_glyph_list 及 对应的字列表 template_num_list
        font_template = TTFont(os.path.join(self.workdir, 'tyc-num_template.woff'))
        template_glyph_list = font_template.getGlyphOrder()[2:]
        # template_word = "012456789飞宝名导正保终利系酒然即里东归收程众价流依平攻张意由连字数选听反亲和八想率后章领国院复三台军色花心红推口古手强海增它文已礼边神果深之用取做地容德别多走船论有志命具而广似兴病头夫星医于实各生宗久称年完图让道经须却济受京念所未本最品些者通究才害管阳近右吗案设龙查考制父愿突首种安已车点快引每斯团夜息天清界世远为片使住按足看黑列统也司同特见步单金能被开师义办确运臣在很几动密十前州向与亚日李件观投形转局传房们今子任成送认击术民断电轻活治重少止书党指叫官五朝失候西起务好器节要干甚至下只阿合当组企黄性空关请身便准布回农弟此高包识部克乎省注如政衣产左声真物虽据进死万争县集但江皇给"
        # template_num_list = list(template_word)
        template_num_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        template_font_coordinates = []
        for key in template_glyph_list:
            template_font_coordinates.append(font_template['glyf'][key].coordinates)

        while True:
            try:
                font = TTFont(os.path.join(self.workdir, 'tyc-num.woff'))
                break
            except:
                print("There is no tyc-num.woff. Try again!")
                time.sleep(1)

        # 取字体的交集
        glyph_list = list(set(font.getGlyphOrder()).intersection(set(template_num_list)))
        trans_map_dict = {}
        for key in glyph_list:
            trans_map_dict[key] = template_num_list[template_font_coordinates.index(font['glyf'][key].coordinates)]

        self.trans_map = str.maketrans(trans_map_dict)

        return self.trans_map

    def login(self):
        self.browser.get(self.url_login)
        time.sleep(round(random.uniform(1, 2), 2))
        # 模拟登陆

        WebDriverWait(self.browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR,
                                                                          "#web-content > div > div.container > div > div.login-right > div > div.module.module1.module2.loginmodule.collapse.in > div.title-tab.text-center > div:nth-child(2)")))
        self.browser.find_element(By.CSS_SELECTOR,
                                  "#web-content > div > div.container > div > div.login-right > div > div.module.module1.module2.loginmodule.collapse.in > div.title-tab.text-center > div:nth-child(2)").click()
        self.browser.find_element_by_css_selector(
            "#web-content > div > div.container > div > div.login-right > div > div.module.module1.module2.loginmodule.collapse.in > div.modulein.modulein1.mobile_box.f-base.collapse.in > div.pb30.position-rel > input").send_keys(
            self.username)
        self.browser.find_element_by_css_selector(
            "#web-content > div > div.container > div > div.login-right > div > div.module.module1.module2.loginmodule.collapse.in > div.modulein.modulein1.mobile_box.f-base.collapse.in > div.input-warp.-block > input").send_keys(
            self.password)
        self.browser.find_element_by_css_selector(
            "#web-content > div > div.container > div > div.login-right > div > div.module.module1.module2.loginmodule.collapse.in > div.modulein.modulein1.mobile_box.f-base.collapse.in > div.btn.-hg.btn-primary.-block").click()

    def get_tianyancha(self, company):
        decode_list = []
        base_table = {}
        url_search = self.url_search.format(company)
        self.browser.get(url_search)
        time.sleep(round(random.uniform(1, 2), 2))
        # content = self.browser.page_source.encode('utf-8')
        # soup = BeautifulSoup(content, 'lxml')
        try:
            url_company = self.browser.find_element_by_css_selector(
                "#web-content > div > div.container-left > div > div.result-list > div:nth-child(1) > div > div.content > div.header > a").get_attribute(
                'href')
        except Exception as e:
            print("Error message: ", e)
            try:
                self.browser.find_element_by_id('hideSearching')
                print("没找到结果")
                return None
            except:
                print("get_url 可能是机器人识别， 识别结束后按键继续：", time.asctime())
                while True:
                    time.sleep(1)
                    if self.browser.current_url.find('captcha') < 0:
                        time.sleep(1)
                        break
                print("OK")
                try:
                    url_company = self.browser.find_element_by_css_selector(
                        "#web-content > div > div.container-left > div > div.result-list > div:nth-child(1) > div > div.content > div.header > a").get_attribute(
                        'href')
                except:
                    return None

        self.browser.get(url_company)
        base_table['link'] = url_company
        time.sleep(round(random.uniform(1, 2), 2))
        # 第一个表格信息
        try:
            base_table['公司名称'] = self.browser.find_element_by_xpath(
                '//*[@id="company_web_top"]/div[2]/div[3]/div[1]/h1').text
        except:
            print("（公司地址）可能是机器人识别， 识别结束后按键继续：", time.asctime())
            while True:
                time.sleep(1)
                if self.browser.current_url.find('captcha') < 0:
                    time.sleep(1)
                    break
            print("OK")
            try:
                base_table['公司名称'] = self.browser.find_element_by_xpath(
                    "//div[@id='company_web_top']/div[2]/div[2]/div/h1").text
            except:
                return None
        company_info = self.browser.find_element_by_class_name('detail')
        ## 爬取数据不完整,要支持展开和多项合并
        base_table['电话'] = company_info.text.split('电话：')[1].split('邮箱：')[0].split('查看')[0].split('编辑')[0]
        base_table['邮箱'] = company_info.text.split('邮箱：')[1].split('\n')[0].split('查看')[0]
        base_table['网址'] = company_info.text.split('网址：')[1].split('地址')[0]
        base_table['地址'] = company_info.text.split('地址：')[1].split('\n')[0]

        try:
            abstract = self.browser.find_element_by_xpath(
                "//div[@class='summary']/script")  # @class='sec-c2 over-hide'
            base_table['简介'] = self.browser.execute_script("return arguments[0].textContent",
                                                           abstract).strip()
        except:
            abstract = self.browser.find_element_by_xpath("//div[@class='summary']")
            base_table['简介'] = self.browser.execute_script("return arguments[0].textContent",
                                                           abstract).strip()[3:]

        # baseInfo 表格信息
        try:
            base_info_elements = self.browser.find_elements_by_xpath('//*[@id="_container_baseInfo"]')[0]
        except:
            print("无信息， 放弃爬取")
            return base_table
        base_info_tables = base_info_elements.find_elements_by_tag_name('table')

        if len(base_info_tables) > 1:
            #  baseInfo表有两个table
            # 第一个表
            rows1 = base_info_tables[0].find_elements_by_tag_name('tr')
            try:
                if len(rows1[1].find_elements_by_tag_name('td')[0].text.split('\n')[0]) < 2:
                    base_table['法人'] = rows1[1].find_elements_by_tag_name('td')[0].text.split('\n')[1]
                else:
                    base_table['法人'] = rows1[1].find_elements_by_tag_name('td')[0].text.split('\n')[0]
            except:
                print("信息未公开， 放弃爬取")
                return base_table
            if (base_table['法人'] in ['未公开']):
                print("信息未公开， 放弃爬取")
                return base_table
            try:
                base_table['注册资本'] = rows1[1].find_elements_by_tag_name('td')[1].text.split('\n')[1]
            except:
                base_table['注册资本'] = rows1[1].find_elements_by_tag_name('td')[1].text.split('\n')[0]
            try:
                base_table['注册资本'] = re.search(r'\d+', base_table['注册资本'])[0] + '万人民币'
            except:
                pass

            # 检测注册资本是否需要解密
            try:
                rows1[1].find_elements_by_tag_name('td')[1].find_element_by_class_name('tyc-num')
                decode_list.append('注册资本')
                decode_list.append('核准日期')
            except:
                pass

            try:
                base_table['注册时间'] = rows1[2].find_elements_by_tag_name('td')[0].text.split('\n')[1]
                # 检测注册时间是否需要解密
                try:
                    rows1[2].find_elements_by_tag_name('td')[0].find_element_by_class_name('tyc-num')
                    decode_list.append('注册时间')
                except:
                    pass
            except:
                base_table['注册时间'] = rows1[1].find_elements_by_tag_name('td')[2].text.split('\n')[0]
                # 检测注册时间是否需要解密
                try:
                    rows1[1].find_elements_by_tag_name('td')[2].find_element_by_class_name('tyc-num')
                    decode_list.append('注册时间')
                except:
                    pass

            try:
                base_table['状态'] = rows1[3].find_elements_by_tag_name('td')[0].text.split('\n')[1]
            except:
                base_table['状态'] = rows1[1].find_elements_by_tag_name('td')[3].text.split('\n')[0]

            # 第二个表
            rows2 = base_info_tables[1].find_elements_by_tag_name('tr')
            base_info_list = []
            for row in rows2:
                for td in row.find_elements_by_tag_name('td'):
                    if td.text != '':
                        base_info_list.append(td.text)

            if len(base_info_list) % 2 == 0:
                for i in range(int(len(base_info_list) / 2)):
                    base_table[base_info_list[2 * i]] = base_info_list[2 * i + 1]
            else:
                print('base_table_2（公司基本信表2）行数不为偶数，请检查代码！')

            # 数字解密, 先检查日期是否是正确数字
            # decode_list = ['注册资本', '注册时间', '核准日期']
            decode_list.sort()
            for item in decode_list:
                if base_table.get(item) is not None:
                    tmp = base_table.get(item).translate(self.trans_map)
                    if item in ['注册时间', '核准日期']:
                        try:
                            if tmp[:2] not in ['19', '20']:
                                print("解密出错，退出重先爬取。。。")
                                self.create_woff_map()
                                tmp = base_table.get(item).translate(self.trans_map)
                            else:
                                pass

                        except:
                            pass

                    base_table[item] = tmp
            try:
                score = base_info_tables[1].find_element_by_tag_name('img').get_attribute('alt')
                base_table['评分'] = re.search(r'\d+', score)[0]
            except:
                pass

            try:
                risk_elements = self.browser.find_elements(By.CLASS_NAME, "tag-risk-count")
                base_table['自身风险'] = risk_elements[0].text
                base_table['周边风险'] = risk_elements[1].text
                base_table['预警提醒'] = risk_elements[2].text
            except:
                pass


            # _container_baseInfo > table.table.-striped-col.-border-top-none > tbody > tr:nth-child(1) > td.sort-bg > img
        return base_table

    def get_companys_tyc(self):
        tyc_file = os.path.join(self.workdir, 'tyc_companys.csv')
        if os.path.exists(tyc_file):
            df = pd.read_csv(tyc_file)
            # last_id = max(df['id'])
            tyc_df = pd.read_sql(
                '''SELECT j.id, j.`company_name` FROM  `tianyancha` j WHERE j.id > {}; '''.format(max(df['id'])),
                self.conn)
            df = pd.concat([df, tyc_df])
        else:
            df = pd.read_sql('''SELECT j.id, j.`company_name` FROM  `tianyancha` j ;''', self.conn)

        df.to_csv(tyc_file, index=False)
        return df

    def get_companys_51job(self):
        start_time = time.clock()
        self.from_table = '51job'
        df = pd.read_sql('''SELECT * FROM `tianyancha` WHERE from_table = '51job' ORDER BY id_related DESC LIMIT 1;''',
                         self.conn)
        last_id = 0
        if len(df) > 0:
            last_id = df['id_related'][0]
        companys_51job_df = pd.read_sql(
            '''SELECT j.id, j.`real_name` FROM  `51job` j WHERE j.`id` > {} ORDER BY j.id;'''.format(last_id),
            self.conn)
        companys_tyc_df = self.get_companys_tyc()
        print(time.clock() - start_time)

        df = companys_51job_df[~companys_51job_df['real_name'].isin(companys_tyc_df['company_name'].tolist())]
        df = df.drop_duplicates(subset='real_name')
        print(len(df), "companys for crawling")

        return df

    def get_companys_lagou(self):
        start_time = time.clock()
        self.from_table = 'lagou'
        df = pd.read_sql('''SELECT * FROM `tianyancha` WHERE from_table = 'lagou' ORDER BY id_related DESC LIMIT 1;''',
                         self.conn)
        last_id = 0
        if len(df) > 0:
            last_id = df['id_related'][0]
        companys_lagou_df = pd.read_sql(
            '''SELECT j.id, j.`company_name` FROM  `lagou` j WHERE j.`id` > {} ORDER BY j.id;'''.format(last_id),
            self.conn)

        companys_tyc_df = self.get_companys_tyc()
        print(time.clock() - start_time)
        df = companys_lagou_df[~companys_lagou_df['company_name'].isin(companys_tyc_df['company_name'].tolist())]
        df = df.drop_duplicates(subset='company_name')
        print(len(df), "companys for crawling")

        return df

    def main_51job(self):
        # 爬取51job所有数据库中未爬取的公司
        ret = 1
        companys_df = self.get_companys_51job()
        # 若没有从事可以爬取数据
        if len(companys_df) > 0:
            ret = 0
            companys = companys_df['real_name'].values.tolist()
            for company in companys:
                print("Start to crawl :", companys_df[companys_df['real_name'] == company])
                base_table = self.get_tianyancha(company)
                if base_table is None:
                    continue
                else:
                    print(base_table)
                    base_table['id_related'] = companys_df[companys_df['real_name'] == company]['id'].values[0]
                    self.insert_data(base_table)

        self.close()
        self.browser.close()
        return ret

    def main_lagou(self):
        ret = 1
        # 爬取51job所有数据库中未爬取的公司
        companys_df = self.get_companys_lagou()
        # 若没有从事可以爬取数据
        if len(companys_df) > 0:
            ret = 0
            companys = companys_df['company_name'].values.tolist()
            for company in companys:
                print("Start to crawl :", companys_df[companys_df['company_name'] == company])
                base_table = self.get_tianyancha(company)
                if base_table is None:
                    continue
                else:
                    print(base_table)
                    base_table['id_related'] = companys_df[companys_df['company_name'] == company]['id'].values[0]
                    self.insert_data(base_table)

        self.close()
        self.browser.close()
        return ret

    def main(self, companys):
        tyc_df = pd.read_sql('''select t.company_name from tianyancha t''', self.conn)
        companys_new = set(companys).difference(tyc_df['company_name'].tolist())
        for company in companys_new:
            print("Start to crawl :", company)
            base_table = self.get_tianyancha(company)
            if base_table is None:
                continue
            else:
                print(base_table)
                try:
                    if base_table['注册时间'][:2] in ['19', '20']:
                        self.insert_data(base_table)
                    else:
                        print("解密出错，退出重先爬取。。。")
                        self.create_woff_map()
                        continue
                except:
                    continue

        self.close()
        self.browser.close()
        return 0

    def crawl_company(self, company):
        df = pd.read_sql('''select * from tianyancha t WHERE t.company_name = '{}';'''.format(company), self.conn)
        if len(df) > 0:
            return df

        print("Start to crawl :", company)
        base_table = self.get_tianyancha(company)
        if base_table is not None:
            print(base_table)
            try:
                if base_table['注册时间'][:2] in ['19', '20']:
                    self.insert_data(base_table)
                    df = pd.read_sql('''select * from tianyancha t WHERE t.company_name = '{}';'''.format(company),
                                     self.conn)
                    return df
                else:
                    print("解密出错，退出重先爬取。。。")
                    self.create_woff_map()
                    self.crawl_company(company)
            except:
                pass

        return df


if __name__ == '__main__':
    count = 0

    print("Starting 51job table ")
    while True:
        count += 1
        print(count)
        spider = SpiderTianyangcha()
        ret = spider.main_51job()

        if ret != 0:
            break

    print("Starting lagou table ")
    while True:
        count += 1
        print(count)
        spider = SpiderTianyangcha()
        ret = spider.main_lagou()

        if ret != 0:
            break

    print("tianyancha spider completed")
