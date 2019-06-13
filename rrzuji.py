# -*- coding:utf-8 -*-
'''
前程无忧网招聘信息爬虫
'''
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from sql.sql import sql_connect
import time, random
import os
import pandas as pd
import uuid


# https://m.rrzuji.com/individual/cate/list?brand=1&cate=phone&cate_name=%E8%8B%B9%E6%9E%9C&short=hz&page=1

# https://h5.jimistore.com/#/moreType

def get_uuid():
    return str(uuid.uuid4())


class Spider51job(object):
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
        # self.table_name = "51job".format(create_time)
        self.table_name = "51job"

        sql = '''CREATE TABLE if not exists `{tbname}`(
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
            `company_name` varchar(50) UNIQUE ,
            `real_name` varchar(255),
            `job_site` char(20),
            `company_link` varchar(255),
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
        sql = '''INSERT INTO `{}`(company_name, real_name, job_site, company_link) VALUES('{}','{}','{}','{}')'''
        try:
            self.conn.cursor().execute(
                sql.format(self.table_name, data.get('company_name'), data.get('real_name'), data.get('job_site'),
                           data.get('company_link')))

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

    def get_one_page(self, url):
        '''从第一页开始，获取信息，并判断是否有下一页，如若有则继续爬虫，递归翻页'''
        cookie = "JSESSIONID=" + get_uuid() + ";" + "user_trace_token=" + get_uuid() + "; LGUID=" + get_uuid() + "; index_location_city=%E6%88%90%E9%83%BD; " + "SEARCH_ID=" + get_uuid() + '; _gid=GA1.2.717841549.1514043316; '                                                                                                                                         '_ga=GA1.2.952298646.1514043316; '                                                                                                                                          'LGSID=' + get_uuid() + "; "                                                                                                                                                                  "LGRID=" + get_uuid() + "; "
        headers = {'cookie': cookie, 'origin': "https://www.51job.com", 'x-anit-forge-code': "0",
                   'accept-encoding': "gzip, deflate, br", 'accept-language': "zh-CN,zh;q=0.8,en;q=0.6",
                   'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
                   'content-type': "application/x-www-form-urlencoded; charset=UTF-8",
                   'accept': "application/json, text/javascript, */*; q=0.01",
                   'referer': "https://www.lagou.com/jobs/list_%20?px=new&city=%E6%88%90%E9%83%BD",
                   'x-requested-with': "XMLHttpRequest", 'connection': "keep-alive", 'x-anit-forge-token': "None",
                   'cache-control': "no-cache", 'postman-token': "91beb456-8dd9-0390-a3a5-64ff3936fa63"}
        url = "https://h5.jimistore.com/#/tab/leaseChoose/969666"
        url = "https://m.rrzuji.com/item/ph4205"
        # url = "https://miku.tools/"
        req = requests.get(url, headers=headers)
        req.encoding = "utf-8"
        soup = BeautifulSoup(req.text, "lxml")
        # 获取所有职位信息，第一条是标题
        jobs = soup.select("#resultList > div.el")[1:]
        for job in jobs:
            data = {}
            # data["job_name"] = job.select("p.t1")[0].text.strip()
            # data["job_link"] = job.select("p.t1 > span > a")[0].get("href")
            data["company_name"] = job.select("span.t2")[0].text.strip()
            data["real_name"] = data["company_name"]

            data["company_link"] = job.select("span.t2 > a")[0].get("href")
            data["job_site"] = self.city
            # data["job_site"] = job.select("span.t3")[0].text.strip()
            # data["salary"] = job.select("span.t4")[0].text.strip()
            # data["create_date"] = job.select("span.t5")[0].text.strip()
            # company_name = job.select("span.t2")[0].text.strip()

            if data["company_name"] not in self.all_companys:
                try:
                    req_company = requests.get(data["company_link"], headers=self.Headers)
                    req_company.encoding = "gbk"
                    soup_company = BeautifulSoup(req_company.text, "lxml")
                    company = soup_company.select(
                        "body > div.tCompanyPage > div.tCompany_center.clearfix > div.tHeader.tHCop > div > p.blicence > span")
                    try:
                        data["real_name"] = company[0].text.strip('营业执照：')
                    except Exception as e:
                        # print('无营业执照对应的公司名称', e)
                        pass
                    req_company.close()
                except:
                    pass

                self.insert_data(data)
                print(data.values())
                self.all_companys.append(data["company_name"])
        req.close()
        print(self.city, len(self.all_companys) - self.prevous_company_count, "companys added")
        if len(self.all_companys) == self.prevous_company_count:
            self.none_count += 1
            if self.none_count > 3:
                return 0
        else:
            self.prevous_company_count = len(self.all_companys)
            self.none_count = 0

        try:
            next_url = soup.select("li.bk")[-1].select("a")[0].get("href")
            pagenum = re.findall(",(\d+)\.html", next_url)[0]
            print("开始爬取第{}页的信息".format(pagenum))
            time.sleep(round(random.uniform(2, 4), 2))
            self.get_one_page(next_url)
        except:
            print("无法获取下一页的链接，想必已经爬到了最后一页了，爬虫即将结束")
            return 0

    def main(self, city, keyword):
        self.none_count = 0
        self.key = keyword
        self.city = city
        self.all_companys = self.get_companys()
        self.prevous_company_count = len(self.all_companys)
        self.Headers = {
            "Host": "search.51job.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.110 Safari/537.36"
        }
        # 获取城市代码
        citynum = city_to_num_dict[city]
        self.start_url = "https://search.51job.com/list/{},000000,0000,00,0,99,{},2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=0&fromType=5&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=".format(
            quote(citynum),
            quote(self.key))
        try:
            self.get_one_page(self.start_url)
        except Exception as e:
            print(e)
        finally:
            self.close()


if __name__ == '__main__':
    citys = ['杭州', '上海', '宁波', '温州', '湖州', '台州', '金华', '绍兴', '嘉兴',
             '南京', '苏州', '常州', '无锡', '南通', '扬州', '镇江', '昆山', '宿迁', '连云港',
             '合肥', '黄山',  '衢州', '舟山',]

    # for city in citys:
    #     spider = Spider51job()
    #     spider.main(city, ' ')

    print("Mission Complete!")
