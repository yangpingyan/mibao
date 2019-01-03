#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2018/12/19 22:03 
# @Author : yangpingyan@gmail.com
import requests
from splinter.browser import Browser
import time, sys, os
from datetime import datetime
import random
from datetime import timedelta


# 读取城市中文与三字码映射
def get_city_code():
    url_city_code = 'https://kyfw.12306.cn/otn/resources/js/framework/station_name.js?station_version=1.9018'
    response = requests.get(url_city_code)
    content = response.text.split('@')
    content.pop(0)
    city_code_dict = {}
    for each in content:
        code_list = each.split('|')
        city_code_dict[code_list[1]] = (code_list[1] + '%2c' + code_list[2]).encode('unicode_escape').decode(
            "utf-8").replace("\\u", "%u")

    return city_code_dict


city_code_dict = get_city_code()

# 席别编码
seat_type_dict = {
    "硬座": "1",
    "硬卧": "3",
    "软卧": "4",
    "一等软座": "7",
    "二等软座": "8",
    "商务座": "9",
    "一等座": "M",
    "二等座": "O",
    "混编硬座": "B",
    "特等座": "P"
}

username = 'yangpingyan'
passwd = ''
# 始发站
starts_city = city_code_dict['鳌江']
ends_city = city_code_dict['杭州东']

# 发车时间
### start_time：发车时间，可选参数，不指定请删除等号后的值，默认值“00:00--24:00”
### 时间格式 12:00--18:00，有效值如下：
##### 00:00--24:00->00:00--24:00
##### 00:00--06:00->00:00--06:00
##### 06:00--12:00->06:00--12:00
##### 12:00--18:00->12:00--18:00
##### 18:00--24:00->18:00--24:00
train_time = '00:00--24:00'
order_number_list = [15]

# 网址
ticket_url = 'https://kyfw.12306.cn/otn/leftTicket/init'
login_url = 'https://kyfw.12306.cn/otn/login/init'
logined_url = 'https://kyfw.12306.cn/otn/view/index.html'
buy_url = 'https://kyfw.12306.cn/otn/confirmPassenger/initDc'

# 席别
seat_type = seat_type_dict['二等座']

# 是否允许分配无座
noseat_allow = 0

# 浏览器驱动（目前使用的是chromedriver）路径
executable_path = 'C:\ProgramData\Anaconda3\envs\envforall\chromedriver.exe'

browser = Browser(driver_name='chrome', executable_path=executable_path)
print("开始登录...")
browser.visit(login_url)
browser.fill("loginUserDTO.user_name", username)
# browser.fill("userDTO.password", passwd)
print(u"等待验证码，自行输入...")
# 验证码需要自行输入，程序自旋等待，直到验证码通过，点击登录
while browser.url != logined_url:
    time.sleep(1)


# In[]

browser.visit(ticket_url)
## order_number：车次，选择第几趟，0则从上至下依次点击，必选参数，如果要特定车次，需要先找到车次在列表中的次序，有效值如下：
# 0->从上至下点击
# 1->第一个车次
# 2->第二个车次
train_date = datetime.now() + timedelta(days=29)
train_date = train_date.strftime("%Y-%m-%d")
print(train_date)
passengers = ['张桦', '杨平言', '杨擎天']

# 加载查询信息
browser.cookies.add({"_jc_save_fromStation": starts_city})
browser.cookies.add({"_jc_save_toStation": ends_city})
browser.cookies.add({"_jc_save_fromDate": train_date})
browser.reload()


# 11：30前刷新， 剩余10秒退出
rob_time = datetime.now().replace(hour=11, minute=29, second=44)
while True:
    now = datetime.now()
    print("sleeping ", now)
    if now > rob_time:
        print("start to rob ticket!!!")
        break
    else:
        time.sleep(round(random.uniform(12, 14), 2))
        browser.find_by_text(u"查询").click()



# 预定车次算法：根据order的配置确定开始点击预订的车次，0-从上至下点击，1-第一个车次，2-第二个车次，类推
count = 0
while browser.url == ticket_url:
    # 勾选车次类型，发车时间
    # 选择车次类型
    # browser.find_by_text(u'GC-高铁/城际').click()
    # browser.find_by_text(u'D-动车').click()
    # browser.find_option_by_text(train_time).first.click()
    try:
        browser.find_by_text(u"查询").click()
    except:
        pass
    time.sleep(0.1)
    count += 1
    print(u"循环查询第%s次... " % count)

    if browser.url != ticket_url:
        break

    try:
        try:
            for order_number in order_number_list:
                print("order_number ", order_number)
                browser.find_by_text(u"预订")[order_number - 1].click()
        except:
            pass
        time.sleep(0.1)
        if browser.url != ticket_url:
            break
    except Exception as e:
        print(e)
        print(u"还没开始预订")
        continue



print(u"开始预订...")
running_time = time.clock()

time.sleep(0.1)
# 选择用户
print(u'选择用户...')
while True:
    if len(browser.find_by_text(passengers[0])) != 0:
        for passenger in passengers:
            browser.find_by_text(passenger).last.click()
        break

    else:
        time.sleep(0.1)
        print("等待选择用户")

print(u"确认儿童票...")
try:
    browser.find_by_id('dialog_xsertcj_ok').click()
except Exception as e:
    print(e)
    pass

print(u"提交订单...")
while True:
    try:
        browser.find_by_id('submitOrder_id').click()
        break
    except Exception as e:
        print("提交订单错误， 重试。。。")
        time.sleep(0.1)

time.sleep(0.5)
print(u"确认选座...")
count = 0
while True:
    try:
        browser.find_by_id('qr_submit_id').click()
        # browser.find_by_id("back_edit_id").click()

    except Exception as e:
        print("确认选座错误， 重试。。。")
        time.sleep(0.1)


print("抢票结束，手动支付...")
print("抢票耗时：{}".format(time.clock() - running_time))

