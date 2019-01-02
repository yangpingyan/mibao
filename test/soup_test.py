#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2018/12/8 22:19 
# @Author : yangpingyan@gmail.com


from bs4 import BeautifulSoup

html = '''
<html><head><title>The Dormouse's story</title></head>
<body>
<p class="title"><b>The Dormouse's story</b></p>

<p class="story">Once upon a time there were three little sisters; and their names were
<a href="http://example.com/elsie" class="sister" id="link1">Elsie</a>,
<a href="http://example.com/lacie" class="sister" id="link2">Lacie</a> and
<a href="http://example.com/tillie" class="sister" id="link3">Tillie</a>;
and they lived at the bottom of a well.</p>
<p class="story">...</p>
'''
soup = BeautifulSoup(html,'lxml')
print(soup.prettify())
print(soup.title)
print(soup.title.name)
print(soup.title.string)
print(soup.title.parent.name)
print(soup.p)
print(soup.p["class"])
print(soup.a)
print(soup.find_all('a'))
print(soup.find(id='link3'))
for link in soup.find_all('a'):
    print(link.get('href'))

print(soup.get_text())

s = "012456789飞宝名导正保终利系酒然即里东归收程众价流依平攻张意由连字数选听反亲和八想率后章领国院复三台军色花心红推口古手强海增它文已礼边神果深之用取做地容德别多走船论有志命具而广似兴病头夫星医于实各生宗久称年完图让道经须却济受京念所未本最品些者通究才害管阳近右吗案设龙查考制父愿突首种安已车点快引每斯团夜息天清界世远为片使住按足看黑列统也司同特见步单金能被开师义办确运臣在很几动密十前州向与亚日李件观投形转局传房们今子任成送认击术民断电轻活治重少止书党指叫官五朝失候西起务好器节要干甚至下只阿合当组企黄性空关请身便准布回农弟此高包识部克乎省注如政衣产左声真物虽据进死万争县集但江皇给"
list(s)
