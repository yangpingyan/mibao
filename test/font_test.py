#!/usr/bin/env python 
# coding: utf-8
# @Time : 2018/12/14 16:37 
# @Author : yangpingyan@gmail.com

#闻 unicode0x95FB， 技 _#218
#所 ， 术
#片  发
#9， 2
# 6， 1
# 1， 8

from fontTools.ttLib import TTFont
from fontTools.ttLib import tables as ft_tables

font = TTFont('tyc-num.woff')
font.saveXML('tyc-num.xml')
# unicode编码对应的font_name
cmap_dict = font['cmap'].getBestCmap()
key = int(hex(ord('6')), 16)
cmap_dict.get(key)
int(hex(ord('1')), 16)

