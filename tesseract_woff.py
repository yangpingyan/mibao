#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2018/12/14 20:30 
# @Author : yangpingyan@gmail.com

from io import BytesIO
from fontTools.ttLib import TTFont
from PIL import Image, ImageFont, ImageDraw
import pytesseract
# TODO: 训练tesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe'  # 设置tesseract安装目录
tessdata_dir_config = r'--tessdata-dir "C:\Program Files (x86)\Tesseract-OCR\tessdata" --psm 10 -l chi_sim'
r"""
tessdata-dir 设置tessdata路径
psm 10 每张图片仅仅有一个文字
digits 读取tessdata\configs\digits配置 设置图片的内容只为数字 提高正确率
"""

font = TTFont('tyc-num.woff', recalcBBoxes=False, recalcTimestamp=False)  # Pillow不支持用woff生成文字图片
# unicode编码对应的font_name
cmap_dict = font['cmap'].getBestCmap()
# int(hex(ord('片')), 16)
font.flavor = None
font.save('tyc-num.ttf', reorderTables=True)  # 将字体文件从woff转换成ttf

word_map_dict = {}
unicode_map_dict = {}
for key in cmap_dict.keys():
    word_key = chr(key)
    img = Image.new("RGB", (50, 60), (255, 255, 255))  # 生成一张25*50白底的图片
    dr = ImageDraw.Draw(img)  # 开始画图
    image_font = ImageFont.truetype('tyc-num.ttf', 50)  # 设置字体文件和大小
    dr.text((0, 0), word_key, font=image_font, fill="#000000")  # 用设置的字体在0,0的位置插入黑色文字
    text = pytesseract.image_to_string(img, config=tessdata_dir_config)  # 使用pytesseract进行分析
    print(text)
    # 把图片转换成bytes数据
    # imgByteArr = BytesIO()
    # img.save(imgByteArr, format='PNG')
    # imgByteArr = imgByteArr.getvalue()
    word_map_dict[word_key] = text
    unicode_map_dict[hex(key)] = text
    if len(word_map_dict) > 12:
        break

print(word_map_dict)
print(unicode_map_dict)



