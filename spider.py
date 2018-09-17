#!usr/bin/python


#   爬虫主文件
#   作者：赵昱
#   时间：2018-09-16 17:48:32
#   内容说明：主要用于爬取 163gz.com 内容信息，其余站点逐渐适配
#   内容增加测试
#

from urllib import request
import requests
import re

import pymysql

import json
import time


#    获取目标信息
def get_target_info(url, encoding, pattern):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"
    }
    responses = requests.get(url, headers)
    responses.encoding = encoding

    result = re.findall(re.compile(pattern), responses.text)

    return result


# headers = {
#     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15"
# }

url = 'http://www.163gz.com/js/163.html'

# req = request.Request(url, headers=headers)

# response = request.urlopen(req, timeout=3)

# print(response.read().decode('gb2312','ignore'))
#
# responses = requests.get(url)
#
# responses.encoding = 'gb2312'  # 解决中文乱码问题，根据网站本身编码填入相应编码代码
#
# print(responses.text)
#
# print('\r准备开始匹配：\r')
#
# #  正文利用正则表达式进行匹配
# # pattern = re.compile(r'<p.*?class=\"STYLE4\".*?>.*?<\/p>',re.S)     #用于匹配招聘信息内容的正则表达式
# # results = re.findall(pattern,responses.text)
#
# pattern_02 = re.compile('<br />\r\n(.*?)<a href="(.*?)".*?>(.*?)</a><br />')
# results_02 = re.findall(pattern_02, responses.text)
#
# # print(results)
#
# print('\r匹配结束。\r')
#
# print(results_02)
#
# print('\r开始准备提取结果：\r')
#

# gyrc_url = 'http://www.gyrc.cn/wzqt/xxzx/xxzx/xxzxsyCx?pageSize=10000&pageNum=0'
#
# responses = requests.get(gyrc_url, headers)
#
# print(responses.text)


# 一、创建数据库对象connection
conn = pymysql.connect(
    host='localhost',
    user='job',
    passwd='xdjyf7CuloKwZTqJ',
    charset='utf8'
)
# 二、创建数据库数据承载对象--获取游标对象cursor
cursor = conn.cursor()

# 2.选择数据库
conn.select_db('job')

result = get_target_info(url, 'gb2312', '<br />\r\n(.*?)<a href="(.*?)".*?>(.*?)</a><br />')

print('开始获取信息：')

for item in result:
    # url,name = result
    if ('163gz.com' in item[1]):  #  判断链接是否为163GZ.COM，否则为广告
        news_title = re.sub('</font>', '', re.sub('<font.*?>', '', item[2]))
        cur_time = re.sub(' ・', '', item[0])
        curr_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print('%s,%s,%s' % (cur_time, news_title, item[1]))
        insert_sql = 'INSERT INTO t_info(title,url,insert_dt,from_src) VALUES("' + news_title + '","' + item[
            1] + '","' + curr_time + '","163gz.com")'
        print(insert_sql)
        cursor.execute(insert_sql)


    # print('\n')
conn.commit()   # 数据提交，否则不生效

print('结束获取信息！')

# 三、关闭游标
cursor.close()

# 四、关闭对象
conn.close()
