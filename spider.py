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

import logging.handlers
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

    # print(responses.content)

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


#
#    设置日志模块
#
LOG_FILE = r'./spider.log'  #  本地缓解经
# LOG_FILE = r'/root/job_spider/job_spider/spider.log'    #线上环境

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5,
                                               encoding='utf-8')  # 实例化handler
fmt = '%(asctime)s - %(levelname)s - %(message)s'

formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter

logger = logging.getLogger('spider')  # 获取名为spider的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)

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

result = get_target_info(url, 'gb2312', '<br />\r\n(.*?)<a href="(.*?)".*?>(.*?)</a>')

# exit()

# print(check_result[0])

logger.info(u'开始获取信息：')
# logger.debug('first debug message')
# print('开始获取信息：')

# 获取数据条数
total_num = 0

#   验证数据是否重复的SQL


# print(check_result)
num = 0
#   广告数
ad_num = 0

for item in result:

    target_time = re.sub(' ・', '', item[0])  #  页面获取的时间

    #  判断重复逻辑更改为：1、URL是否存在；2、信息在原网站发布时间是否重复
    check_sql = "SELECT COUNT(*) AS num FROM t_info WHERE url='" + item[1] + "' AND target_dt='" + target_time + "'"

    # logger.info(u'check_sql_info：%s' % (check_sql))

    cursor.execute(check_sql)
    check_result = cursor.fetchone()
    num += 1

    # logger.info(u'check_sql_result：%s' % (check_result[0]))

    if check_result[0] == 0:
        if ('163gz.com' in item[1]):  #  判断链接是否为163GZ.COM，否则为广告
            news_title = re.sub('</font>', '', re.sub('<font.*?>', '', item[2]))
            news_title = re.sub('"','',news_title)  #过滤双引号

            curr_time = time.strftime("%Y-%m-%d %H:%M:%S")
            # print('%s,%s,%s' % (cur_time, news_title, item[1]))
            insert_sql = 'INSERT INTO t_info(title,url,target_dt,insert_dt,from_src) VALUES("' + news_title + '","' + item[1] + '","' + target_time + '","' + curr_time + '","163gz.com")'
            print(insert_sql)
            total_num += 1
            cursor.execute(insert_sql)
        else:
            ad_num += 1
            # print('广告信息：%s,%s,%s' % (cur_time, news_title, item[1]))
    else:
        break

#  防止结果为空，进行过滤
# if check_result[0] == 0:
#     for item in result:
#         if ('163gz.com' in item[1]):  #  判断链接是否为163GZ.COM，否则为广告
#             news_title = re.sub('</font>', '', re.sub('<font.*?>', '', item[2]))
#             cur_time = re.sub(' ・', '', item[0])
#             curr_time = time.strftime("%Y-%m-%d %H:%M:%S")
#             # print('%s,%s,%s' % (cur_time, news_title, item[1]))
#             insert_sql = 'INSERT INTO t_info(title,url,insert_dt,from_src) VALUES("' + news_title + '","' + item[
#                 1] + '","' + curr_time + '","163gz.com")'
#             # print(insert_sql)
#             total_num += 1
#             cursor.execute(insert_sql)
#         else:
#             ad_num += 1
# print('广告信息：%s,%s,%s' % (cur_time, news_title, item[1]))

# for i=0
#
# print('倒序之后结果：')
# print(result.reverse())

# exit()


# print('\n')

# 如果数据不为空，则进行数据提交
if (total_num != 0):
    conn.commit()  #  数据提交，否则不生效

logger.info(u'结束获取信息，共计获取：%s 条，广告共计：%s条' % (total_num, ad_num))

logger.info(u'for执行次数：%s' % (num))

# 三、关闭游标
cursor.close()

# 四、关闭对象
conn.close()

# if __name__ == '__init__':
#     print('hello world')
