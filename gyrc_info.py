#!usr/bin/python


#   用于爬取gyrc.com.cn的网站信息
#   作者：赵昱
#   时间：2018-09-24 13:26:13
#

from urllib import request
import requests
import re

import pymysql
import json
import logging.handlers
import time


#    获取目标信息
def get_target_info(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"
    }
    responses = requests.get(url, headers)
    return responses


url = 'http://www.gyrc.cn/wzqt/xxzx/xxzx/xxzxsyCx?pageSize=20&pageNum=1'

result = get_target_info(url)

data = json.loads(result.text)['returnData']['zxzxs']['xxzxs']  # 获取JSON结果
result_info = []
for item in data:
    # print(item['xw_id'])
    info = {
        'curr_time': item['cjrq'],
        'id': item['xw_id'],
        'title': item['bt']
    }
    # print(info)
    result_info.append(info)

# print(result_info)
# exit()

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

# exit()

# print(check_result[0])

logger.info(u'gyrc.com.cn 开始获取信息：')
# logger.debug('first debug message')
# print('开始获取信息：')

# 获取数据条数
total_num = 0

#   验证数据是否重复的SQL


# print(check_result)
num = 0
#   广告数
ad_num = 0

for item in result_info:

    # print(item)

    target_time = item['curr_time']  #  页面获取的时间

    # 拼接新URL
    new_url = "http://www.gyrc.com.cn/information/policyDetailed.html?xwid=" + item['id'] + "&dlid=10000000"

    #  判断重复逻辑更改为：1、URL是否存在；2、信息在原网站发布时间是否重复
    check_sql = "SELECT COUNT(*) AS num FROM t_info WHERE url='" + new_url + "' AND target_dt='" + target_time + "'"

    logger.info(u'check_sql_info：%s' % (check_sql))

    cursor.execute(check_sql)
    check_result = cursor.fetchone()
    num += 1

    logger.info(u'check_sql_result：%s' % (check_result[0]))

    if check_result[0] == 0:
        news_title = item['title']

        curr_time = time.strftime("%Y-%m-%d %H:%M:%S")

        #   此处的目标URL需要进行拼接，http://www.gyrc.com.cn/information/policyDetailed.html?xwid=fb29abc8956b00000000&dlid=10000000
        insert_sql = 'INSERT INTO t_info(title,url,target_dt,insert_dt,from_src) VALUES("' + news_title + '","' + new_url + '","' + target_time + '","' + curr_time + '","gyrc.com.cn")'
        # print(insert_sql)
        total_num += 1
        cursor.execute(insert_sql)
    else:
        break

# print('\n')

# 如果数据不为空，则进行数据提交
if (total_num != 0):
    conn.commit()  #  数据提交，否则不生效

logger.info(u'gyrc.com.cn结束获取信息，共计获取：%s 条，广告共计：%s条' % (total_num, ad_num))

logger.info(u'for执行次数：%s' % (num))

# 三、关闭游标
cursor.close()

# 四、关闭对象
conn.close()

# if __name__ == '__init__':
#     print('hello world')
