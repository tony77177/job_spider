#
#    用于检测WEB首页缓存是否过期机制
#    时间：2018-09-25 22:09:43
#       判断逻辑：
#           1、获取数据总条数 total_num
#           2、用total_num与指定文件数据进行对比：
# 	            A)如果total_num较大，则更新此值，同时删除缓存文件
# 	            B)如果值相等，不进行任何操作
#

import pymysql
import os

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

#   获取现有数据条数
get_num_sql = "SELECT COUNT(*) AS num FROM t_info"
cursor.execute(get_num_sql)
check_result = cursor.fetchone()
# print((check_result[0]))


# check_result[0] = 100

#  进行文件读取，获取上一次存取的条数
cache_path = '/var/www/findthejob/job/job/application/cache/cache_info_list'
file_path = '/root/job_spider/job_spider/check_cache.log'
f = open(file_path, 'r+')
curr_num = f.read()
# print('read : ', type(content))  #

if (check_result[0] > int(curr_num)):
    f.seek(0, 0)
    f.write(str(check_result[0]))
    os.remove(cache_path)

f.close()

# 三、关闭游标
cursor.close()

# 四、关闭对象
conn.close()
