# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 13:35:03 2017

@author: Yixiang Zhang
"""

import sshtunnel,pymysql,time
import numpy as np
import pandas as pd


#输入数据库的用户名和密码
username = 'shlawson'
password = 'Lawson2017'

server = sshtunnel.SSHTunnelForwarder(('115.29.237.50', 22),ssh_password="3vkxgKqX7D",ssh_username="ydoor",remote_bind_address=('rdsnfjbfyfquanq.mysql.rds.aliyuncs.com', 3306))
server.start()
connection = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user=username, passwd=password, db='lawson_db', charset='utf8')
cursor = connection.cursor()
print(time.ctime(),'连接成功，开始查询')

import yorencommodanallawson_db.sell
df1 = yorencommodanallawson_db.sell.sell(SKU='01063339,01239232,01222372,01200972,01076431,01010251,01097802,01100781,01034126,01076112,01162205,01136042,01329584,01320757,01314021', begindate='20170601',enddate='20170630',region_block_code='sh-lawson',cursor=cursor)

import yorencommodanallawson_db.repeat
df2 = yorencommodanallawson_db.repeat.repeat(SKU='01063339,01239232,01222372,01200972,01076431,01010251,01097802,01100781,01034126,01076112,01162205,01136042,01329584,01320757,01314021', begindate='20170601',enddate='20170630',region_block_code='sh-lawson',cursor=cursor)

import yorencommodanallawson_db.dayofweek
df3 = yorencommodanallawson_db.dayofweek.dayofweek(SKU='01063339,01239232,01222372,01200972,01076431,01010251,01097802,01100781,01034126,01076112,01162205,01136042,01329584,01320757,01314021', begindate='20170601',enddate='20170630',region_block_code='sh-lawson',cursor=cursor)

import yorencommodanallawson_db.gender
df4 = yorencommodanallawson_db.gender.gender(SKU='01063339,01239232,01222372,01200972,01076431,01010251,01097802,01100781,01034126,01076112,01162205,01136042,01329584,01320757,01314021', begindate='20170601',enddate='20170630',region_block_code='sh-lawson',cursor=cursor)

import yorencommodanallawson_db.flow
df5 = yorencommodanallawson_db.flow.flow(SKU='01063339,01239232,01222372,01200972,01076431,01010251,01097802,01100781,01034126,01076112,01162205,01136042,01329584,01320757,01314021', begindate1='20170501',enddate1='20170531',begindate2='20170601',enddate2='20170630',region_block_code='sh-lawson',cursor=cursor)

connection.close()