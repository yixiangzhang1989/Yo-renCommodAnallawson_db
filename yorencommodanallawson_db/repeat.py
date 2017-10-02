# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 16:37:35 2017

@author: Yixiang Zhang
"""

import pandas as pd

def repeat(SKU,begindate=None,enddate=None,region_block_code=None,cursor=None):
    sql = '''
SELECT u.commodity_id, u.commodity_name, u.user_commodity_visit_count, COUNT(*) 
FROM

(
SELECT a.user_id AS user_id, c.COMMODITY_ID AS commodity_id ,c.COMMODITY_NAME AS commodity_name,
       CASE COUNT(a.USER_ID)
            WHEN 1 THEN '1'
            WHEN 2 THEN '2'
            WHEN 3 THEN '3'
            WHEN 4 THEN '4'
            ELSE '>=5' END user_commodity_visit_count #期限内某人购买某物的总次数
FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c on b.commodity_id = c.COMMODITY_ID
WHERE a.purchase_date BETWEEN "%s" AND "%s"
AND b.purchase_date BETWEEN "%s" AND "%s"
AND a.region_block_code = "%s"
AND b.region_block_code = "%s"
AND c.region_block_code = "%s"
AND a.BUSINESS_FLG = 0
AND b.BUSINESS_FLG = 0
AND b.drcr_type = 1
AND a.USER_ID != 0
AND c.COMMODITY_ID IN 
(
%s
)
GROUP BY 1, 2
ORDER BY 1, 2
) u

GROUP BY u.commodity_id, u.user_commodity_visit_count
ORDER BY u.commodity_id, u.user_commodity_visit_count
;''' % (begindate,enddate,begindate,enddate,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        df2 = pd.DataFrame({'commodity_id':[],'commodity_name':[],'user_commodity_visit_count':[],'user_count':[]})
        k = 0
        for row in results:
            df2.loc[k]={'commodity_id':row[0],'commodity_name':row[1],'user_commodity_visit_count':row[2],'user_count':row[3]}
            k = k + 1
            
        print('Stage 2 Analysis of Repeat is successful!')
        return df2
    
    except:
        print('Stage 2 Anaysis of Repeat failed!')