# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 18:21:21 2017

@author: Yixiang Zhang
"""

import pandas as pd

def gender(SKU,begindate=None,enddate=None,region_block_code=None,cursor=None):
    sql = '''
SELECT c.COMMODITY_ID, c.COMMODITY_NAME,
       CASE d.gender
            WHEN 1 THEN 'M'
            WHEN 2 THEN 'F'
       END gender,
       COUNT(DISTINCT a.user_id) AS commodity_gender_user_count

FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c ON b.commodity_id = c.COMMODITY_ID
LEFT JOIN t_user d ON a.USER_ID = d.user_id

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

GROUP BY commodity_id, gender
ORDER BY commodity_id, gender
;''' % (begindate,enddate,begindate,enddate,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        df4 = pd.DataFrame({'commodity_id':[],'commodity_name':[],'gender':[],'commodity_gender_user_count':[]})
        k = 0
        for row in results:
            df4.loc[k]={'commodity_id':row[0],'commodity_name':row[1],'gender':row[2],'commodity_gender_user_count':row[3]}
            k = k + 1
            
        print('Stage 4 Anaysis of Gender is successful!')
        return df4
    
    except:
        print('Stage 4 Analysis of Gender failed!')