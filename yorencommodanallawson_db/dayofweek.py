# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 17:47:58 2017

@author: Yixiang Zhang
"""

import pandas as pd

def dayofweek(SKU,begindate=None,enddate=None,region_block_code=None,cursor=None):
    sql = '''
SELECT c.COMMODITY_ID, c.COMMODITY_NAME, DAYNAME(a.DEAL_TIME) AS dayname, WEEKDAY(a.DEAL_TIME)+1 AS weekday, SUM(sells_count) AS commodity_dayofweek_sells_count

FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c ON b.commodity_id = c.COMMODITY_ID

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

GROUP BY commodity_id, weekday
ORDER BY commodity_id, weekday
;''' % (begindate,enddate,begindate,enddate,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        df3 = pd.DataFrame({'commodity_id':[],'commodity_name':[],'dayname':[],'weekday':[],'commodity_dayofweek_sells_count':[]})
        k = 0
        for row in results:
            df3.loc[k]={'commodity_id':row[0],'commodity_name':row[1],'dayname':row[2],'weekday':row[3],'commodity_dayofweek_sells_count':row[4]}
            k = k + 1
        
        print('Stage 3 Analysis of Day of Week is successful!')
        return df3
    
    except:
        print('Stage 3 Analysis of Day of Week failed!')