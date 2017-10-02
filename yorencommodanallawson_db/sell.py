# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 13:04:26 2017

@author: Yixiang Zhang
"""

import pandas as pd

def sell(SKU,begindate=None,enddate=None,region_block_code=None,cursor=None):
    sql = '''
SELECT u.commodity_id, u.commodity_name, SUM(u.user_commodity_sells_count) AS commodity_sells_count, COUNT(*) AS user_count, u.sell_price, SUM(u.user_commodity_money_sum) AS commodity_money_sum
FROM

(
SELECT a.user_id, c.COMMODITY_ID AS commodity_id, c.COMMODITY_NAME AS commodity_name, SUM(b.sells_count) AS user_commodity_sells_count, c.SELL_PRICE AS sell_price, SUM(b.discount_tax_inclusive_price) AS user_commodity_money_sum
FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c on b.commodity_id = c.COMMODITY_ID
WHERE a.purchase_date BETWEEN "%s" and "%s"
AND b.purchase_date BETWEEN "%s" and "%s"
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

GROUP BY u.commodity_id
ORDER BY u.commodity_id
;''' % (begindate,enddate,begindate,enddate,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        df1 = pd.DataFrame({'commodity_id':[],'commodity_name':[],'commodity_sells_count':[],'user_count':[], 'sell_price':[],'commodity_money_sum':[]})
        k = 0
        for row in results:
            df1.loc[k]={'commodity_id':row[0],'commodity_name':row[1],'commodity_sells_count':row[2],'user_count':row[3],'sell_price':row[4],'commodity_money_sum':row[5]}
            k = k + 1
            
        print('Stage 1 Analysis of Sell is successful!')
        return df1
    
    except:
        print('Stage 1 Analysis of Sell failed!')