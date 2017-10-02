# -*- coding: utf-8 -*-
"""
Created on Thu Aug 31 09:15:40 2017

@author: Yixiang Zhang
"""

import pandas as pd

def flow(SKU,begindate1=None,enddate1=None,begindate2=None,enddate2=None,region_block_code=None,cursor=None):
    sql1 = '''
SELECT b.commodity_id, a.USER_ID, COUNT(a.USER_ID) AS commodity_user_visit_count

FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c ON b.commodity_id = c.COMMODITY_ID

WHERE a.PURCHASE_DATE BETWEEN "%s" AND "%s"
AND b.purchase_date BETWEEN "%s" AND "%s"
AND a.REGION_BLOCK_CODE = "%s"
AND b.region_block_code = "%s"
AND c.REGION_BLOCK_CODE = "%s"
AND a.BUSINESS_FLG = 0
AND b.business_flg = 0
AND b.drcr_type = 1
AND a.USER_ID != 0
AND b.commodity_id IN
(
%s
)

GROUP BY b.commodity_id, a.USER_ID
ORDER BY b.commodity_id, a.USER_ID
;''' % (begindate1,enddate1,begindate1,enddate1,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql1)
        results = cursor.fetchall()
        
        df5_1 = pd.DataFrame({'commodity_id':[],'user_id':[],'commodity_user_visit_count':[]})
        k = 0
        for row in results:
            df5_1.loc[k]={'commodity_id':row[0],'user_id':row[1],'commodity_user_visit_count':row[2]}
            k = k + 1
            
        print('Stage 5 Step 1 Flow 1 is successful!')
    
    except:
        print('Stage 5 Step 1 Flow 1 failed!')

###############################################################################

    sql2 = '''
SELECT b.commodity_id, a.USER_ID, COUNT(a.USER_ID) AS commodity_user_visit_count

FROM yoren_new_pos_log a
INNER JOIN t_pos_purchase_commodity_log b ON a.SHOP_ID = b.SHOP_ID AND a.POS_NO = b.POS_NO AND a.DEAL_TIME = b.DEAL_TIME and a.SERIAL_NUMBER = b.SERIAL_NUMBER
LEFT JOIN yoren_commodity_mst c ON b.commodity_id = c.COMMODITY_ID

WHERE a.PURCHASE_DATE BETWEEN "%s" AND "%s"
AND b.purchase_date BETWEEN "%s" AND "%s"
AND a.REGION_BLOCK_CODE = "%s"
AND b.region_block_code = "%s"
AND c.REGION_BLOCK_CODE = "%s"
AND a.BUSINESS_FLG = 0
AND b.business_flg = 0
AND b.drcr_type = 1
AND a.USER_ID != 0
AND b.commodity_id IN
(
%s
)

GROUP BY b.commodity_id, a.USER_ID
ORDER BY b.commodity_id, a.USER_ID
;''' % (begindate2,enddate2,begindate2,enddate2,region_block_code,region_block_code,region_block_code,SKU)

    try:
        cursor.execute(sql2)
        results = cursor.fetchall()
        
        df5_2 = pd.DataFrame({'commodity_id':[],'user_id':[],'commodity_user_visit_count':[]})
        k = 0
        for row in results:
            df5_2.loc[k]={'commodity_id':row[0],'user_id':row[1],'commodity_user_visit_count':row[2]}
            k = k + 1
            
        print('Stage 5 Step 2 Flow 2 is successful!')
    
    except:
        print('Stage 5 Step 2 Flow 2 failed!')

###############################################################################

    try:
        newdata = pd.merge(df5_1,df5_2,on="user_id",how='outer')
        newdata = newdata[['user_id','commodity_id_x','commodity_id_y']]
        newdata.columns
        
        x = newdata.groupby(newdata['commodity_id_x']).size()
        levels_x = list(x.index)
        y = newdata.groupby(newdata['commodity_id_y']).size()
        levels_y = list(y.index)
        
        levels_x = levels_x + ['NA']
        levels_y = levels_y + ['NA']
        
        newdata = newdata.fillna('NA')
        sizes = newdata.groupby(['commodity_id_x','commodity_id_y']).size()
        sizes = sizes.unstack()
        
        print('Stage 5 Analysis of Flow is successful!')
        print(sizes)
        return sizes

    except:
        print('Stage 5 Analysis of Flow failed!')