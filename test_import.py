import pandas as pd
import redis
import time
import pandas as pd
import ast
from queue import Queue,Empty
from queue import LifoQueue
from threading import Thread
from sqlalchemy import create_engine
import sys
import connectorx as cx
from decimal import Decimal
from datetime import datetime
import gc
def create_file():
    with open('realtime_ord.csv','w',encoding='utf-8') as f:
        f.write('Type,Security_Code,Exchange_Code,Security_Id,Security_Type,Listed_Shares,Time,Trade_Date,Ceiling_Price,Floor_Price,Ref_Price,Buy_Price1,Buy_Volume1,Buy_Price2,Buy_Volume2,Buy_Price3,Buy_Volume3,Sell_Price1,Sell_Volume1,Sell_Price2,Sell_Volume2,Sell_Price3,Sell_Volume3,Buy_Price4,Buy_Volume4,Buy_Price5,Buy_Volume5,Buy_Price6,Buy_Volume6,Buy_Price7,Buy_Volume7,Buy_Price8,Buy_Volume8,Buy_Price9,Buy_Volume9,Buy_Price10,Buy_Volume10,Sell_Price4,Sell_Volume4,Sell_Price5,Sell_Volume5,Sell_Price6,Sell_Volume6,Sell_Price7,Sell_Volume7,Sell_Price8,Sell_Volume8,Sell_Price9,Sell_Volume9,Sell_Price10,Sell_Volume10,Close_Price,Match_Volume,Match_Value,Diff,Diff_Rate,Open_Price,High_Price,Low_Price,Average_Price,Total_Volume,Total_Value,Match_Price_Pt,Match_Volume_Pt,Total_Pt_Volume,Total_Pt_Value,F_Buy_Volume,F_Buy_Volume_Last,F_Sell_Volume,F_Sell_Volume_Last,F_Buy_Value,F_Sell_Value,F_Room,F_Room_Pct,Total_Room,Buy_Order,Sell_Order,Total_Buy_Volume,Total_Sell_Volume,Remain_Buy,Remain_Sell,Open_Interest,Last_Trade_Date,Vn30_Basis,Stock_Basis,Balance_Price,INav,IIndex')
        f.write('\n')
def create_pd():
    df = pd.DataFrame(columns=['Type','Security_Code','Exchange_Code','Security_Id','Security_Type','Listed_Shares','Time','Trade_Date','Ceiling_Price','Floor_Price','Ref_Price','Buy_Price1','Buy_Volume1','Buy_Price2','Buy_Volume2','Buy_Price3','Buy_Volume3','Sell_Price1','Sell_Volume1','Sell_Price2','Sell_Volume2','Sell_Price3','Sell_Volume3','Buy_Price4','Buy_Volume4','Buy_Price5','Buy_Volume5','Buy_Price6','Buy_Volume6','Buy_Price7','Buy_Volume7','Buy_Price8','Buy_Volume8','Buy_Price9','Buy_Volume9','Buy_Price10','Buy_Volume10','Sell_Price4','Sell_Volume4','Sell_Price5','Sell_Volume5','Sell_Price6','Sell_Volume6','Sell_Price7','Sell_Volume7','Sell_Price8','Sell_Volume8','Sell_Price9','Sell_Volume9','Sell_Price10','Sell_Volume10','Close_Price','Match_Volume','Match_Value','Diff','Diff_Rate','Open_Price','High_Price','Low_Price','Average_Price','Total_Volume','Total_Value','Match_Price_Pt','Match_Volume_Pt','Total_Pt_Volume','Total_Pt_Value','F_Buy_Volume','F_Buy_Volume_Last','F_Sell_Volume','F_Sell_Volume_Last','F_Buy_Value','F_Sell_Value','F_Room','F_Room_Pct','Total_Room','Buy_Order','Sell_Order','Total_Buy_Volume','Total_Sell_Volume','Remain_Buy','Remain_Sell','Open_Interest','Last_Trade_Date','Vn30_Basis','Stock_Basis','Balance_Price','INav','IIndex'])
    return df
engine = create_engine('postgresql://eros:erosnguyen123@192.168.110.17:9998/realtime_data')
engine_sqlserver = "mssql://dbfin:finpros2022@192.168.110.194%5CSQLEXPRESS:1433/stockdata"
f = open("log.txt", "a")
while True:
    try:
        df2=pd.read_csv('realtime_ord.csv',dtype={'INav':'str'},low_memory=False)
        if df2.empty == True:
            df2=pd.read_csv('realtime_ord2.csv',dtype={'INav':'str'},low_memory=False)
        query = f'''select top 1 [OPEN] from VN_Intraday where SYMBOL ='VN30' order by [DATE] desc '''
        price_df=cx.read_sql(engine_sqlserver, query)
        df2['Vn30_Basis'] = df2['Close_Price']-price_df.iloc[0].values[0]
        df2['Time'] = pd.to_datetime(df2['Time']) 
        df2.to_sql('realtime_ord_rs', engine,if_exists='append',index=False)
        create_file()
        time.sleep(5)
    except:
        now=datetime.now()
        now_str=now.strftime("%m/%d/%Y, %H:%M:%S")
        create_file()
        time.sleep(5)
        f.write(f'Catch except and delete file at {now_str}')
        f.write('\n')
    gc.collect()