import pandas as pd
import redis
import time
import ast
from queue import Queue,Empty
from queue import LifoQueue
from threading import Thread
from sqlalchemy import create_engine
import sys
import connectorx as cx
from decimal import Decimal
from datetime import datetime
from check_time import check_time_now
import gc
from check_time_csv import time_range
from queue import LifoQueue
def create_file():
    with open('buy_sell.csv','w',encoding='utf-8') as f:
        f.write('Type,Security_Code,Exchange_Code,Security_Id,Security_Type,Listed_Shares,Time,Trade_Date,Ceiling_Price,Floor_Price,Ref_Price,Buy_Price1,Buy_Volume1,Buy_Price2,Buy_Volume2,Buy_Price3,Buy_Volume3,Sell_Price1,Sell_Volume1,Sell_Price2,Sell_Volume2,Sell_Price3,Sell_Volume3,Buy_Price4,Buy_Volume4,Buy_Price5,Buy_Volume5,Buy_Price6,Buy_Volume6,Buy_Price7,Buy_Volume7,Buy_Price8,Buy_Volume8,Buy_Price9,Buy_Volume9,Buy_Price10,Buy_Volume10,Sell_Price4,Sell_Volume4,Sell_Price5,Sell_Volume5,Sell_Price6,Sell_Volume6,Sell_Price7,Sell_Volume7,Sell_Price8,Sell_Volume8,Sell_Price9,Sell_Volume9,Sell_Price10,Sell_Volume10,Close_Price,Match_Volume,Match_Value,Diff,Diff_Rate,Open_Price,High_Price,Low_Price,Average_Price,Total_Volume,Total_Value,Match_Price_Pt,Match_Volume_Pt,Total_Pt_Volume,Total_Pt_Value,F_Buy_Volume,F_Buy_Volume_Last,F_Sell_Volume,F_Sell_Volume_Last,F_Buy_Value,F_Sell_Value,F_Room,F_Room_Pct,Total_Room,Buy_Order,Sell_Order,Total_Buy_Volume,Total_Sell_Volume,Remain_Buy,Remain_Sell,Open_Interest,Last_Trade_Date,Vn30_Basis,Stock_Basis,Balance_Price,INav,IIndex')
        f.write('\n')
def check_df(value):
    check_text = f"{value}"
    df = pd.read_csv('buy_sell.csv')
    #check for text
    mask  = df['Security_Code'].str.match(check_text)
    if mask.any():
        return df.loc[mask]
def VN_30F1M(callback):
    callback['Security_Code'].iloc[0][-2:]
    callback['time_2'] = [float(str(i)[-4:-2])*12 + float(str(i)[-2:]) for i in callback['Security_Code']]
    df1 = callback.groupby('Time').min()
    df2 = df1.drop('time_2',axis=1)
    return df2 
total_sell = 0
total_buy = 0
pnl = 0
current_df = pd.DataFrame()
# price = 0
def check_buy_sell():
    while True:
        try:
            global price
            df_cs = pd.read_csv('buy_sell.csv')
            max_open_interest = df_cs['Open_Interest'].max()
            df =df_cs[df_cs['Open_Interest'] == max_open_interest]
            df = df.copy()
            df["buy_sell"] = "neutral"
            df["total_bid"] = df["Buy_Price1"] * df["Buy_Volume1"] + df["Buy_Price2"] * df["Buy_Volume2"] + df["Buy_Price3"] * df["Buy_Volume3"]
            df["total_ask"] = df["Sell_Price1"] * df["Sell_Volume1"] + df["Sell_Price2"] * df["Sell_Volume2"] + df["Sell_Price3"] * df["Sell_Volume3"]
            # calculate the buy/sell status for each row
            for i, row in df.iterrows():
                if row["total_bid"] > row["total_ask"]:
                    global total_buy 
                    total_buy +=1
                    df.at[i, "buy_sell"] = "buy"
                    price=df.iloc[-1]['Sell_Price1']
                    print('buy')
                elif row["total_bid"] < row["total_ask"]:
                    global total_sell 
                    total_sell +=1
                    df.at[i, "buy_sell"] = "sell"
                    price=df.iloc[-1]['Buy_Price1']
                    print('sell')
                create_file() 
            time.sleep(1)
        except:
            create_file()
            continue
def sell_buy_1m():
    while True:
        f = open('check_pnl.txt',mode='a',encoding='utf-8')
        global total_buy 
        global total_sell
        now = datetime.now()
        try:
            if total_sell + total_buy ==25:
                if total_sell > total_buy:
                    f.write(f'predict sell at {now} {price}')
                    f.write('\n')
                    total_buy , total_sell= 0 , 0
                elif total_sell < total_buy:
                    f.write(f'predict buy at {now} {price}')
                    f.write('\n')
                    total_buy , total_sell= 0 , 0
                else:
                    print(f'\nneutral at {now} {price}')
                    total_buy , total_sell= 0 , 0
        except:
            pass
def run():
    Thread(target=check_buy_sell).start()
    # buy_sell_signal.start()
    Thread(target=sell_buy_1m).start()
    # Thread(target=check_open_order).start()
if __name__ == "__main__":
    run()