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

def check_df(value):
    check_text = f"{value}"
    df = pd.read_csv('realtime_ord.csv')
    #check for text
    mask  = df['Security_Code'].str.match(check_text)
    if mask.any():
        return df.loc[mask]
def VN_30F1M(callback):
    callback['Security_Code'].iloc[0][-2:]
    callback['time_2'] = [float(str(i)[-4:-2])*12 + float(str(i)[-2:]) for i in callback['Security_Code']]
    df1 = callback.groupby('Time').min()
    df2= df1.drop('time_2',axis=1)
    return df2 

def check_buy_sell(df):
    bid_ask_weight = 0.6
    match_volume_weight = 0.3
    total_volume_weight = 0.1
    df["buy_sell"] = "neutral"
    df["total_bid"] = df["Buy_Price1"] * df["Buy_Volume1"] + df["Buy_Price2"] * df["Buy_Volume2"] + df["Buy_Price3"] * df["Buy_Volume3"]
    df["total_ask"] = df["Sell_Price1"] * df["Sell_Volume1"] + df["Sell_Price2"] * df["Sell_Volume2"] + df["Sell_Price3"] * df["Sell_Volume3"]
    # calculate the buy/sell status for each row
    for i, row in df.iterrows():
        if row["total_bid"] > row["total_ask"]:
            df.at[i, "buy_sell"] = "buy"
        elif row["total_bid"] < row["total_ask"]:
            df.at[i, "buy_sell"] = "sell"
            
        
        # weighted_sum = (row["total_bid"] - row["total_ask"]) * bid_ask_weight + \
        #            row["Match_Volume"] * match_volume_weight + \
        #            row["Total_Volume"] * total_volume_weight
        # if weighted_sum > 0:
        #     df.at[i, "buy_sell"] = "buy"
        # elif weighted_sum < 0:
        #     df.at[i, "buy_sell"] = "sell"
        return df
check = check_df('VN30F')
df = VN_30F1M(check)
df2=check_buy_sell(df)
print(df[['Security_Code','Close_Price','buy_sell','total_bid','total_ask']])