import pandas as pd
import redis
import time
import ast
from queue import Queue,Empty
from queue import LifoQueue
from threading import Thread
from sqlalchemy import create_engine
import sys
from datetime import datetime, timedelta
import connectorx as cx

from check_time import check_time_now
import gc
import pandas as pd
import sqlalchemy
from datetime import datetime, time
import numpy as np
from tqdm import tqdm
from sklearn import linear_model
from sklearn.linear_model import LinearRegression,Ridge

def tf_pbuy_psell(df, window_min=10):
    df1 = df[['Security_Code','Time','Buy_Price1','Buy_Price2','Buy_Price3','Buy_Price4','Buy_Price5','Sell_Price1','Sell_Price2','Sell_Price3','Sell_Price4','Sell_Price5']].set_index(['Security_Code','Time']).copy()
    df1['sx'] = range(len(df1))
    df1 = df1.loc[~(df1==0).all(axis=1)].replace(0,np.nan)
    df1['max'] = df1[['Sell_Price1','Sell_Price2','Sell_Price3','Sell_Price4','Sell_Price5']].max(axis=1)
    df1['min'] = df1[['Buy_Price1','Buy_Price2','Buy_Price3','Buy_Price4','Buy_Price5']].min(axis=1)
    df1 = df1.reset_index()
    df1['min'] = df1['min'].rolling(window_min).min()
    df1['max'] = df1['max'].rolling(window_min).max()
    df1['spread'] = df1['max']-df1['min']
    return df1[['Time','spread','sx']]
def z_t(df):
    """初探市场微观结构：指令单薄与指令单流——资金交易策略之四 成交价的对数减去中间价的对数"""
    df1 = df[['Time','Buy_Price1','Sell_Price1','Close_Price']]
    df1['sx'] = range(len(df1))
def tf_pbuy_psell(df, window_min=10):
    df1 = df[['Security_Code','Time','Buy_Price1','Buy_Price2','Buy_Price3','Buy_Price4','Buy_Price5','Sell_Price1','Sell_Price2','Sell_Price3','Sell_Price4','Sell_Price5']].set_index(['Security_Code','Time']).copy()
    df1['sx'] = range(len(df1))
    df1 = df1.loc[~(df1==0).all(axis=1)].replace(0,np.nan)
    df1['max'] = df1[['Sell_Price1','Sell_Price2','Sell_Price3','Sell_Price4','Sell_Price5']].max(axis=1)
    df1['min'] = df1[['Buy_Price1','Buy_Price2','Buy_Price3','Buy_Price4','Buy_Price5']].min(axis=1)
    df1 = df1.reset_index()
    df1['min'] = df1['min'].rolling(window_min).min()
    df1['max'] = df1['max'].rolling(window_min).max()
    df1['spread'] = df1['max']-df1['min']
    return df1[['Time','spread','sx']]
def z_t(df):
    """初探市场微观结构：指令单薄与指令单流——资金交易策略之四 成交价的对数减去中间价的对数"""
    df1 = df[['Time','Buy_Price1','Sell_Price1','Close_Price']]
    df1['sx'] = range(len(df1))
    df1 = df1.loc[~(df1==0).all(axis=1)].replace(0,np.nan)
    df1['tick_fac_data'] = np.log(df1['Close_Price']) - np.log((df1['Buy_Price1'] + df1['Sell_Price1']) / 2)
    return df1[['Time','tick_fac_data','sx']]
def cal_weight_volume(df):
        """计算加权的盘口挂单量"""
        data_dic = df.copy()
        data_dic['sx'] = range(len(data_dic))
        w = [1 - (i - 1) / 5 for i in range(1, 6)]
        w = np.array(w) / sum(w)
        data_dic['wb'] = data_dic['Buy_Volume1'] * w[0] + data_dic['Buy_Volume2'] * w[1] + data_dic['Buy_Volume3'] * w[2] + data_dic[
            'Buy_Volume4'] * w[3] + data_dic['Buy_Volume5'] * w[4]
        data_dic['wa'] = data_dic['Sell_Volume1'] * w[0] + data_dic['Sell_Volume2'] * w[1] + data_dic['Sell_Volume3'] * w[2] + data_dic[
            'Sell_Volume4'] * w[3] + data_dic['Sell_Volume5'] * w[4]
        return data_dic[['Time','wb','wa','sx']]

def hft_model(df,n):
    hf1 = tf_pbuy_psell(df, window_min=10)
    hf2 = z_t(df)
    hf3 = cal_weight_volume(df)
    train = hf1.merge(hf2[['sx','tick_fac_data']],on=['sx'],how='left').merge(hf3[['wb','wa','sx']],on=['sx'],how='left')
    train = train.loc[train['Time'].dt.time>=time(9,30)].sort_values('Time')
    train.index = range(len(train))
    fac_lis = ['spread','sx','tick_fac_data','wb','wa']
    train = train[['Time']+fac_lis].dropna()
    train = train.groupby('Time').mean().reset_index()
    close = df[['Time','Close_Price']].groupby('Time').tail(1).sort_values('Time')
    close.index = range(len(close))
    train = train.merge(close,on=['Time'],how='left')
    train['day'] = train['Time'].dt.date
    pct = train.sort_values('Time').set_index('Time').groupby('day').apply(lambda x:x['Close_Price'].pct_change()).to_frame().reset_index()[['Time','Close_Price']].rename(columns={"Close_Price":"pct"})
    pct_shift = n
    pcts = train.sort_values('Time').set_index('Time').groupby('day').apply(lambda x:x['Close_Price'].pct_change().shift(-pct_shift)).to_frame().reset_index()[['Time','Close_Price']].rename(columns={"Close_Price":"pct"+str(pct_shift)}).copy()
    train = train.merge(pct,on=['Time'],how='left').merge(pcts,on=['Time'],how='left')#.drop('day',axis=1)
    train_x = train.loc[train['day']<=pd.Timestamp('2022-12-20')].dropna().set_index('Time')[fac_lis].copy()
    train_y = train.loc[train['day']<=pd.Timestamp('2022-12-20')].dropna().set_index('Time')["pct"+str(pct_shift)].copy()
    reg = Ridge(alpha=0.01)
    model = reg.fit(train_x,train_y)
    return model

def predict_buy_sell(df_history):
    n = 60
    model = hft_model(df_history,n)

    df_buy_sell = pd.read_csv('buy_sell.csv')
    fac_lis = ['spread','sx','tick_fac_data','wb','wa']
    hf1 = tf_pbuy_psell(df_buy_sell, window_min=10)
    hf2 = z_t(df_buy_sell)
    hf3 = cal_weight_volume(df_buy_sell)
    predict = hf1.merge(hf2[['sx','tick_fac_data']],on=['sx'],how='left').merge(hf3[['wb','wa','sx']],on=['sx'],how='left')
    pre = model.predict(predict[fac_lis])[-1]
    if pre>0:
        pre = 'buy'
    elif pre<0:
        pre = 'sell'
    return pre

















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
time_df=''
current_df = pd.DataFrame()
price = 0
def check_buy_sell():
    df_history=pd.read_csv('full_data.csv')
    while True:
        check_time = check_time_now(datetime.now().time()) 
        if check_time:
            try:
                # global price
                # global time_df
                # df_cs = pd.read_csv('buy_sell.csv')
                # max_open_interest = df_cs['Open_Interest'].max()
                # df =df_cs[df_cs['Open_Interest'] == max_open_interest]
                # df = df.copy()
                # df["buy_sell"] = "neutral"
                # df["total_bid"] = df["Buy_Price1"] * df["Buy_Volume1"] + df["Buy_Price2"] * df["Buy_Volume2"] + df["Buy_Price3"] * df["Buy_Volume3"]
                # df["total_ask"] = df["Sell_Price1"] * df["Sell_Volume1"] + df["Sell_Price2"] * df["Sell_Volume2"] + df["Sell_Price3"] * df["Sell_Volume3"]
                # calculate the buy/sell status for each row
                pre=predict_buy_sell(df_history)
                if pre=='buy':
                    global total_buy 
                    total_buy +=1
                    # df.at[i, "buy_sell"] = "buy"
                    # price=df.iloc[-1]['Sell_Price1']
                    # time_df=str(df.iloc[-1]['Time'])
                    print('buy')
                elif pre=='sell':
                    global total_sell 
                    total_sell +=1
                    # df.at[i, "buy_sell"] = "sell"
                    # price=df.iloc[-1]['Buy_Price1']
                    # time_df=str(df.iloc[-1]['Time'])
                    print('sell')
            except:
                print('error')
        
def sell_buy_1m():
    start_time = datetime.now()
    while True:
        check_time = check_time_now(datetime.now().time()) 
        if check_time:
            df=pd.DataFrame()
            # f = open('check_pnl.txt',mode='a',encoding='utf-8')
            global total_buy 
            global total_sell
            global time_df
            # now = time_df
            try:
                if (datetime.now() - start_time) >= timedelta(seconds=30):
                    if total_sell > total_buy:
                        # f.write(f'predict sell at {now} {price}')
                        # f.write('\n')
                        # df.at[1,'Time'] = now
                        # df.at[1,'Price'] = price
                        df.at[1,'Status'] = 'sell'
                    elif total_sell < total_buy:
                        # f.write(f'predict buy at {now} {price}')
                        # f.write('\n')
                        # df.at[1,'Time'] = now
                        # df.at[1,'Price'] = price
                        df.at[1,'Status'] = 'buy'
                    df.to_csv('signal.csv',index=False,mode='a',header=False)
                    total_buy , total_sell= 0 , 0
            except:
                print('error')
        
def run():
    Thread(target=check_buy_sell).start()
    # buy_sell_signal.start()
    Thread(target=sell_buy_1m).start()
    # Thread(target=check_open_order).start()
if __name__ == "__main__":
    run()