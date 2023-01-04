import redis
import time
import pandas as pd
import ast
from queue import Queue,Empty
from queue import LifoQueue
from threading import Thread
import gc
HOST = '192.168.110.15'
PORT = '6379'
CHANNEL = 'test'
stack=LifoQueue()
def create_file():
    with open('realtime_ord3.csv','a',encoding='utf-8') as f:
        f.write('Type,Security_Code,Exchange_Code,Security_Id,Security_Type,Listed_Shares,Time,Trade_Date,Ceiling_Price,Floor_Price,Ref_Price,Buy_Price1,Buy_Volume1,Buy_Price2,Buy_Volume2,Buy_Price3,Buy_Volume3,Sell_Price1,Sell_Volume1,Sell_Price2,Sell_Volume2,Sell_Price3,Sell_Volume3,Buy_Price4,Buy_Volume4,Buy_Price5,Buy_Volume5,Buy_Price6,Buy_Volume6,Buy_Price7,Buy_Volume7,Buy_Price8,Buy_Volume8,Buy_Price9,Buy_Volume9,Buy_Price10,Buy_Volume10,Sell_Price4,Sell_Volume4,Sell_Price5,Sell_Volume5,Sell_Price6,Sell_Volume6,Sell_Price7,Sell_Volume7,Sell_Price8,Sell_Volume8,Sell_Price9,Sell_Volume9,Sell_Price10,Sell_Volume10,Close_Price,Match_Volume,Match_Value,Diff,Diff_Rate,Open_Price,High_Price,Low_Price,Average_Price,Total_Volume,Total_Value,Match_Price_Pt,Match_Volume_Pt,Total_Pt_Volume,Total_Pt_Value,F_Buy_Volume,F_Buy_Volume_Last,F_Sell_Volume,F_Sell_Volume_Last,F_Buy_Value,F_Sell_Value,F_Room,F_Room_Pct,Total_Room,Buy_Order,Sell_Order,Total_Buy_Volume,Total_Sell_Volume,Remain_Buy,Remain_Sell,Open_Interest,Last_Trade_Date,Vn30_Basis,Stock_Basis,Balance_Price,INav,IIndex')
def listen_redis(st):
    create_file()
    r=redis.Redis(
        host=HOST,
        password='erosnguyen123'
    )
    pub = r.pubsub()
    pub.subscribe(CHANNEL)

    while True:
        data = pub.get_message()
        if data:
            message = data['data']
            if message and message != 1:
                df=pd.DataFrame([ast.literal_eval(message.decode('utf-8'))])
                st.put(df)
                
def trans_load(st):
    while True:
            df=st.get()
            st.task_done()
            try:
                df['Time'] = pd.to_datetime(df['Time'])
                df.to_csv('realtime_ord3.csv',index=False,header=False,mode='a')
            except:
                df.to_csv('no_time.csv',index=False,header=False,mode='a')
            
            
def run():
    listen_redis_thread=Thread(target=listen_redis,args={stack,})
    listen_redis_thread.start()
    Thread(target=trans_load,args={stack,}).start()
    listen_redis_thread.join()
    stack.join()
if __name__ == '__main__':
    run()
