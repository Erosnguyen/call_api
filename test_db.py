import sqlalchemy
import pandas as pd
from datetime import date,timedelta
import time
import connectorx as cx
import datetime
from datetime import datetime
import gc
class Query_realtime():
    def __init__(self,from_date='2022-10-05'):
        self.from_date=from_date
        self.to_date=from_date
        self.db_realtime=sqlalchemy.create_engine('postgresql://client1:finpros2022@192.168.110.15:6431/db_data',pool_pre_ping=True)
        self.db_history=sqlalchemy.create_engine('postgresql://client2:finpros2022@192.168.110.15:6431/db_ps',pool_pre_ping=True)
    def history_realtime(self):
        now=date.today()
        to_day=now-timedelta(days=1)
        query=f'''select * from  realtime_ord_1m
                where "Time" between cast('{self.from_date} 09:00:00' as timestamp) and cast('{to_day} 15:00:00' as timestamp) 
                order by "Time"'''
        df=pd.read_sql_query(query,self.db_history)
        # reference_time = '2022-01-10 09:00:00'
        # df.loc[(df['Total_Volume'] == 0) & (df['Time'] > reference_time), 'Total_Volume'] = None
        # df['Total_Volume'] = df['Total_Volume'].bfill()
        df.set_index('Time',inplace=True)
        return df
    def realtime_ps(self):
        today = date.today().strftime('%Y_%m_%d')
        today_query = date.today().strftime('%Y-%m-%d')
        start_time = pd.to_datetime(f'{today_query} 09:00:00')
        query=f'''select * from  realtime_{today}
                    where "Open_Interest"=(select max("Open_Interest") from realtime_{today}) 

                    order by "Time" desc '''
        df=pd.read_sql_query(query,self.db_realtime)
        df.set_index('Time',inplace=True)
        df2 = df.resample('1Min',closed='right').first()
        # df2['Close_Price'] = df2['Close_Price'].apply(lambda x: x if x >0 else None)
        # df2['Total_Volume'] = df2['Total_Volume'].apply(lambda x: x if x >0 else None)
        # if df2['Close_Price'].isnull().any():
        #     df2['Close_Price'].fillna(method='bfill', inplace=True)
        return df2  
    def query_his_real(self):
        df1=self.history_realtime()
        df2=self.realtime_ps()
        df3=pd.concat([df1,df2])
        return df3

Qr=Query_realtime('2023-01-10')
now = datetime.now()    
df = Qr.realtime_ps()
# df.to_csv('test20.csv')
print(df[['Security_Code','Vn30_Basis','Close_Price','Open_Interest','Total_Volume']])
