import sqlalchemy
import pandas as pd
from datetime import date,timedelta
import time
import connectorx as cx
import datetime
from datetime import datetime
import gc
# engine = sqlalchemy.create_engine('postgresql://eros:erosnguyen123@192.168.110.17:9998/db_test',pool_size=5)
# engine_sqlserver=sqlalchemy.create_engine('mssql://dbfin:finpros2022@192.168.110.194%5CSQLEXPRESS:1433/stockdata?driver=SQL+Server+Native+Client+11.0')
 
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
        df.set_index('Time',inplace=True)
        return df
    def realtime_ps(self):
        today = date.today().strftime('%Y_%m_%d')
        query=f'''select * from  realtime_{today}
                    where "Open_Interest"=(select max("Open_Interest") from realtime_{today}) 
                    order by "Time" desc '''
        df=pd.read_sql_query(query,self.db_realtime)
        df.set_index('Time',inplace=True)
        df2 = df.resample('1Min',closed='right').last()
        return df2      
    def query_his_real(self):
        df1=self.history_realtime()
        df2=self.realtime_ps()
        df3=pd.concat([df1,df2])
        return df3
# engine_sqlserver= "mssql://dbfin:finpros2022@192.168.110.194%5CSQLEXPRESS:1433/stockdata"
# Qr=Query_realtime(from_date='2022-11-21') # Thay doi from_date de lay gia lich su
# now=datetime.datetime.now()
# date_time = now.strftime("%Y/%m/%d %H:%M:%S.%f")


Qr=Query_realtime('2022-12-29')
now = datetime.now()    
df = Qr.realtime_ps()

# df.to_sql('test_tsv', engine,if_exists='append',index=False)
print(df[['Security_Code','Vn30_Basis','Close_Price','Open_Interest']])
# print(df)
# print(datetime.now() - now)