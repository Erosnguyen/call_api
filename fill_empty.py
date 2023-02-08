import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import sqlalchemy
import psycopg2
def get_vn30f():
    def vn30f():
            return requests.get("https://services.entrade.com.vn/chart-api/chart?from=0&resolution=1&symbol=VN30F1M&to=9999999999").json()
    vn30fm = pd.DataFrame(vn30f()).iloc[:,:6]
    vn30fm['t'] = vn30fm['t'].astype(int).apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours = 7))
    vn30fm.columns = ['Date','Open','High','Low','Close','Volume']
    ohlc_dict = {                                                                                                             
        'Open': 'first',                                                                                                    
        'High': 'max',                                                                                                       
        'Low': 'min',                                                         
        'Close': 'last',                                                                                                    
        'Volume': 'sum',}
    vn30fm = pd.DataFrame(vn30f()).iloc[:,:6]
    vn30fm['t'] = vn30fm['t'].astype(int).apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours = 7))
    vn30fm.columns = ['Date','Open','High','Low','Close','Volume']
    return vn30fm.set_index('Date')
def get_vn30():
    def vn30():
            return requests.get("https://services.entrade.com.vn/chart-api/v2/ohlcs/index?from=0&resolution=1&symbol=VN30&to=9999999999").json()
    vn30fm = pd.DataFrame(vn30()).iloc[:,:6]
    vn30fm['t'] = vn30fm['t'].astype(int).apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours = 7))
    vn30fm.columns = ['Date','Open','High','Low','Close','Volume']
    ohlc_dict = {                                                                                                             
        'Open': 'first',                                                                                                    
        'High': 'max',                                                                                                       
        'Low': 'min',                                                         
        'Close': 'last',                                                                                                    
        'Volume': 'sum',}
    vn30fm = pd.DataFrame(vn30()).iloc[:,:6]
    vn30fm['t'] = vn30fm['t'].astype(int).apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours = 7))
    vn30fm.columns = ['Date','Open','High','Low','Close','Volume']
    return vn30fm.set_index('Date')
ohlc_dict = {                                                                                                                                                                                                                 
    'basis': 'mean',                                                                                                        
    'Close': 'last',                                                                                                    
    'Volume': 'sum',}
data2 = pd.DataFrame()
data2['Close'] = get_vn30f()['Close']
data2['Volume'] = get_vn30f()['Volume']
data2['Close_VN30'] = get_vn30()['Close']
data2 = data2.ffill()
data2['basis'] = data2['Close_VN30'] - data2['Close']
dataset1 = data2.drop_duplicates().dropna()
dataset2 = dataset1.reset_index()
dataset2['Date'] = pd.to_datetime(dataset2['Date'])
dataset2 = dataset2[['Date','Close', 'Volume', 'basis']]
dataset = dataset2.resample('1Min', on='Date', label='left').apply(ohlc_dict).dropna()
dataset['High'] = dataset2.resample('1Min', on='Date', label='left')['Close'].max().dropna()
dataset['Low'] = dataset2.resample('1Min', on='Date', label='left')['Close'].min().dropna()
dataset =dataset.reset_index()
dataset2['time'] = [str(i)[11:16] for i in dataset2['Date']]
dataset2['Date'] = [str(i)[:10] for i in dataset2['Date']]

df = dataset2

df['Date'] = pd.to_datetime(df['Date'] + ' ' + df['time'])
df=df.drop('time',axis=1)
mask = (pd.to_datetime(df['Date']) > pd.Timestamp('2022-11-21 09:00:00')) 
df = df[mask]


df3=df.rename(columns={"Date": "Time", "Close": "Close_Price","Volume":"Total_Volume","basis":"Vn30_Basis"})
df=df3
df['Trade_Date'] = [str(i)[:10] for i in df['Time']]

df['Total_Volume'] = df.groupby('Trade_Date').cumsum()['Total_Volume']

def create_pd():
    df = pd.DataFrame(columns=['Type','Security_Code','Exchange_Code','Security_Id','Security_Type','Listed_Shares','Time','Trade_Date','Ceiling_Price','Floor_Price','Ref_Price','Buy_Price1','Buy_Volume1','Buy_Price2','Buy_Volume2','Buy_Price3','Buy_Volume3','Sell_Price1','Sell_Volume1','Sell_Price2','Sell_Volume2','Sell_Price3','Sell_Volume3','Buy_Price4','Buy_Volume4','Buy_Price5','Buy_Volume5','Buy_Price6','Buy_Volume6','Buy_Price7','Buy_Volume7','Buy_Price8','Buy_Volume8','Buy_Price9','Buy_Volume9','Buy_Price10','Buy_Volume10','Sell_Price4','Sell_Volume4','Sell_Price5','Sell_Volume5','Sell_Price6','Sell_Volume6','Sell_Price7','Sell_Volume7','Sell_Price8','Sell_Volume8','Sell_Price9','Sell_Volume9','Sell_Price10','Sell_Volume10','Close_Price','Match_Volume','Match_Value','Diff','Diff_Rate','Open_Price','High_Price','Low_Price','Average_Price','Total_Volume','Total_Value','Match_Price_Pt','Match_Volume_Pt','Total_Pt_Volume','Total_Pt_Value','F_Buy_Volume','F_Buy_Volume_Last','F_Sell_Volume','F_Sell_Volume_Last','F_Buy_Value','F_Sell_Value','F_Room','F_Room_Pct','Total_Room','Buy_Order','Sell_Order','Total_Buy_Volume','Total_Sell_Volume','Remain_Buy','Remain_Sell','Open_Interest','Last_Trade_Date','Vn30_Basis','Stock_Basis','Balance_Price','INav','IIndex'])
    return df
df_m=create_pd()

df['Time'] = pd.to_datetime(df['Time'])
df = pd.concat([df_m,df])
df=df.fillna(0)
df['Security_Code']='VN30F1M'



# delete null
conn = psycopg2.connect(
   database="realtime_ps", user='eros', password='erosnguyen123', host='192.168.110.17', port= '9998'
)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute('''delete from realtime_ord_1m where "Security_Code" is null''')
conn.commit()
conn.close()

#import empty
query=f'''select * from realtime_ord_1m order by "Time"'''  
db_realtime=sqlalchemy.create_engine('postgresql://client1:finpros2022@192.168.110.17:9998/realtime_ps')
df2 = pd.read_sql_query(query,db_realtime)

df_same=df.merge(df2,how='inner',on="Time")
df=df[~df.Time.isin(df_same.Time)]
df['Time'] = pd.to_datetime(df['Time'])


df.to_sql('realtime_ord_1m', db_realtime,if_exists='append',index=False)
df