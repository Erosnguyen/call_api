import sys
import asyncio
from pyppeteer import launch
import time
import pprint
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import redis
import datetime
from datetime import datetime,date
import connectorx as cx
engine = create_engine('postgresql://eros:erosnguyen123@192.168.110.17:9998/realtime_data')
engine_sqlserver = "mssql://dbfin:finpros2022@192.168.110.194%5CSQLEXPRESS:1433/stockdata"
f = open("log.txt", "a")
def create_file():
    with open('realtime_ord.csv','w',encoding='utf-8') as f:
        f.write('Type,Security_Code,Exchange_Code,Security_Id,Security_Type,Listed_Shares,Time,Trade_Date,Ceiling_Price,Floor_Price,Ref_Price,Buy_Price1,Buy_Volume1,Buy_Price2,Buy_Volume2,Buy_Price3,Buy_Volume3,Sell_Price1,Sell_Volume1,Sell_Price2,Sell_Volume2,Sell_Price3,Sell_Volume3,Buy_Price4,Buy_Volume4,Buy_Price5,Buy_Volume5,Buy_Price6,Buy_Volume6,Buy_Price7,Buy_Volume7,Buy_Price8,Buy_Volume8,Buy_Price9,Buy_Volume9,Buy_Price10,Buy_Volume10,Sell_Price4,Sell_Volume4,Sell_Price5,Sell_Volume5,Sell_Price6,Sell_Volume6,Sell_Price7,Sell_Volume7,Sell_Price8,Sell_Volume8,Sell_Price9,Sell_Volume9,Sell_Price10,Sell_Volume10,Close_Price,Match_Volume,Match_Value,Diff,Diff_Rate,Open_Price,High_Price,Low_Price,Average_Price,Total_Volume,Total_Value,Match_Price_Pt,Match_Volume_Pt,Total_Pt_Volume,Total_Pt_Value,F_Buy_Volume,F_Buy_Volume_Last,F_Sell_Volume,F_Sell_Volume_Last,F_Buy_Value,F_Sell_Value,F_Room,F_Room_Pct,Total_Room,Buy_Order,Sell_Order,Total_Buy_Volume,Total_Sell_Volume,Remain_Buy,Remain_Sell,Open_Interest,Last_Trade_Date,Vn30_Basis,Stock_Basis,Balance_Price,INav,IIndex')
        f.write('\n')
def create_pd():
    df = pd.DataFrame(columns=['Type','Security_Code','Exchange_Code','Security_Id','Security_Type','Listed_Shares','Time','Trade_Date','Ceiling_Price','Floor_Price','Ref_Price','Buy_Price1','Buy_Volume1','Buy_Price2','Buy_Volume2','Buy_Price3','Buy_Volume3','Sell_Price1','Sell_Volume1','Sell_Price2','Sell_Volume2','Sell_Price3','Sell_Volume3','Buy_Price4','Buy_Volume4','Buy_Price5','Buy_Volume5','Buy_Price6','Buy_Volume6','Buy_Price7','Buy_Volume7','Buy_Price8','Buy_Volume8','Buy_Price9','Buy_Volume9','Buy_Price10','Buy_Volume10','Sell_Price4','Sell_Volume4','Sell_Price5','Sell_Volume5','Sell_Price6','Sell_Volume6','Sell_Price7','Sell_Volume7','Sell_Price8','Sell_Volume8','Sell_Price9','Sell_Volume9','Sell_Price10','Sell_Volume10','Close_Price','Match_Volume','Match_Value','Diff','Diff_Rate','Open_Price','High_Price','Low_Price','Average_Price','Total_Volume','Total_Value','Match_Price_Pt','Match_Volume_Pt','Total_Pt_Volume','Total_Pt_Value','F_Buy_Volume','F_Buy_Volume_Last','F_Sell_Volume','F_Sell_Volume_Last','F_Buy_Value','F_Sell_Value','F_Room','F_Room_Pct','Total_Room','Buy_Order','Sell_Order','Total_Buy_Volume','Total_Sell_Volume','Remain_Buy','Remain_Sell','Open_Interest','Last_Trade_Date','Vn30_Basis','Stock_Basis','Balance_Price','INav,IIndex'])
    return df
listColumesName = ['Exchange_Code', 'Time', 'Security_Code', 'Security_Id', 'Trade_Date', 'Security_Type', 'Ceiling_Price', 'Floor_Price', 'Ref_Price',
                   'Buy_Price3', 'Buy_Volume3', 'Buy_Price2', 'Buy_Volume2', 'Buy_Price1', 'Buy_Volume1',
                   'Close_Price', 'Match_Volume', 'Diff',
                   'Sell_Price1', 'Sell_Volume1', 'Sell_Price2', 'Sell_Volume2', 'Sell_Price3', 'Sell_Volume3',
                   'Total_Volume', 'Total_Value', 'Average_Price', 'Open_Price', 'High_Price', 'Low_Price', 'F_Buy_Volume', 'F_Sell_Volume', 'F_Room', 'Total_Room', 'Open_Interest']
def trans_mess_bsc(response, ex):
    strJson = json.dumps(response['response']['payloadData'], ensure_ascii=False)
    strData = strJson.replace('"{', '{')
    strData = strData.replace('}"', '}')
    strData = strData.replace('\"ST\":\"2\"', '\"ST\":\"ST\"')
    strData = strData.replace('\"ST\":\"3\"', '\"ST\":\"FU\"')
    strData = strData.replace('\"ST\":\"4\"', '\"ST\":\"CW\"')
    strData = strData.replace('ATC', '0')  # xem để null hoạc 0
    strData = strData.replace('ATO', '0')  # xem để null hoạc 0
    strData = strData.replace(r'\{', '{')
    strData = strData.replace(r'\"', '"')
    strData = strData.replace(r'\\"', '"')
    strData = strData.replace(r'\\\"', '"')
    strData = strData.replace('}"', '}')
    strData = strData.replace('"{', '{')
    strData = strData.replace('+', '')
    jsonData = json.loads(strData)
    if 'M' in jsonData and len(jsonData['M']) > 0:
        # print(len(jsonData['M']))
        if jsonData['M'][0]['M'] == 'updateStockPrice' and 'A' in jsonData['M'][0]:
            if len(jsonData['M'][0]['A']) > 0 and 'pb' in jsonData['M'][0]['A'][0]:
                if 'f' in jsonData['M'][0]['A'][0]['pb'] and len(jsonData['M'][0]['A'][0]['pb']['f']) > 0:
                    # print('f')
                    df = pd.DataFrame(jsonData['M'][0]['A'][0]['pb']['f'])
                    df['Exchange_Code'] = ex
                    df['Trade_Date'] = str(date.today())
                    if 'TD' in df.columns:
                         df = df.drop(columns=['TD'])
                    if 'f' in jsonData['M'][0]['A'][0]['st'] is not None:
                        dateTimeServer = jsonData['M'][0]['A'][0]['st']['f'].split()
                        if len(dateTimeServer) > 1:
                            df['Time'] = pd.to_datetime(str(date.today())+" " + dateTimeServer[1], errors='coerce')
                    df.rename(columns={'A': 'Security_Code',
                                       'SI': 'Security_Id',
                                       'ST': 'Security_Type',
                                       'CL': 'Ceiling_Price',
                                       'FL': 'Floor_Price',
                                       'RE': 'Ref_Price',
                                       'B': 'Buy_Price3',
                                       'C': 'Buy_Volume3',
                                       'D': 'Buy_Price2',
                                       'E': 'Buy_Volume2',
                                       'F': 'Buy_Price1',
                                       'G': 'Buy_Volume1',
                                       'H': 'Close_Price',
                                       'I': 'Match_Volume',
                                       'J': 'Diff',
                                       'K': 'Sell_Price1',
                                       'L': 'Sell_Volume1',
                                       'M': 'Sell_Price2',
                                       'N': 'Sell_Volume2',
                                       'O': 'Sell_Price3',
                                       'P': 'Sell_Volume3',
                                       'Q': 'Total_Volume',
                                       'R': 'Total_Value',
                                       'W': 'Average_Price',
                                       'X': 'Open_Price',
                                       'Y': 'High_Price',
                                       'Z': 'Low_Price',
                                       'S': 'F_Buy_Volume',
                                       'T': 'F_Sell_Volume',
                                       'V': 'F_Room',
                                       'U': 'Total_Room',
                                       'OI': 'Open_Interest',
                                       }, inplace=True)
                    for ci in df.columns:
                        if ci not in listColumesName:
                            df = df.drop(columns=[ci])
                    df = df.replace(np.nan, 0, regex=True)
                    df = df.replace('', 0, regex=True)
                    # Chuyển dạng string 1,222,222 thành số
                    df.replace(',', '', regex=True, inplace=True)

                    # print("3-->"+df['Buy_Volume3'].str.replace(r',',''))
                    if 'Ceiling_Price' in df.columns:
                        df['Ceiling_Price'] = df['Ceiling_Price'].replace(
                            '', np.nan).astype(float)
                    if 'Floor_Price' in df.columns:
                        df['Floor_Price'] = df['Floor_Price'].astype(float)
                    if 'Ref_Price' in df.columns:
                        df['Ref_Price'] = df['Ref_Price'].astype(float)
                    if 'Buy_Price3' in df.columns:
                        df['Buy_Price3'] = df['Buy_Price3'].astype(float)
                    if 'Buy_Volume3' in df.columns:
                        df['Buy_Volume3'] = df['Buy_Volume3'].astype('int64')
                    if 'Buy_Price2' in df.columns:
                        df['Buy_Price2'] = df['Buy_Price2'].astype(float)
                    if 'Buy_Volume2' in df.columns:
                        df['Buy_Volume2'] = df['Buy_Volume2'].astype('int64')
                    if 'Buy_Price1' in df.columns:
                        df['Buy_Price1'] = df['Buy_Price1'].astype(float)
                    if 'Buy_Volume1' in df.columns:
                        df['Buy_Volume1'] = df['Buy_Volume1'].astype('int64')
                    if 'Close_Price' in df.columns:
                        df['Close_Price'] = df['Close_Price'].astype(float)
                    if 'Match_Volume' in df.columns:
                        df['Match_Volume'] = df['Match_Volume'].astype('int64')
                    if 'Diff' in df.columns:
                        df['Diff'] = df['Diff'].astype(float)
                    if 'Sell_Price1' in df.columns:
                        df['Sell_Price1'] = df.Sell_Price1.astype(float)
                    if 'Sell_Volume1' in df.columns:
                        df['Sell_Volume1'] = df.Sell_Volume1.astype('int64')
                    if 'Sell_Price2' in df.columns:
                        df['Sell_Price2'] = df.Sell_Price2.astype(float)
                    if 'Sell_Volume2' in df.columns:
                        df['Sell_Volume2'] = df.Sell_Volume2.astype('int64')
                    if 'Sell_Price3' in df.columns:
                        df['Sell_Price3'] = df.Sell_Price3.astype(float)
                    if 'Sell_Volume3' in df.columns:
                        df['Sell_Volume3'] = df.Sell_Volume3.astype('int64')
                    if 'Total_Volume' in df.columns:
                        df['Total_Volume'] = df.Total_Volume.astype('int64')
                    if 'Total_Value' in df.columns:
                        df['Total_Value'] = df.Total_Value.astype('int64')
                    if 'Average_Price' in df.columns:
                        df['Average_Price'] = df.Average_Price.astype(float)
                    if 'Open_Price' in df.columns:
                        df['Open_Price'] = df.Open_Price.astype(float)
                    if 'High_Price' in df.columns:
                        df['High_Price'] = df.High_Price.astype(float)
                    if 'Low_Price' in df.columns:
                        df['Low_Price'] = df.Low_Price.astype(float)
                    if 'F_Buy_Volume' in df.columns:
                        df['F_Buy_Volume'] = df.F_Buy_Volume.astype('int64')
                    if 'F_Sell_Volume' in df.columns:
                        df['F_Sell_Volume'] = df.F_Sell_Volume.astype('int64')
                    if 'F_Room' in df.columns:
                        df['F_Room'] = df.F_Room.astype('int64')
                    if 'Total_Room' in df.columns:
                        df['Total_Room'] = df.Total_Room.astype('int64')
                    if 'Open_Interest' in df.columns:
                        df['Open_Interest'] = df['Open_Interest'].astype(float)

                    print(df)
                    df_m=create_pd()
                    df_m=pd.concat([df_m,df])
                    # df_m['Time']=df_m['Time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                try:                 
                    df_m.to_csv('realtime_ord.csv',index=False,header=False,mode='a')
                except:
                    pass
    else:
        print("Not data")

async def browserRun(company,url,ex):
    args = ['--start-maximized']
    browser = await launch(
        # args=args,
        headless=True,
        args=['--no-sandbox'],
        # autoClose=False
        )

    page = await browser.newPage()
    await page.goto(url)

    cdp = await page.target.createCDPSession()
    await cdp.send('Network.enable')


    def printResponse(response):
        if company=='bsc':
            trans_mess_bsc(response,ex)
        elif company=='vps':
            print('vps is call')
        else:
            print('not exist')

    cdp.on('Network.webSocketFrameReceived', printResponse) 

    await asyncio.sleep(28800)



if __name__ == "__main__":
    # nhận tham số Command Line
    company = str(sys.argv[1])
    url = str(sys.argv[2])
    ex = str(sys.argv[3])
    asyncio.get_event_loop().run_until_complete(browserRun(company,url,ex))
    now=datetime.now()
    now_str=now.strftime("%m/%d/%Y, %H:%M:%S")
    
    f.write(f'End at pub_realtime at {now_str}')
    f.write('\n')