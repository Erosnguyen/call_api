import sys
import asyncio
from pyppeteer import launch
import time
from datetime import date, datetime
import pprint
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import redis
import zlib
import logging
def create_file():
    with open('realtime_ord.csv','w',encoding='utf-8') as f:
        f.write('Type,Security_Code,Exchange_Code,Security_Id,Security_Type,Listed_Shares,Time,Trade_Date,Ceiling_Price,Floor_Price,Ref_Price,Buy_Price1,Buy_Volume1,Buy_Price2,Buy_Volume2,Buy_Price3,Buy_Volume3,Sell_Price1,Sell_Volume1,Sell_Price2,Sell_Volume2,Sell_Price3,Sell_Volume3,Buy_Price4,Buy_Volume4,Buy_Price5,Buy_Volume5,Buy_Price6,Buy_Volume6,Buy_Price7,Buy_Volume7,Buy_Price8,Buy_Volume8,Buy_Price9,Buy_Volume9,Buy_Price10,Buy_Volume10,Sell_Price4,Sell_Volume4,Sell_Price5,Sell_Volume5,Sell_Price6,Sell_Volume6,Sell_Price7,Sell_Volume7,Sell_Price8,Sell_Volume8,Sell_Price9,Sell_Volume9,Sell_Price10,Sell_Volume10,Close_Price,Match_Volume,Match_Value,Diff,Diff_Rate,Open_Price,High_Price,Low_Price,Average_Price,Total_Volume,Total_Value,Match_Price_Pt,Match_Volume_Pt,Total_Pt_Volume,Total_Pt_Value,F_Buy_Volume,F_Buy_Volume_Last,F_Sell_Volume,F_Sell_Volume_Last,F_Buy_Value,F_Sell_Value,F_Room,F_Room_Pct,Total_Room,Buy_Order,Sell_Order,Total_Buy_Volume,Total_Sell_Volume,Remain_Buy,Remain_Sell,Open_Interest,Last_Trade_Date,Vn30_Basis,Stock_Basis,Balance_Price,INav,IIndex')
        f.write('\n')
def create_pd():
    df = pd.DataFrame(columns=['Type','Security_Code','Exchange_Code','Security_Id','Security_Type','Listed_Shares','Time','Trade_Date','Ceiling_Price','Floor_Price','Ref_Price','Buy_Price1','Buy_Volume1','Buy_Price2','Buy_Volume2','Buy_Price3','Buy_Volume3','Sell_Price1','Sell_Volume1','Sell_Price2','Sell_Volume2','Sell_Price3','Sell_Volume3','Buy_Price4','Buy_Volume4','Buy_Price5','Buy_Volume5','Buy_Price6','Buy_Volume6','Buy_Price7','Buy_Volume7','Buy_Price8','Buy_Volume8','Buy_Price9','Buy_Volume9','Buy_Price10','Buy_Volume10','Sell_Price4','Sell_Volume4','Sell_Price5','Sell_Volume5','Sell_Price6','Sell_Volume6','Sell_Price7','Sell_Volume7','Sell_Price8','Sell_Volume8','Sell_Price9','Sell_Volume9','Sell_Price10','Sell_Volume10','Close_Price','Match_Volume','Match_Value','Diff','Diff_Rate','Open_Price','High_Price','Low_Price','Average_Price','Total_Volume','Total_Value','Match_Price_Pt','Match_Volume_Pt','Total_Pt_Volume','Total_Pt_Value','F_Buy_Volume','F_Buy_Volume_Last','F_Sell_Volume','F_Sell_Volume_Last','F_Buy_Value','F_Sell_Value','F_Room','F_Room_Pct','Total_Room','Buy_Order','Sell_Order','Total_Buy_Volume','Total_Sell_Volume','Remain_Buy','Remain_Sell','Open_Interest','Last_Trade_Date','Vn30_Basis','Stock_Basis','Balance_Price','INav,IIndex'])
    return df

engine = create_engine('postgresql://mrneo:123123@localhost:5432/postgres')
r = redis.StrictRedis(host='localhost', port=6379, db=0)
# Danh sách các colum được insert
listColumesName = ['Exchange_Code', 'Time', 'Security_Code', 'Security_Id', 'Trade_Date', 'Security_Type', 'Ceiling_Price', 'Floor_Price', 'Ref_Price',
                   'Buy_Price3', 'Buy_Volume3', 'Buy_Price2', 'Buy_Volume2', 'Buy_Price1', 'Buy_Volume1',
                   'Close_Price', 'Match_Volume', 'Diff',
                   'Sell_Price1', 'Sell_Volume1', 'Sell_Price2', 'Sell_Volume2', 'Sell_Price3', 'Sell_Volume3',
                   'Total_Volume', 'Total_Value', 'Average_Price', 'Open_Price', 'High_Price', 'Low_Price', 'F_Buy_Volume', 'F_Sell_Volume', 'F_Room', 'Total_Room', 'Open_Interest']


_df_vps_all_stock = pd.DataFrame(columns=listColumesName)


def trans_mess_vps(response, ex):
    print("VPS ->>>")
    global _df_vps_all_stock
    response = response['response']
    response = json.dumps(response, ensure_ascii=False)
    response = response.replace(r"42[", '[')
    response = response.replace('ATC', '0')  # xem để null hoạc 0
    response = response.replace('ATO', '0')  # xem để null hoạc 0
    # print(response)
    response = json.loads(response)
    if 'payloadData' in response:
        response = json.loads(response['payloadData'])
        # print(response)
        if len(response) == 2 and 'data' in response[1]:
            # print(response)
            df_new = pd.DataFrame(response[1]['data'], index=[0])
            if 'g1' in response[1]['data']:
                if response[1]['data']['side'] == 'B':
                    df_new[['Buy_Price1', 'Buy_Volume1', 'code']
                           ] = response[1]['data']['g1'].split('|')
                else:
                    df_new[['Sell_Price1', 'Sell_Volume1', 'code']
                           ] = response[1]['data']['g1'].split('|')
            if 'g2' in response[1]['data']:
                if response[1]['data']['side'] == 'B':
                    df_new[['Buy_Price2', 'Buy_Volume2', 'code2']
                           ] = response[1]['data']['g2'].split('|')
                else:
                    df_new[['Sell_Price2', 'Sell_Volume2', 'code2']
                           ] = response[1]['data']['g2'].split('|')
            if 'g3' in response[1]['data']:
                if response[1]['data']['side'] == 'B':
                    df_new[['Buy_Price3', 'Buy_Volume3', 'code3']
                           ] = response[1]['data']['g3'].split('|')
                else:
                    df_new[['Sell_Price3', 'Sell_Volume3', 'code3']
                           ] = response[1]['data']['g3'].split('|')
            # Đổi lại tên colume
            # print(df_new['timeServer'])
            df_new.rename(columns={'sym': 'Security_Code',
                                   'id': 'Security_Id',
                                   'c': 'Ceiling_Price',  # không phải gói tin nào cũng có
                                   'f': 'Floor_Price',  # không phải gói tin nào cũng có
                                   'r': 'Ref_Price',  # không phải gói tin nào cũng có
                                   'lastPrice': 'Close_Price',
                                   'lastVol': 'Match_Volume',
                                   'change': 'Diff',
                                   'totalVol': 'Total_Volume',
                                   'R': 'Total_Value',
                                   'ap': 'Average_Price',
                                   'openPrice': 'Open_Price',  # Chưa có
                                   'hp': 'High_Price',
                                   'lp': 'Low_Price',
                                   'S': 'F_Buy_Volume',  # chưa có
                                   'T': 'F_Sell_Volume',  # chưa có
                                   'V': 'F_Room',  # chưa có
                                   'U': 'Total_Room',  # chưa có
                                   'OI': 'Open_Interest',  # chưa có
                                   'timeServer': 'Time'
                                   }, inplace=True)
            df_new['Exchange_Code'] = ex
            df_new['Trade_Date'] = str(date.today())
            df_new['Time'] = pd.to_datetime(
                str(date.today())+" " + df_new['Time'].astype(str))
            # df_new['Time'] = pd.to_datetime(df_new['Time'], errors='coerce')

            # Ép lại kiểu dữ liệu
            df_new = df_new.replace(np.nan, 0, regex=True)
            df_new = df_new.replace('', 0, regex=True)
            # Chuyển dạng string 1,222,222 thành số
            df_new.replace(',', '', regex=True, inplace=True)

            # print("3-->"+df['Buy_Volume3'].str.replace(r',',''))
            if 'Ceiling_Price' in df_new.columns:
                df_new['Ceiling_Price'] = df_new['Ceiling_Price'].replace(
                    '', np.nan).astype(float)
            if 'Floor_Price' in df_new.columns:
                df_new['Floor_Price'] = df_new['Floor_Price'].astype(float)
            if 'Ref_Price' in df_new.columns:
                df_new['Ref_Price'] = df_new['Ref_Price'].astype(float)
            if 'Buy_Price3' in df_new.columns:
                df_new['Buy_Price3'] = df_new['Buy_Price3'].astype(float)
            if 'Buy_Volume3' in df_new.columns:
                df_new['Buy_Volume3'] = df_new['Buy_Volume3'].astype('int64')
            if 'Buy_Price2' in df_new.columns:
                df_new['Buy_Price2'] = df_new['Buy_Price2'].astype(float)
            if 'Buy_Volume2' in df_new.columns:
                df_new['Buy_Volume2'] = df_new['Buy_Volume2'].astype('int64')
            if 'Buy_Price1' in df_new.columns:
                df_new['Buy_Price1'] = df_new['Buy_Price1'].astype(float)
            if 'Buy_Volume1' in df_new.columns:
                df_new['Buy_Volume1'] = df_new['Buy_Volume1'].astype('int64')
            if 'Close_Price' in df_new.columns:
                df_new['Close_Price'] = df_new['Close_Price'].astype(float)
            if 'Match_Volume' in df_new.columns:
                df_new['Match_Volume'] = df_new['Match_Volume'].astype('int64')
            if 'Diff' in df_new.columns:
                df_new['Diff'] = df_new['Diff'].astype(float)
            if 'Sell_Price1' in df_new.columns:
                df_new['Sell_Price1'] = df_new.Sell_Price1.astype(float)
            if 'Sell_Volume1' in df_new.columns:
                df_new['Sell_Volume1'] = df_new.Sell_Volume1.astype('int64')
            if 'Sell_Price2' in df_new.columns:
                df_new['Sell_Price2'] = df_new.Sell_Price2.astype(float)
            if 'Sell_Volume2' in df_new.columns:
                df_new['Sell_Volume2'] = df_new.Sell_Volume2.astype('int64')
            if 'Sell_Price3' in df_new.columns:
                df_new['Sell_Price3'] = df_new.Sell_Price3.astype(float)
            if 'Sell_Volume3' in df_new.columns:
                df_new['Sell_Volume3'] = df_new.Sell_Volume3.astype('int64')
            if 'Total_Volume' in df_new.columns:
                df_new['Total_Volume'] = df_new.Total_Volume.astype('int64')
            if 'Total_Value' in df_new.columns:
                df_new['Total_Value'] = df_new.Total_Value.astype('int64')
            if 'Average_Price' in df_new.columns:
                df_new['Average_Price'] = df_new.Average_Price.astype(float)
            if 'Open_Price' in df_new.columns:
                df_new['Open_Price'] = df_new.Open_Price.astype(float)
            if 'High_Price' in df_new.columns:
                df_new['High_Price'] = df_new.High_Price.astype(float)
            if 'Low_Price' in df_new.columns:
                df_new['Low_Price'] = df_new.Low_Price.astype(float)
            if 'F_Buy_Volume' in df_new.columns:
                df_new['F_Buy_Volume'] = df_new.F_Buy_Volume.astype('int64')
            if 'F_Sell_Volume' in df_new.columns:
                df_new['F_Sell_Volume'] = df_new.F_Sell_Volume.astype('int64')
            if 'F_Room' in df_new.columns:
                df_new['F_Room'] = df_new.F_Room.astype('int64')
            if 'Total_Room' in df_new.columns:
                df_new['Total_Room'] = df_new.Total_Room.astype('int64')
            if 'Open_Interest' in df_new.columns:
                df_new['Open_Interest'] = df_new['Open_Interest'].astype(float)

             # Tính toán giá tham chiếu
            if 'ch' in df_new.columns and df_new['ch'].values[0] == 'i':  # tăng
                df_new['Ref_Price'] = df_new['Close_Price'].values[0] - \
                    df_new['Diff'].values[0]
            if 'ch' in df_new.columns and df_new['ch'].values[0] == 'd':  # giảm
                df_new['Ref_Price'] = df_new['Close_Price'].values[0] + \
                    df_new['Diff'].values[0]
            if 'change' in df_new.columns and df_new['change'].values[0] == '0.00':
                df_new['Ref_Price'] = df_new['Close_Price'].values[0]
            # kiểm tra nếu colum
            # nằm ngoài danh sách định nghĩa thì xóa
            for ci in df_new.columns:
                if ci not in listColumesName:
                    df_new = df_new.drop(columns=[ci])

                # Cập nhật lại dữ liệu mới vào dòng dữ liệu cũ
                # print(df_new['Security_Code'])
            if 'sym' in response[1]['data'] and df_new['Security_Code'] is not None:
                df_stock_current_row = _df_vps_all_stock.loc[_df_vps_all_stock['Security_Code']
                                                             == df_new['Security_Code'].values[0]]
                # nếu chưa có thì gán mới
                if len(df_stock_current_row.index) == 0 and df_new['Security_Code'] is not None:
                    df_stock_current_row = df_new
                    # print("old->>>"+df_stock_current_row['Security_Code'].values[0])
                    _df_vps_all_stock = _df_vps_all_stock.append(
                        df_new, ignore_index=True)
                # có rồi cập nhật dữ liệu mới
                else:
                    print("update data for old row->>>" +
                          df_stock_current_row['Security_Code'].values[0])
                    for c_name in df_stock_current_row.columns:
                        if c_name in df_new and df_new[c_name] is not None:
                            df_stock_current_row[c_name].values[0] = df_new[c_name].values[0]
                            _df_vps_all_stock.loc[_df_vps_all_stock['Security_Code']
                                                  == df_new['Security_Code'].values[0], c_name] = df_new[c_name].values[0]
                print(df_stock_current_row)
                df_m=create_pd()
                df_m=pd.concat([df_m,df_stock_current_row]).fillna(0)
                try:                 
                    df_m.to_csv('realtime_ord2.csv',index=False,header=False,mode='a')
                except:
                    pass
                # insert tất cả dữ liệu chứng khoán đã lấy được mới nhất vào csv
                # df_stock_current_row.to_csv('test2.csv',  header=True)
                # _df_vps_all_stock là bảng tạm chứa tất cả mã chứng khoán với dữ liệu mới nhất
                # df_stock_current_row có thể insert vào DB từ dòng này , là dữ liệu mới nhất của 1 mã chúng khoán
        else:
            print("VPS No stock data")
    else:
        print("VPS No payloadData")


async def browserRun(company, url, ex):
    # args = ['--start-maximized']
    browser = await launch(
        # args=args,
        headless=True,
        args=['--no-sandbox'],
        # autoClose=False
    )

    page = await browser.newPage()
    await page.goto(url)
    # await page.setViewport({'width': 1920, 'height': 1080})

    # create CDP Session
    cdp = await page.target.createCDPSession()
    await cdp.send('Network.enable')

    def printResponse(response):
        try:
            if company == 'vps':
                # print(response)
                # if isinstance(response['response']['payloadData'], dict):
                trans_mess_vps(response, ex)
                # else:
                #     print(response)
            else:
                print('not exist')
        except BaseException:
            logging.exception("An exception was thrown!")
            pass
    cdp.on('Network.webSocketFrameReceived', printResponse)


    await asyncio.sleep(28800)


if __name__ == "__main__":
    # nhận tham số Command Line
    company = str(sys.argv[1])
    url = str(sys.argv[2])
    ex = str(sys.argv[3])
    asyncio.get_event_loop().run_until_complete(browserRun(company, url, ex))

