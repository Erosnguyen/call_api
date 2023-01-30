import csv
import requests
import pandas as pd
import time
from fake_useragent import UserAgent
from check_time import check_time_now
from datetime import datetime
ua = UserAgent()


# login
def login_dnse():
  # authentication: user&pass
  url = 'https://services.entrade.com.vn/entrade-api/v2/auth'
  myobj = {"password": "erosnguyen123","username": "cuong123go@gmail.com"}
  x = requests.post(url, json = myobj)
  token = x.json()['token']
  headers = {'Authorization': "Bearer {}".format(token),
             "User-Agent":ua.random}
  return headers,token
# long
def enter_long(lots):
  # gửi lệnh lên cty chứng khoán
  # authentication: user&pass
  headers,token = login_dnse()

  # khối lượng đặt
  long_MTL = {
   "bankMarginPortfolioId":32,
   "investorId":1000056212,
   "symbol":'VN30F2302',
   "price":0,
   "orderType":"MTL",
   "side":"NB",
   "quantity":lots
   }
  #order
  response = requests.post(link_order, headers = headers, json = long_MTL) # order long
  return response
def enter_short(lots):
  # gửi lệnh lên cty chứng khoán
  # authentication: user&pass
  headers, token = login_dnse()

  # khối lượng đặt
  long_MTL = {
   "bankMarginPortfolioId":32,
   "investorId":1000056212,
   "symbol":'VN30F2302',
   "price":0,
   "orderType":"MTL",
   "side":"NS",
   "quantity":lots
   }
  #order
  response = requests.post(link_order, headers = headers, json = long_MTL) # order long
  return response
link_order = 'https://services.entrade.com.vn/papertrade-entrade-api/derivative/orders'
link_status_order = 'https://services.entrade.com.vn/papertrade-entrade-api/derivative/orders?_end=1&_start=0&investorAccountId=1000056212'


df = pd.read_csv("signal.csv")
result = []
def delete_signal():
    with open('signal.csv','w',encoding='utf-8') as f:
        f.write('Time,Price,Status')
        f.write('\n')
pos = 0
prev_status = None
while True:
    check_time = check_time_now(datetime.now().time()) 
    if check_time:
        # Store the order information
        orders = []
    
        # Read the CSV file
        with open("signal.csv", "r") as file:
            reader = csv.reader(file)
            next(reader)  # skip the header row
            for row in reader:
                # Extract the relevant information from each row
                time_df,price,status = row[0],row[1],row[2]
    
                # Store the order information
                orders.append((time_df,price,status))
        
        # Loop through the orders
        for i, order in enumerate(orders):
            time_df,price,status = order
    
            # If there's no previous status, it's an open order
            if prev_status is None:
                print("Open Order:",time_df,price,status)
                if status=='buy':
                    enter_long(1)
                elif status=='sell':
                    enter_short(1)
                prev_status = status
    
            # If the current status is different from the previous status, it's an open order
            elif prev_status != status:
                print("Close Order:",time_df,price,status)
                print(pos)
                if status=='buy':
                    enter_long(1)
                elif status=='sell':
                    enter_short(1)
                prev_status = None
                delete_signal()
                # prev_status = status
    
            # If the current status is the same as the previous status, it's a holding order
            else:
                print("Holding:", time_df,price,status)
        time.sleep(1)
    
    
    
    