# This code use vnstock open-source library to get Vietnam stock data. 
# Here is more detail about the library:
# https://vnstocks.com/docs/tai-lieu/huong-dan-nhanh/
from vnstock import *
import pandas as pd
from datetime import date,timedelta

# Get the current day, and some specific day to crawl up-to-date data
def current_day():
    return(str(date.today()))
def year_start_day():
    return("2024-01-01")
def month_start_day():
    return(str(date.today().replace(day=1)))
week_start_day=str(date.today() - timedelta(days=date.today().weekday()))
# Function to get a stock up-to-date price and convert to csv file 
def get_stock_data(stock_symbol:str):
    data_price=stock_historical_data(symbol=stock_symbol, start_date=year_start_day(), end_date=current_day(), resolution="1D", type="stock", beautify=True, decor=False, source='DNSE')
    df = pd.DataFrame(data_price)
    path='/opt/airflow/data/stockprice.csv'
    df.to_csv(path, index=False)
def get_full_list_stock_price():
    companies_list=pd.DataFrame(listing_companies(live=True))
    companies_list=companies_list[['ticker','comGroupCode']]
    platform_condition = companies_list['comGroupCode'] == "HOSE"
    com_list=companies_list[platform_condition]
    com_list=com_list['ticker']
    columns = ['time','open','high','low','close','volume','ticker']
    df_combined = pd.DataFrame(columns=columns)
    for company in com_list:
        temp_df=pd.DataFrame(stock_historical_data(symbol=company, start_date=current_day(), end_date=current_day(), resolution="1D", type="stock", beautify=True, decor=False, source='DNSE'))
        df_combined=pd.concat([df_combined, temp_df], ignore_index=True)
    df_combined.to_csv("/opt/airflow/data/allstockprice.csv",index=False,header=False)
# Function to get vnindex up-to-date data and convert to csv file 
def get_vnindex(): 
    df= pd.DataFrame(stock_historical_data("VNINDEX",year_start_day(), current_day(), "1D", "index", source='TCBS'))
    df.to_csv("/opt/airflow/data/vnindex.csv",index=False)
# get_vnindex()
# get_full_list_stock_price()