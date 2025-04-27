import os
import pandas as pd
from datetime import datetime
# from utils.csv_utils import *

"""
COLUMNS: 
"domain", -> 수기로 추가 (etf, indices, currency)
"symbol", 
"name", -> etf_list로 매핑
"asset_class", 
"category", 
"region", 
"stock_exchange", 
"provider", 
"index_tracked", 
"aum", 
"nav", 
"expense_ratio", 
"shares_outstanding", 
"dividend_yield", 
"inception_date", 
"description", 
"holdings_count", 
"holdings_top10_percentage", 
"holdings_date"
"""

def run_l1_etf_master():
    print("Start: run_l0_currency_master")

    # load etf profile
    etf_profile = pd.read_csv('downloads/l0_etf_profile.csv')
    
    # load etf list
    etf_list = pd.read_csv('downloads/etf_list/etf_list_240605_union.csv')
    etf_list = etf_list[['symbol', 'name']]

    etf_master = etf_profile.merge(etf_list, how='left', on='symbol')

    # 도메인 할당
    etf_master['domain'] = 'etf'

    # 컬럼순서 재배치
    etf_master = etf_master[[
        'domain', 
        'symbol', 
        'name', 
        'asset_class', 
        'category', 
        'region', 
        'stock_exchange', 
        'provider', 
        'index_tracked', 
        'aum', 
        'nav', 
        'expense_ratio', 
        'shares_outstanding',
        'dividend_yield', 
        'inception_date', 
        'description', 
        'holdings_count', 
        'holdings_top10_percentage', 
        'holdings_date'
        ]]

    # symbol 순서대로 정렬
    etf_master = etf_master.sort_values(by=['symbol']).reset_index(drop=True)

    dirpath = 'downloads/'
    os.makedirs(dirpath, exist_ok=True)
    fpath = os.path.join(dirpath, f'l1_etf_master.csv')

    etf_master.to_csv(fpath, index=False)
    print(f"Saved: {fpath} | Size: {etf_master.shape[0]} rows x {etf_master.shape[1]} columns")
    print("End: run_l1_etf_master")
    