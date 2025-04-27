import os
import pandas as pd
from datetime import datetime

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

def run_l1_symbol_master():
    print("Start: run_l1_symbol_master")

    # load etf master
    etf_master = pd.read_csv('downloads/l1_etf_master.csv')

    # load indices master
    indices_master_yahoo = pd.read_csv('downloads/l0_indices_master/l0_indices_master_yahoo.csv')
    indices_master_yahoo['domain'] = 'indices'

    indices_master_fred = pd.read_csv('downloads/l0_indices_master/l0_indices_master_fred.csv')
    indices_master_fred['domain'] = 'indices'

    # load currency master
    currency_master = pd.read_csv('downloads/l0_currency_master.csv')
    currency_master['domain'] = 'currency'

    # concat
    symbol_master = pd.concat([etf_master, indices_master_yahoo, indices_master_fred, currency_master], ignore_index=True)
    
    # save
    dirpath = 'downloads/'
    os.makedirs(dirpath, exist_ok=True)
    fpath = os.path.join(dirpath, f'l1_symbol_master.csv')
    symbol_master.to_csv(fpath, index=False)

    print(f"Saved: {fpath} | Size: {symbol_master.shape[0]} rows x {symbol_master.shape[1]} columns")
    print("End: run_l1_symbol_master")
