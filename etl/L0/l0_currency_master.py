import os
import pandas as pd
from datetime import datetime
# from utils.csv_utils import *

# 너무 간단해서 logging은 생략

def run_l0_currency_master():
    print("Start: run_l0_etf_master")

    dirpath = 'downloads/'
    os.makedirs(dirpath, exist_ok=True)
    fpath = os.path.join(dirpath, f'l0_currency_master.csv')

    currency_master = get_currency_master()
    currency_master.to_csv(fpath, index=False)

    print(f"Saved: {fpath} | Size: {currency_master.shape[0]} rows x {currency_master.shape[1]} columns")

    print("End: run_l0_currency_master")

    