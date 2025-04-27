from financial_data.indices import *
import os
from datetime import datetime
# from utils.csv_utils import *

# 너무 간단해서 logging은 생략

def run_l0_indices_master():
    print("Start: run_l0_indices_master")

    dirpath = 'downloads/l0_indices_master/'
    os.makedirs(dirpath, exist_ok=True)
    fpath_indices_yahoo = os.path.join(dirpath, f'l0_indices_master_yahoo.csv')
    fpath_indices_fred = os.path.join(dirpath, f'l0_indices_master_fred.csv')

    indices_yahoo = get_indices_masters_yahoo()
    indices_yahoo.to_csv(fpath_indices_yahoo, index=False)
    print(f"Saved: {fpath_indices_yahoo} | Size: {indices_yahoo.shape[0]} rows x {indices_yahoo.shape[1]} columns")

    indices_fred = get_indices_masters_fred()
    indices_fred.to_csv(fpath_indices_fred, index=False)
    print(f"Saved: {fpath_indices_fred} | Size: {indices_fred.shape[0]} rows x {indices_fred.shape[1]} columns")

    print("End: run_l0_indices_master")
