import os
import time
import random
import logging
from datetime import datetime
from history.preprocessor import *

def setup_logger():
    log_dirpath = "logs/L1/l1_history"
    now = datetime.now().strftime("%y%m%d_%H%M")
    log_fname = f"l1_history_{now}.log"

    os.makedirs(log_dirpath, exist_ok=True)
    log_fpath = os.path.join(log_dirpath, log_fname)

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,  # DEBUG로 바꾸면 더 상세히 출력 가능
        handlers=[
            logging.FileHandler(log_fpath),  # 파일 저장
            logging.StreamHandler()  # 콘솔에도 출력
        ]
        , force=True
    )

def run_l1_history():
    setup_logger()
    logging.info("Start: run_l1_history")
    

    # etf history
    etf_history_paths = [os.path.join('downloads/l0_etf_history', f) for f in sorted(os.listdir('downloads/l0_etf_history')) if f.endswith('.csv')][:]
    for etf_history_path in etf_history_paths:
        symbol = etf_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{symbol}] Processing")
            etf_history = pd.read_csv(etf_history_path)
            etf_history = transform_history(etf_history)
            
            
            path_to_save = os.path.join(f'downloads/l1_etf_history/l1_etf_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            etf_history.to_csv(path_to_save, index=False)
            logging.info(f"[{symbol}] Saved: {path_to_save} | Size: {etf_history.shape[0]} rows x {etf_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")


    # currency history
    currency_history_paths = [os.path.join('downloads/l0_currency_history', f) for f in sorted(os.listdir('downloads/l0_currency_history')) if f.endswith('.csv')][:]
    for currency_history_path in currency_history_paths:
        symbol = currency_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{symbol}] Processing")
            currency_history = pd.read_csv(currency_history_path)
            currency_history = transform_history(currency_history)
            
            path_to_save = os.path.join(f'downloads/l1_currency_history/l1_currency_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            currency_history.to_csv(path_to_save, index=False)
            logging.info(f"[{symbol}] Saved: {path_to_save} | Size: {currency_history.shape[0]} rows x {currency_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")
    

    # index yahoo history
    index_yahoo_history_paths = [os.path.join('downloads/l0_indices_yahoo_history', f) for f in sorted(os.listdir('downloads/l0_indices_yahoo_history')) if f.endswith('.csv')][:]
    for index_yahoo_history_path in index_yahoo_history_paths:
        symbol = index_yahoo_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{symbol}] Processing")
            index_yahoo_history = pd.read_csv(index_yahoo_history_path)
            index_yahoo_history = transform_history(index_yahoo_history)
            
            path_to_save = os.path.join(f'downloads/l1_indices_yahoo_history/l1_indices_yahoo_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            index_yahoo_history.to_csv(path_to_save, index=False)
            logging.info(f"[{symbol}] Saved: {path_to_save} | Size: {index_yahoo_history.shape[0]} rows x {index_yahoo_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")


    # index fred history
    index_fred_history_paths = [os.path.join('downloads/l0_indices_fred_history', f) for f in sorted(os.listdir('downloads/l0_indices_fred_history')) if f.endswith('.csv')][:]
    for index_fred_history_path in index_fred_history_paths:
        symbol = index_fred_history_path.split('_')[-1].split('.')
        
        try:
            logging.info(f"[{symbol}] Processing")
            index_fred_history = pd.read_csv(index_fred_history_path)

            # 컬럼 구조 맞추기
            header_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split']
            index_fred_history = index_fred_history.reindex(columns=header_cols)
    
            index_fred_history = transform_history(index_fred_history)
            
            path_to_save = os.path.join(f'downloads/l1_indices_fred_history/l1_indices_fred_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            index_fred_history.to_csv(path_to_save, index=False)
            logging.info(f"[{symbol}] Saved: {path_to_save} | Size: {index_fred_history.shape[0]} rows x {index_fred_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")


    logging.info("end: run_l0_currency_history")