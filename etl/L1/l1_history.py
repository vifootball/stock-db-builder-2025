import os
import time
import random
import logging
from datetime import datetime
from history.preprocessor import *
from tqdm import tqdm
from utils.logging_helper import *

def run_l1_etf_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # etf history
    etf_history_paths = [os.path.join('downloads/l0_etf_history', f) for f in sorted(os.listdir('downloads/l0_etf_history')) if f.endswith('.csv')][:]
    total_len = len(etf_history_paths)

    for idx, etf_history_path in enumerate(etf_history_paths, start=1):
        symbol = etf_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{idx}/{total_len}][{symbol}] Processing")
            etf_history = pd.read_csv(etf_history_path)
            etf_history = transform_history(etf_history)
            
            path_to_save = os.path.join(f'downloads/l1_etf_history/l1_etf_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            etf_history.to_csv(path_to_save, index=False)
            logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {path_to_save} | Size: {etf_history.shape[0]} rows x {etf_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{idx}/{total_len}][{symbol}] Error occurred: {e}")

    logging.info(f"End: {function_name}")


def run_l1_currency_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # currency history
    currency_history_paths = [os.path.join('downloads/l0_currency_history', f) for f in sorted(os.listdir('downloads/l0_currency_history')) if f.endswith('.csv')][:]
    total_len = len(currency_history_paths)
    
    for idx, currency_history_path in enumerate(currency_history_paths, start=1):
        symbol = currency_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{idx}/{total_len}][{symbol}] Processing")
            currency_history = pd.read_csv(currency_history_path)
            currency_history = transform_history(currency_history)
            
            path_to_save = os.path.join(f'downloads/l1_currency_history/l1_currency_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            currency_history.to_csv(path_to_save, index=False)
            logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {path_to_save} | Size: {currency_history.shape[0]} rows x {currency_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")
    
    logging.info(f"End: {function_name}")


def run_l1_indices_yahoo_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # index yahoo history
    index_yahoo_history_paths = [os.path.join('downloads/l0_indices_yahoo_history', f) for f in sorted(os.listdir('downloads/l0_indices_yahoo_history')) if f.endswith('.csv')][:]
    total_len = len(index_yahoo_history_paths)
    
    for idx, index_yahoo_history_path in enumerate(index_yahoo_history_paths, start=1):
        symbol = index_yahoo_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{idx}/{total_len}][{symbol}] Processing")
            index_yahoo_history = pd.read_csv(index_yahoo_history_path)
            index_yahoo_history = transform_history(index_yahoo_history)
            
            path_to_save = os.path.join(f'downloads/l1_indices_yahoo_history/l1_indices_yahoo_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            index_yahoo_history.to_csv(path_to_save, index=False)
            logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {path_to_save} | Size: {index_yahoo_history.shape[0]} rows x {index_yahoo_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{idx}/{total_len}][{symbol}] Error occurred: {e}")
    
    logging.info(f"End: {function_name}")


def run_l1_indices_fred_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # index fred history
    index_fred_history_paths = [os.path.join('downloads/l0_indices_fred_history', f) for f in sorted(os.listdir('downloads/l0_indices_fred_history')) if f.endswith('.csv')][:]
    total_len = len(index_fred_history_paths)
    
    for idx, index_fred_history_path in enumerate(index_fred_history_paths, start=1):
        symbol = index_fred_history_path.split('_')[-1].split('.')[0]
        
        try:
            logging.info(f"[{idx}/{total_len}][{symbol}] Processing")
            index_fred_history = pd.read_csv(index_fred_history_path)

            # 컬럼 구조 맞추기
            header_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split']
            index_fred_history = index_fred_history.reindex(columns=header_cols)
    
            index_fred_history = transform_history(index_fred_history)
            
            path_to_save = os.path.join(f'downloads/l1_indices_fred_history/l1_indices_fred_history_{symbol}.csv')
            os.makedirs(os.path.dirname(path_to_save), exist_ok=True)

            index_fred_history.to_csv(path_to_save, index=False)
            logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {path_to_save} | Size: {index_fred_history.shape[0]} rows x {index_fred_history.shape[1]} columns")
        
        except Exception as e:
            logging.error(f"[{idx}/{total_len}][{symbol}] Error occurred: {e}")
    
    logging.info(f"End: {function_name}")