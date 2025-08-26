import os
import time
import random
import logging
from datetime import datetime
from financial_data.currency import *
from history.history import *
from utils.logging_helper import *

def run_l0_currency_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # currency list 불러오기
    currency_list = get_currencies()
    total_len = len(currency_list)

    # 수집하기
    for idx, symbol in enumerate(currency_list[:], start=1):
        time.sleep(round(random.uniform(0.1, 0.3), 3))
        logging.info(f"[{idx}/{total_len}][{symbol}] Fetching data")

        try:
            # 응답 획득
            history = get_history_from_yf(symbol)
            # 파싱 후 저장
            if len(history):
                # logging.info(f"[{idx}/{total_len}][{symbol}] Fetched rows: {len(history)}")
                dirpath = 'downloads/l0_currency_history/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_currency_history_{symbol}.csv')

                history.to_csv(fpath, index=False)
                logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {fpath} | Size: {history.shape[0]} rows x {history.shape[1]} columns")
                
            else:
                logging.warning(f"[{idx}/{total_len}][{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{idx}/{total_len}][{symbol}] Error occurred: {e}")

    logging.info(f"End: {function_name}")

    