from manual_data.etf_exclude import *
from financial_data.etf import *
from history.history import *
from utils.csv_utils import *
from utils.logging_helper import *
import os
import logging
from datetime import datetime

# TODO: ray 구현
def run_l0_etf_history():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # etf list 불러오기
    etf_list = get_symbols()
    etf_list = [x for x in etf_list if x not in ETF_EXCLUDE][:]
    total_len = len(etf_list)
    # logging.info(f"Total ETFs to Process: {len(etf_list)}")

    # 파싱하여 저장
    for idx, symbol in enumerate(etf_list, start=1):
        time.sleep(round(random.uniform(0.1, 0.3), 3))
        logging.info(f"[{idx}/{total_len}][{symbol}] Fetching data")

        try:
            # 응답 획득
            history = get_history_from_yf(symbol)
            # 파싱 후 저장
            if len(history):
                # logging.info(f"[{idx}/{total_len}][{symbol}] Fetched rows: {len(history)}")
                dirpath = 'downloads/l0_etf_history/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_etf_history_{symbol}.csv')

                history.to_csv(fpath, index=False)
                logging.info(f"[{idx}/{total_len}][{symbol}] Saved: {fpath} | Size: {history.shape[0]} rows x {history.shape[1]} columns")

            else:
                logging.warning(f"[{idx}/{total_len}][{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{idx}/{total_len}][{symbol}] Error occurred: {e}")

    logging.info(f"End: {function_name}")