import os
import time
import random
import logging
from datetime import datetime
from manual_data import indices_reference
from history.history import *

"""
COLUMNS: 
"symbol",
"date",
"close"
"""

# 여기서 선언해야 다른 로그들이랑 안꼬이는 듯
def setup_logger():
    log_dirpath = "logs/L0/l0_indices_fred_history"
    now = datetime.now().strftime("%y%m%d_%H%M")
    log_fname = f"l0_indices_fred_history_{now}.log"

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

def run_l0_indices_fred_history():
    setup_logger()

    logging.info("Start: run_l0_fred_history")

    # indices list 불러오기
    fred_indices = indices_reference.FRED
    fred_indices_list = [item["symbol"] for item in fred_indices]

    # 수집하기
    for symbol in fred_indices_list[:]:
        time.sleep(round(random.uniform(0.5, 1), 3))
        logging.info(f"[{symbol}] Fetching data")

        try:
            # 응답 획득
            history = get_history_from_fred(symbol)
            # 파싱 후 저장
            if len(history):
                logging.info(f"[{symbol}] Fetched rows: {len(history)}")
                dirpath = 'downloads/l0_indices_fred_history/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_indices_fred_history_{symbol}.csv')

                history.to_csv(fpath, index=False)
                logging.info(f"[{symbol}] Saved: {fpath} | Size: {history.shape[0]} rows x {history.shape[1]} columns")
                
            else:
                logging.warning(f"[{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")

    logging.info("end: run_l0_indices_fred_history")

    