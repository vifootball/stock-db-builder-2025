from manual_data.etf_exclude import *
from financial_data.etf import *
from history.history import *
from utils.csv_utils import *
import os
import logging
from datetime import datetime

log_dirpath = "logs/L0/l0_etf_history"
now = datetime.now().strftime("%y%m%d_%H%M")
log_fname = f"l0_etf_history_{now}.log"

os.makedirs(log_dirpath, exist_ok=True)
log_fpath = os.path.join("logs/L0/l0_etf_history", f"l0_etf_history_{now}.log")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,  # DEBUG로 바꾸면 더 상세히 출력 가능
    handlers=[
        logging.FileHandler(log_fpath),  # 파일 저장
        logging.StreamHandler()  # 콘솔에도 출력
    ]
    , force=True
)

# TODO: ray 구현
def run_l0_etf_history():
    logging.info("Start: run_l0_etf_history")

    # etf list 불러오기
    etf_list = get_symbols()
    etf_list = [x for x in etf_list if x not in ETF_EXCLUDE][:]
    logging.info(f"Total ETFs to Process: {len(etf_list)}")

    # 파싱하여 저장
    for symbol in etf_list:
        time.sleep(round(random.uniform(0.5, 1), 3))
        logging.info(f"[{symbol}] Fetching data")

        try:
            # 응답 획득
            history = get_history_from_yf(symbol)
            # 파싱 후 저장
            if len(history):
                logging.info(f"[{symbol}] Fetched rows: {len(history)}")
                dirpath = 'downloads/l0_etf_history/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_etf_history_{symbol}.csv')
                
                history.to_csv(fpath, index=False)
                logging.info(f"[{symbol}] Saved: {fpath}")
            else:
                logging.warning(f"[{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")

    logging.info("end: run_l0_etf_history")
