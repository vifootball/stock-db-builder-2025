from manual_data.etf_exclude import *
from financial_data.etf import *
from utils.csv_utils import *
import os
import logging
from datetime import datetime

def setup_logger():
    log_dirpath = "logs/L0/l0_etf_holdings"
    now = datetime.now().strftime("%y%m%d_%H%M")
    log_fname = f"l0_etf_holdings_{now}.log"

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

# TODO: ray 구현
def run_l0_etf_holdings():
    setup_logger()

    logging.info("Start: run_l0_etf_holdings")

    # etf list 불러오기
    etf_list = get_symbols()
    etf_list = [x for x in etf_list if x not in ETF_EXCLUDE][:10]
    logging.info(f"Total ETFs to Process: {len(etf_list)}")

    # 파싱하여 저장
    for symbol in etf_list:
        time.sleep(round(random.uniform(0.5, 1), 3))
        logging.info(f"[{symbol}] Fetching data")

        try:
            # 응답 획득
            holdings = get_etf_holdings(symbol)
            # 파싱 후 저장
            if len(holdings):
                logging.info(f"[{symbol}] Fetched rows: {len(holdings)}")
                dirpath = 'downloads/l0_etf_holdings/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_etf_holdings_{symbol}.csv')

                holdings.to_csv(fpath, index=False)
                logging.info(f"[{symbol}] Saved: {fpath} | Size: {holdings.shape[0]} rows x {holdings.shape[1]} columns")

            else:
                logging.warning(f"[{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")

    logging.info("end: run_l0_etf_holdings")
