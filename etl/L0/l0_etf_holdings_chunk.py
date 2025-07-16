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

def run_l0_etf_holdings_chunk():
    setup_logger()

    logging.info("Start: run_l0_etf_holdings_chunk")

    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l0_etf_holdings/',
            put_dirpath='downloads/l0_etf_holdings_chunk/',
            prefix_chunk='etf_holdings_chunk'
        )

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    
    logging.info("end: run_l0_etf_holdings_chunk")