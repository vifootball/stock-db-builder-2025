from utils.csv_utils import *
import os
import logging
from datetime import datetime

def setup_logger():
    task_layer = "L1"
    task_name = "l1_history_chunk"
    log_dirpath = f"logs/{task_layer}/{task_name}"
    now = datetime.now().strftime("%y%m%d_%H%M")
    log_fname = f"{task_name}_{now}.log"

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

# 1. ETF history
# 2. Currency History
# 3. Index Yahoo history
# 4. Index FRED history


def run_l1_etf_history_chunk():
    setup_logger()
    logging.info("Start: run_l1_etf_history_chunk")

    # 1. ETF history
    logging.info("Start: Save dfs by chunk: ETF History")
    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l1_etf_history/',
            put_dirpath='downloads/l1_etf_history_chunk/',
            prefix_chunk='etf_history_chunk_chunk'
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    logging.info("End: Save dfs by chunk: ETF History")


def run_l1_currency_history_chunk():
    setup_logger()

    # 2. Currency History
    logging.info("Start: Save dfs by chunk: Currency History")
    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l1_currency_history/',
            put_dirpath='downloads/l1_currency_history_chunk/',
            prefix_chunk='currency_history_chunk_chunk'
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    logging.info("End: Save dfs by chunk: Currency History")


def run_l1_indices_yahoo_history_chunk():
    setup_logger()

    # 3. Index Yahoo history
    logging.info("Start: Save dfs by chunk: Index Yahoo History")
    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l1_indices_yahoo_history/',
            put_dirpath='downloads/l1_indices_yahoo_history_chunk/',
            prefix_chunk='indices_yahoo_history_chunk'
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    logging.info("End: Save dfs by chunk: Index Yahoo History")


def run_l1_indices_fred_history_chunk():
    setup_logger()

    # 4. Index Fred history
    logging.info("Start: Save dfs by chunk: Index Fred History")
    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l1_indices_fred_history/',
            put_dirpath='downloads/l1_indices_fred_history_chunk/',
            prefix_chunk='indices_fred_history_chunk_chunk'
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    logging.info("End: Save dfs by chunk: Index Fred History")

