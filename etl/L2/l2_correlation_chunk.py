from utils.csv_utils import *
import os
import logging
from datetime import datetime

def setup_logger():
    task_layer = "L2"
    task_name = "l2_correlation_chunk"
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
            logging.StreamHandler()          # 콘솔에도 출력
        ]
        , force=True
    )

def run_l2_correlation_chunk():
    setup_logger()

    logging.info("Start: run_l2_correlation_chunk")

    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l2_correlation/',
            put_dirpath='downloads/l2_correlation_chunk/',
            prefix_chunk='correlation_chunk'
        )

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    
    logging.info("end: run_l2_correlation_chunk")