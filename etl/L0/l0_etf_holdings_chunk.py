import os
import logging
from utils.logging_helper import *
from utils.csv_utils import *
from datetime import datetime

def run_l0_etf_holdings_chunk():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        save_dfs_by_chunk(
            get_dirpath='downloads/l0_etf_holdings/',
            put_dirpath='downloads/l0_etf_holdings_chunk/',
            prefix_chunk='etf_holdings_chunk'
        )

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    
    logging.info(f"End: {function_name}")