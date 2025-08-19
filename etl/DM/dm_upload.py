import logging
from datetime import datetime
from utils.postgresql_helper import *


def setup_logger():
    task_layer = "DM"
    task_name = "dm_upload"
    log_dirpath = f"logs/{task_layer}/{task_name}"
    # now = datetime.now().strftime("%y%m%d_%H%M")
    now = datetime.now().strftime("%y%m%d")
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


def upload_l1_symbol_master(connection_name):
    setup_logger()
    function_name = "upload_l1_symbol_master"
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_file_to_db(
            csv_file_path = "downloads/l1_symbol_master.csv",
            connection_name = connection_name,
            table_name = "l1_symbol_master",
            create_table_sql_path = "sql/create_table_l1_symbol_master.sql",
            mode = "replace"
        )

    except Exception as e:
        logging.error(f"[{upload_l1_symbol_master}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")


def upload_l1_currency_history_chunk(connection_name):
    setup_logger()
    function_name = "upload_l1_currency_history_chunk"
    logging.info(f"Start: {function_name}")
    
    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l1_currency_history_chunk/",
            connection_name = connection_name,
            table_name = "l1_history",
            create_table_sql_path = "sql/create_table_l1_history.sql",
            mode = "replace"
        )

    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")


def upload_l1_etf_history():
    pass

def upload_l1_indices_yahoo_history():
    pass

def upload_l1_indices_fred_history():
    pass

def upload_l1_symbol_master():
    pass

def upload_l0_holdings_chunk():
    pass

def upload_l2_correlation_chunk():
    pass

def upload_l2_grade()
    pass

def upload_l2_grade_pivot()
    pass