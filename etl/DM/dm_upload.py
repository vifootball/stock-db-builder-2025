import logging
import inspect
from datetime import datetime
from utils.postgresql_helper import *
from utils.logging_helper import *

def upload_l1_symbol_master(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_file_to_db(
            csv_file_path = "downloads/l1_symbol_master.csv",
            connection_name = connection_name,
            table_name = "l1_symbol_master",
            create_table_sql_path = "sql/create_table_l1_symbol_master.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{upload_l1_symbol_master}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")


def upload_l1_currency_history_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")
    
    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l1_currency_history_chunk/",
            connection_name = connection_name,
            table_name = "l1_history",
            create_table_sql_path = "sql/create_table_l1_history.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")


def upload_l1_etf_history_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l1_etf_history_chunk/",
            connection_name = connection_name,
            table_name = "l1_history",
            create_table_sql_path = "sql/create_table_l1_history.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")
    

def upload_l1_indices_yahoo_history_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l1_indices_yahoo_history_chunk/",
            connection_name = connection_name,
            table_name = "l1_history",
            create_table_sql_path = "sql/create_table_l1_history.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")    


def upload_l1_indices_fred_history_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l1_indices_fred_history_chunk/",
            connection_name = connection_name,
            table_name = "l1_history",
            create_table_sql_path = "sql/create_table_l1_history.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")    


def upload_l0_etf_holdings_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l0_etf_holdings_chunk/",
            connection_name = connection_name,
            table_name = "l0_etf_holdings",
            create_table_sql_path = "sql/create_table_l0_etf_holdings.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")  


def upload_l2_correlation_chunk(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_files_to_db(
            csv_dir_path="downloads/l2_correlation_chunk/",
            connection_name = connection_name,
            table_name = "l2_correlation",
            create_table_sql_path = "sql/create_table_l2_correlation.sql",
            mode = mode
        )
    except Exception as e:
        logging.error(f"[{function_name}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")  


def upload_l2_grade(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_file_to_db(
            csv_file_path = "downloads/l2_grade.csv",
            connection_name = connection_name,
            table_name = "l2_grade",
            create_table_sql_path = "sql/create_table_l2_grade.sql",
            mode = "replace"
        )
    except Exception as e:
        logging.error(f"[{upload_l1_symbol_master}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")  


def upload_l2_grade_pivot(connection_name, mode):
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    try:
        copy_csv_file_to_db(
            csv_file_path = "downloads/l2_grade_pivot.csv",
            connection_name = connection_name,
            table_name = "l2_grade_pivot",
            create_table_sql_path = "sql/create_table_l2_grade_pivot.sql",
            mode = "replace"
        )
    except Exception as e:
        logging.error(f"[{upload_l1_symbol_master}] Error occurred: {e}")

    logging.info(f"End: {function_name}\n")      