from financial_data.etf import *
from manual_data.etf_exclude import *
from utils.csv_utils import *
import os
import logging
from datetime import datetime

"""
COLUMNS: 
"symbol", 
"asset_class", 
"category", 
"region", 
"stock_exchange", 
"provider", 
"index_tracked", 
"aum", 
"nav", 
"expense_ratio", 
"shares_outstanding", 
"dividend_yield", 
"inception_date", 
"description", 
"holdings_count", 
"holdings_top10_percentage", 
"holdings_date"
"""

def setup_logger():
    log_dirpath = "logs/L0/l0_etf_profile"
    now = datetime.now().strftime("%y%m%d_%H%M")
    log_fname = f"l0_etf_profile_{now}.log"

    os.makedirs(log_dirpath, exist_ok=True)
    log_fpath = os.path.join(log_dirpath, log_fname)
    
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,  # DEBUG로 바꾸면 더 상세히 출력 가능
        handlers=[
            logging.FileHandler(log_path),  # 파일 저장
            logging.StreamHandler()  # 콘솔에도 출력
        ]
        , force=True
    )

def run_l0_etf_profile():
    setup_logger()

    logging.info("Start: run_l0_etf_profile")

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
            data = fetch_etf_data(symbol)
            # 파싱 후 저장
            if data:
                etf_profile = parse_etf_data(data)

                dirpath = 'downloads/l0_etf_profile/'
                os.makedirs(dirpath, exist_ok=True)
                fpath = os.path.join(dirpath, f'l0_etf_profile_{symbol}.csv')
                etf_profile.to_csv(fpath, index=False)
                logging.info(f"[{symbol}] Saved: {fpath}")
            else:
                logging.warning(f"[{symbol}] No data returned or empty response")

        except Exception as e:
            logging.error(f"[{symbol}] Error occurred: {e}")

    try:
        output_path = 'downloads/l0_etf_profile.csv'
        concat_csv_files_in_dir('downloads/l0_etf_profile/').to_csv(output_path, index=False)
        logging.info(f"Concatenated and Saved: {output_path}")
    except Exception as e:
        logging.error(f"Error while concatenating CSV files: {e}")
