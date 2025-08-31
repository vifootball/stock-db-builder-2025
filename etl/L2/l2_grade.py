import os
import time
import random
import logging
import pandas as pd
from datetime import datetime
from history.grade import *
from tqdm import tqdm
from utils.logging_helper import *


# def setup_logger():
#     log_dirpath = "logs/L2/l2_grade"
#     now = datetime.now().strftime("%y%m%d_%H%M")
#     log_fname = f"l0_indices_yahoo_history_{now}.log"

#     os.makedirs(log_dirpath, exist_ok=True)
#     log_fpath = os.path.join(log_dirpath, log_fname)

#     logging.basicConfig(
#         format="%(asctime)s | %(levelname)s | %(message)s",
#         level=logging.INFO,  # DEBUG로 바꾸면 더 상세히 출력 가능
#         handlers=[
#             logging.FileHandler(log_fpath),  # 파일 저장
#             logging.StreamHandler()  # 콘솔에도 출력
#         ]
#         , force=True
#     )

# TBD 로깅 추가
def run_l2_grade():
    setup_logger()
    enter_root()
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Start: {function_name}")

    # (1) 고정 데이터 로드
    # (2) History 데이터 경로 설정
    # (3) 반복문으로 History에 대한 Grade 수집
    # (4) 수집한 Grades를 Concat & 저장
    # (5) Concat한 Grades를 Pivot & 저장

    # (1) 고정 데이터 로드
    logging.info("Required Data Loading")
    masterdata_path = './downloads/l1_etf_master.csv'
    masterdata = pd.read_csv(masterdata_path)

    history_market_path = './downloads/l1_etf_history/l1_etf_history_SPY.csv'
    history_market = pd.read_csv(history_market_path)
    logging.info("Required Data Loaded")

    # (2) History 데이터 경로 설정
    history_dirpath = './downloads/l1_etf_history'
    history_fpaths = [os.path.join(history_dirpath, f) for f in sorted(os.listdir(history_dirpath)) if f.endswith('.csv')][:]

    # (3) 반복문으로 History에 대한 Grade 수집
    logging.info("Calculating Grades: Start")
    grades = []
    for history_fpath in tqdm(history_fpaths, mininterval=0.5):
        history_symbol = pd.read_csv(history_fpath)
        symbol = history_fpath.split('_')[-1].split('.')[0]
        master_symbol = masterdata[masterdata['symbol']==symbol]
        df = generate_grades_by_period(master_symbol=master_symbol, history_symbol=history_symbol, history_market=history_market)
        grades.append(df)
    logging.info("Calculating Grades: End")


    # (4) 수집한 Grades를 Concat & 저장
    logging.info("Concatenating Grades: Start")
    grades = pd.concat(grades).reset_index(drop=True)
    grades.to_csv('./downloads/l2_grade.csv', index=False)
    logging.info("Concatenating Grades: End")

    # (5) Concat한 Grades를 Pivot & 저장
    logging.info("Pivotting Grades: Start")
    grades_pivotted = pivot_grades(grades)
    grades_pivotted.to_csv('./downloads/l2_grade_pivot.csv', index=False)
    logging.info("Pivotting Grades: Start")

    logging.info(f"End: {function_name}")