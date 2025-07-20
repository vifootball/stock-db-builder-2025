import os
import time
import random
import logging
import pandas as pd
from datetime import datetime
from history.correlation import *
from tqdm import tqdm

def setup_logger():
    task_layer = "L2"
    task_name = "l2_correlation"
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


# TODO: run_l2_correlation_ray

def run_l2_correlation():
    setup_logger()
    logging.info("Start: run_l2_correlation")
    # 진행상황 알 수 있는 프린트 문 작성하기

    # 히스토리 데이터 경로 설정
    dirpath = './downloads/l1_etf_history'
    base_fname_list = sorted([x for x in os.listdir('./downloads/l1_etf_history') if x.endswith('csv')])[:]
    target_fname_list = sorted([x for x in os.listdir('./downloads/l1_etf_history') if x.endswith('csv')])

    # base 마다 target과 의 상관관계를 계산하여 파일로 저장
    for base_fname in tqdm(base_fname_list[:], mininterval=0.5):
        time.sleep(0.3)
        corrs_list = []
        
        # Load base history
        base_fpath = os.path.join(dirpath, base_fname)
        base_history = pd.read_csv(base_fpath)
        base_symbol = base_history['symbol'][0]

        print(f'[Collect Correlations] [{base_symbol}] Calculating Correlations: Processing')
        # target을 하나씩 불러와서 base와 상관관계 계산
        for target_fname in tqdm(target_fname_list[:],mininterval=0.5):
            
            # Load target history
            target_fpath = os.path.join(dirpath, target_fname)
            target_history = pd.read_csv(target_fpath)

            corrs = get_availaible_corrs(base_history, target_history)
            corrs_list.append(corrs)
        
        corrs_list = pd.concat(corrs_list)
        
        # 상관관계 순위 매기기
        corrs_list['rank_yearly_asc'] = corrs_list.groupby('unit_period')['corr_yearly'].rank(method='max')
        corrs_list['rank_yearly_desc'] = corrs_list.groupby('unit_period')['corr_yearly'].rank(method='max', ascending=False)
        corrs_list['rank_monthly_asc'] = corrs_list.groupby('unit_period')['corr_monthly'].rank(method='max')
        corrs_list['rank_monthly_desc'] = corrs_list.groupby('unit_period')['corr_monthly'].rank(method='max', ascending=False)
        corrs_list['rank_weekly_asc'] = corrs_list.groupby('unit_period')['corr_weekly'].rank(method='max')
        corrs_list['rank_weekly_desc'] = corrs_list.groupby('unit_period')['corr_weekly'].rank(method='max', ascending=False)
        corrs_list['rank_daily_asc'] = corrs_list.groupby('unit_period')['corr_daily'].rank(method='max')
        corrs_list['rank_daily_desc'] = corrs_list.groupby('unit_period')['corr_daily'].rank(method='max', ascending=False)
        
        print(f'[Collect Correlations] [{base_symbol}] Calculating Correlations: Completed')

        # base에 대한 corrs 파일 저장
        save_dirpath = 'downloads/l2_correlation'
        os.makedirs(save_dirpath, exist_ok=True)
        corrs_list.to_csv(f'{save_dirpath}/l2_correlation_{base_symbol}.csv', index=False)

    logging.info("End: run_l2_correlation")

