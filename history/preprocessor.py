import pandas as pd
import numpy as np
# from dateutil.relativedelta import relativedelta
from typing import Union


# general
def copy_column(col: pd.Series) -> pd.Series:
    col_copied = col.copy(deep=True)
    return col_copied

def calc_price_change(price: pd.Series) -> pd.Series:
    price_change = price.diff()
    return price_change

def calc_price_change_rate(price: pd.Series, price_change: pd.Series) -> pd.Series:
    try:
        price_change_rate = price_change / price
        price_change_rate = price_change_rate.round(6)
    except ZeroDivisionError:
        price_change_rate = np.nan
    return price_change_rate

def calc_price_change_sign(price_change: pd.Series) -> pd.Series:
    if price_change.dtype in [int, float]:
        price_change_sign =  np.sign(price_change)
    else:
        price_change_sign = np.nan
    return price_change_sign

def calc_price_all_time_high(price):
    price_all_time_high = price.cummax()
    return price_all_time_high

def calc_drawdown_current(price, price_all_time_high):
    try:
        drawdown_current_plus1 = (price / price_all_time_high)
        if drawdown_current_plus1.dtype in [int, float]:
            drawdown_current = ((price / price_all_time_high) - 1).round(6)
        else:
            drawdown_current = np.nan
    except ZeroDivisionError:
        drawdown_current = np.nan
    return drawdown_current

def calc_drawdown_max(drawdown_current):
    drawdown_max = drawdown_current.cummin().round(6)
    return drawdown_max

def calc_price_7d_ago(price):
    price_7d_ago = price.shift(periods=7, freq='D')
    price_7d_ago = price_7d_ago.loc[price_7d_ago.index <= price_7d_ago.index.max() - pd.to_timedelta('7days')]
    price_7d_ago = price_7d_ago.ffill()
    return price_7d_ago

def calc_weekly_price_change(price, price_7d_ago): # 현재 가격과 1주일 전 가격의 차이 # 매 월/금 에 쓸듯
    weekly_price_change = (price - price_7d_ago).ffill()
    return weekly_price_change

def calc_weekly_price_change_rate(price_7d_ago, weekly_price_change):
    try:
        weekly_price_change_rate = (weekly_price_change / price_7d_ago).round(6)
    except ZeroDivisionError:
        weekly_price_change_rate = np.nan
    return weekly_price_change_rate

def calc_price_30d_ago(price):
    price_30d_ago = price.shift(periods=30, freq='D')
    price_30d_ago = price_30d_ago.loc[price_30d_ago.index <= price_30d_ago.index.max() - pd.to_timedelta('30days')]
    price_30d_ago = price_30d_ago.ffill()
    return price_30d_ago

# def calc_price_1m_ago(price):
    # 1안 relativedelta 이용하여 함수 구현..
    # 2안 월이나 연 단위 summary table 생성
    # unit column을 추가하여 하낭의 테이블에 union도 가능
    # pass


def calc_monthly_price_change(price, price_30d_ago):
    monthly_price_change = (price - price_30d_ago).ffill()
    return monthly_price_change
    
def calc_monthly_price_change_rate(price_30d_ago, monthly_price_change):
    try:
        monthly_price_change_rate = (monthly_price_change / price_30d_ago).round(6)
    except ZeroDivisionError:
        monthly_price_change_rate = np.nan
    return monthly_price_change_rate

def calc_price_365d_ago(price):
    price_365d_ago = price.shift(periods=365, freq='D')
    price_365d_ago = price_365d_ago.loc[price_365d_ago.index <= price_365d_ago.index.max() - pd.to_timedelta('365days')]
    price_365d_ago = price_365d_ago.ffill()
    return price_365d_ago

def calc_yearly_price_change(price, price_365d_ago):
    yearly_price_change = (price - price_365d_ago).ffill()
    return yearly_price_change
    
def calc_yearly_price_change_rate(price_365d_ago, yearly_price_change):
    try:
        yearly_price_change_rate = (yearly_price_change / price_365d_ago).round(6)
    except ZeroDivisionError:
        yearly_price_change_rate = np.nan
    return yearly_price_change_rate

#volume
def calc_volume_of_dollar(price, volume_of_shares): # estimated by close price
    volume_of_dollar = (price * volume_of_shares)
    if volume_of_dollar.dtype in [int, float]:
        volume_of_dollar = volume_of_dollar.round(0)
    return volume_of_dollar

def calc_volume_of_share_3m_avg(volume_of_share):
    volume_of_share_3m_avg = volume_of_share.rolling(window='90d').mean().round(0).to_numpy()
    return volume_of_share_3m_avg

def calc_volume_of_dollar_3m_avg(volume_of_dollar):
    volume_of_dollar_3m_avg = volume_of_dollar.rolling(window='90d').mean().round(0).to_numpy()
    return volume_of_dollar_3m_avg

# dividend
def calc_dividend_paid_or_not(dividend):
    if dividend.dtype in [int, float]:
        dividend_paid_or_not = np.sign(dividend)
    else:
        dividend_paid_or_not = np.nan
    return dividend_paid_or_not

def calc_dividend_paid_count_ttm(dividend_paid_or_not):
    dividend_paid_count_ttm = dividend_paid_or_not.rolling(window='365d').sum().to_numpy()
    return dividend_paid_count_ttm

def calc_dividend_ttm(dividend):
    dividend_ttm = dividend.rolling(window='365d').sum().to_numpy()
    return dividend_ttm

def calc_dividend_rate(price, dividend):
    try:
        dividend_rate = (dividend / price)
        if dividend_rate.dtype in [int, float]:
            dividend_rate = dividend_rate.round(6)
    except ZeroDivisionError: # price가 0을 찍는 것은 어차피 지수밖에 없음
        dividend_rate = np.nan
    return dividend_rate
    
def calc_dividend_rate_ttm(price, dividend_ttm):
    try:
        dividend_ttm = dividend_ttm / price
    except ZeroDivisionError:
        dividend_ttm = np.nan
    return dividend_ttm

def calculate_metrics_on_trading_dates(history):
    history['date'] = pd.to_datetime(history['date'])
    history = history.set_index('date')

    # price
    history['price'] = copy_column(history['close']).astype(float)
    history['price_all_time_high'] = calc_price_all_time_high(history['price'])
    history['drawdown_current'] = calc_drawdown_current(history['price'], history['price_all_time_high'])
    history['drawdown_max'] = calc_drawdown_max(history['drawdown_current'])
    
    # volume
    history['volume_of_share'] = copy_column(history['volume'])
    history['volume_of_share_3m_avg'] = calc_volume_of_share_3m_avg(history['volume_of_share'])
    history['volume_of_dollar'] = calc_volume_of_dollar(history['price'], history['volume_of_share'])
    history['volume_of_dollar_3m_avg'] = calc_volume_of_dollar_3m_avg(history['volume_of_dollar'])

    # div
    history['dividend_paid_or_not'] = calc_dividend_paid_or_not(history['dividend'])
    history['dividend_paid_count_ttm'] = calc_dividend_paid_count_ttm(history['dividend_paid_or_not'])
    history['dividend_ttm'] = calc_dividend_ttm(history['dividend'])
    history['dividend_rate'] = calc_dividend_rate(history['price'], history['dividend'])
    history['dividend_rate_ttm'] = calc_dividend_rate_ttm(history['price'], history['dividend_ttm'])

    # etc
    history['trading_day_tf'] = 1

    history = history.reset_index()
    return history

def fill_missing_date_index(history):
    history = history.set_index('date')
    dates = history.index
    dr = pd.date_range(start=min(dates), end=max(dates))
    dr = pd.DataFrame(dr).set_index(0)
    dr.index.name = 'date'
    history = dr.join(history, how='left')
    history.reset_index(drop=False, inplace=True)
    return history

def fill_na_values(history):
    history['symbol'] = history['symbol'].ffill()
    history['price'] = history['price'].ffill()
    history['price_all_time_high'] = history['price_all_time_high'].ffill()
    history['drawdown_current'] = history['drawdown_current'].ffill()
    history['drawdown_max'] = history['drawdown_max'].ffill()

    history['dividend_paid_count_ttm'] = history['dividend_paid_count_ttm'].ffill()
    history['dividend_ttm'] = history['dividend_ttm'].ffill()
    history['dividend_rate_ttm'] = history['dividend_rate_ttm'].ffill()

    history['trading_day_tf'] = history['trading_day_tf'].fillna(0)
    return history

def calculate_metrics_on_all_dates(history):
    history['date'] = pd.to_datetime(history['date'])
    history['date_id'] = history['date'].dt.strftime('%Y%m%d')
    history['transaction_id'] = history['symbol'] + '-' + history['date_id'].astype(str)
    history = history.set_index('date')

    # price
    history['price_change'] = calc_price_change(price=history['price'])
    history['price_change_rate'] = calc_price_change_rate(history['price'], history['price_change'])
    history['price_change_sign'] = calc_price_change_sign(history['price_change'])
    
    history['price_7d_ago'] = calc_price_7d_ago(price=history['price'])
    history['weekly_price_change'] = calc_weekly_price_change(price=history['price'], price_7d_ago=history['price_7d_ago'])
    history['weekly_price_change_rate'] = calc_weekly_price_change_rate(price_7d_ago=history['price_7d_ago'], weekly_price_change=history['weekly_price_change'])
    

    history['price_30d_ago'] = calc_price_30d_ago(price=history['price'])
    history['monthly_price_change'] = calc_monthly_price_change(price=history['price'], price_30d_ago=history['price_30d_ago'])
    history['monthly_price_change_rate'] = calc_monthly_price_change_rate(price_30d_ago=history['price_30d_ago'], monthly_price_change=history['monthly_price_change'])

    history['price_365d_ago'] = calc_price_365d_ago(price=history['price'])
    history['yearly_price_change'] = calc_yearly_price_change(price=history['price'], price_365d_ago=history['price_365d_ago'])
    history['yearly_price_change_rate'] = calc_yearly_price_change_rate(price_365d_ago=history['price_365d_ago'], yearly_price_change=history['yearly_price_change'])

    history = history.reset_index()
    return history

def transform_history(history: pd.DataFrame) -> Union[pd.DataFrame, None]:
    # 1. 거래일 데이터만으로 지표 게산
    # 2. 빈 날짜 채워주기
    # 3. 컬럼 별 적절한 방법으로 결측치 보간하기
    # 4. 모든 날짜범위에서 지표 계산
    
    if history is None:
        return None
    else:
        try:
            # 1
            history = calculate_metrics_on_trading_dates(history)
            # 2
            history = fill_missing_date_index(history)
            # 3
            history = fill_na_values(history)
            # 4
            history = calculate_metrics_on_all_dates(history)
        except:
            print(f'error in transform_history: check the Symbol {history["symbol"].iloc[0]}')
            return None
    return history