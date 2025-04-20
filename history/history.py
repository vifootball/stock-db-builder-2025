from typing import Union, Optional
import pandas as pd
import yfinance as yf
import datetime as dt
from pandas_datareader.data import DataReader # FRED Data


def get_history_from_yf(symbol: str) -> Optional[pd.DataFrame]:
    # COLUMNS: "symbol", "date", "open", "high", "low", "close", "volume", "dividend", "stock_split"

    history = yf.Ticker(symbol).history(period="10000mo")

    # if history.empty:
        # history = ticker.history(period="max")

    # 데이터 부족 여부 판단
    # 데이터가 어느정도 있으면서 최근까지 업데이트 되는 종목만 수집하고자 함 
    # 정상데이터가 아니라면, 데이터가 아예 없거나 과거 데이터만 있음
    if history.empty or len(history) <= 50:
        return None  

    history = history.reset_index(drop=False)

    # 마지막 거래일이 얼마나 오래되었는지 확인
    # 데이터가 어느정도 있으면서 최근까지 업데이트 되는 종목만 수집하고자 함 
    # 정상데이터가 아니라면, 데이터가 아예 없거나 과거 데이터만 있음
    last_date = history["Date"].max().replace(tzinfo=None)
    if (dt.datetime.today() - last_date).days > 50:
        return None  # 최근 거래 기록이 없음

    history["symbol"] = symbol.upper()
    history["Date"] = history["Date"].dt.strftime("%Y-%m-%d")
    history = history.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Dividends": "dividend",
        "Stock Splits": "stock_split",
        "Capital Gains": "capital_gain"
    })

    history = history[[
        "symbol", "date", "open", "high", "low", "close", "volume", "dividend", "stock_split"
    ]]

    return history


def get_history_from_fred(symbol: str) -> Optional[pd.DataFrame]:
    # COLUMNS: "symbol", "date", "close"

    try:
        start = dt.datetime(1800, 1, 1)
        end = dt.datetime.today()

        df = DataReader(symbol, "fred", start, end).reset_index()
        df.rename(columns={symbol: "close", "DATE": "date"}, inplace=True)
        df["symbol"] = symbol.upper()

        history = df[["symbol", "date", "close"]]
        return history

    except Exception as e:
        print(f"[ERROR] Failed to fetch FRED data for {symbol}: {e}")
        return None