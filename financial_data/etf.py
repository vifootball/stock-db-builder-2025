import os
import time
import random
import requests
import pandas as pd
from typing import Optional
from datetime import datetime
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from utils.parse_str import *

def get_symbols() -> list:
    fname = "./downloads/etf_list/etf_list_240605_union.csv"
    etf_list = pd.read_csv(fname)
    symbols = sorted(etf_list['symbol'].to_list())
    return symbols


def fetch_etf_data(symbol: str) -> dict:
    url = f"https://api.stockanalysis.com/api/symbol/e/{symbol}/overview"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as e:
        print(f"[ERROR] {symbol} - {e}")
        return {}


def parse_etf_data(data: dict) -> pd.DataFrame:

    info_table = dict(data.get("infoTable", [])) #빈 값이어도 에러 안나도록 처리
    holdings_table = data.get("holdingsTable", {}) or {} #빈 값이어도 에러 안나도록 처리, 값 자체가 None으로 박혀있는 경우도 있어서 or {}으ㄹ 추가함
    
    etf_profile = {
        # Dimensions
        "symbol": info_table.get("Ticker Symbol"),
        "asset_class": info_table.get("Asset Class"),
        "category": info_table.get("Category"),
        "region": info_table.get("Region"),
        "stock_exchange": info_table.get("Stock Exchange"),
        "provider": info_table.get("Provider"),
        "index_tracked": info_table.get("Index Tracked"),
        
        # Measures
        "aum": str_to_int(data.get("aum", "").replace("$", "")),
        "nav": data.get("nav", "").replace("$", ""),
        "expense_ratio": percentage_to_float(data.get("expenseRatio")),
        "shares_outstanding": str_to_int(data.get("sharesOut")),
        "dividend_yield": percentage_to_float(data.get("dividendYield")),
        "inception_date": datetime.strptime(data.get("inception"), "%b %d, %Y").strftime("%Y-%m-%d") if data.get("inception") else None,
        "description": data.get("description"),

        # Holdings
        "holdings_count": data.get("holdings", None),
        "holdings_top10_percentage": holdings_table.get("top10") / 100 if holdings_table.get("top10") else None,
        "holdings_date": datetime.strptime(holdings_table.get("updated", ""), "%b %d, %Y").strftime("%Y-%m-%d") if holdings_table.get("updated", "") else None,
    }

    return pd.DataFrame([etf_profile])


def get_etf_holdings(symbol: str) -> Optional[pd.DataFrame]:

    try:
        url = f"https://api.stockanalysis.com/api/symbol/e/{symbol}/holdings"
        response = requests.get(url)
        response.raise_for_status()
        etf_data = response.json().get("data", {})

    except Exception as e:
        print(f"[ERROR] Failed to fetch holdings for {symbol}: {e}")
        return None

    holdings_raw = etf_data.get("holdings")
    if not holdings_raw:
        print(f"[WARNING] Empty holdings data for {symbol}")
        return None

    holdings = pd.DataFrame(holdings_raw)

    holdings.rename(columns={
        "s": "holding_symbol",
        "n": "name",
        "as": "weight",
        "sh": "shares"
    }, inplace=True)

    for col in ["holding_symbol", "shares"]:
        if col not in holdings.columns:
            holdings[col] = "n/a"

    holdings["symbol"] = symbol.upper()
    holdings["holding_symbol"] = holdings["holding_symbol"].str.replace(r"^[$#]", "", regex=True)
    holdings["weight"] = holdings["weight"].str.replace(",", "").apply(percentage_to_float)
    holdings["shares"] = holdings["shares"].str.replace(",", "")

    date_str = etf_data.get("date")
    as_of_date = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d") if date_str else None
    holdings["as_of_date"] = as_of_date

    holdings = holdings[[
        "symbol", "as_of_date", "no", "holding_symbol", "name", "weight", "shares"
    ]]

    return holdings

