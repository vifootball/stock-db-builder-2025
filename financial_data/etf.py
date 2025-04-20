import os
import time
import random
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from utils.parse_str import *

def get_symbols() -> list:
    fname = "./manual_data/etf_list/etf_list_240605_union.csv"
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

