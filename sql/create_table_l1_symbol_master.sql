CREATE TABLE IF NOT EXISTS l1_symbol_master (

    -- 공통
    domain          TEXT,
    symbol          TEXT,
    name            TEXT,

    -- ETF Only (대부분)
    asset_class     TEXT,
    category        TEXT,
    region          TEXT,
    stock_exchange  TEXT,
    provider        TEXT,
    index_tracked   TEXT,

    aum                 REAL,
    nav                 REAL,
    expense_ratio       REAL,
    shares_outstanding  REAL,
    dividend_yield      REAL,
    inception_date      DATE,
    description         TEXT,
    
    holdings_count              REAL,
    holdings_top10_percentage   REAL,
    holdings_date               DATE

);