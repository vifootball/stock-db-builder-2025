CREATE TABLE IF NOT EXISTS l1_history (

    -- TODO: indexing, partitioning (?)

    date DATE,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,

    close REAL,
    volume REAL,
    dividend REAL,
    stock_split REAL,
    price REAL,
    
    price_all_time_high REAL,
    drawdown_current REAL,
    drawdown_max REAL,
    volume_of_share REAL,
    volume_of_share_3m_avg REAL,
    
    volume_of_dollar REAL,
    volume_of_dollar_3m_avg REAL,
    dividend_paid_or_not REAL,
    dividend_paid_count_ttm REAL,
    dividend_ttm REAL,
    
    dividend_rate REAL,
    dividend_rate_ttm REAL,
    normal_day_tf REAL,
    date_id INT,
    transaction_id TEXT,
    
    price_change REAL,
    price_change_rate REAL,
    price_change_sign REAL,
    price_7d_ago REAL,
    weekly_price_change REAL,

    weekly_price_change_rate REAL,
    price_30d_ago REAL,
    monthly_price_change REAL,
    monthly_price_change_rate REAL,
    price_365d_ago REAL,

    yearly_price_change REAL,
    yearly_price_change_rate REAL
);