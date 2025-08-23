CREATE TABLE IF NOT EXISTS l2_correlation (
    -- pk 걸어줘야하나

    unit_period TEXT,
    symbol TEXT,
    target_symbol TEXT,
    start_date DATE,
    end_date DATE,
    
    corr_yearly REAL,
    corr_monthly REAL,
    corr_weekly REAL,
    corr_daily REAL,
    rank_yearly_asc REAL,
    
    rank_yearly_desc REAL,
    rank_monthly_asc REAL,
    rank_monthly_desc REAL,
    rank_weekly_asc REAL,
    rank_weekly_desc REAL,

    rank_daily_asc REAL,
    rank_daily_desc REAL
);
