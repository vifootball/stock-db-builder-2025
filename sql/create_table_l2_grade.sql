CREATE TABLE IF NOT EXISTS l2_grade (

    symbol          TEXT,
    start_date      DATE,
    end_date        DATE,
    unit_period     TEXT,
    fund_age_day    REAL,

    fund_age_year       REAL,
    expense_ratio       REAL,
    nav                 REAL,
    shares_outstanding  REAL,
    aum                 REAL,

    total_return        REAL,
    cagr                REAL,
    std_yearly_return   REAL,
    drawdown_max        REAL,
    div_ttm             REAL,

    div_yield_ttm       REAL,
    div_paid_cnt_ttm    REAL,
    mkt_corr_daily      REAL,
    mkt_corr_weekly     REAL,
    mkt_corr_monthly    REAL,

    mkt_corr_yearly         REAL,
    volume_dollar_3m_avg    REAL
);