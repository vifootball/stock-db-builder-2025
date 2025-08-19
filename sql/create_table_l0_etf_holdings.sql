CREATE TABLE IF NOT EXISTS l0_etf_holdings (

    symbol          TEXT,
    as_of_date      DATE,
    no              REAL,
    holding_symbol  TEXT,
    name            TEXT,

    weight          REAL,
    shares          REAL
);