CREATE TABLE IF NOT EXISTS l2_grade_pivot (

    symbol          TEXT,
    start_date      DATE,
    end_date        DATE,
    unit_period     TEXT,
    var_name        TEXT,

    value           REAL
);