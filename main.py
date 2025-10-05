from etl.L0.l0_etf_holdings import *
from etl.L0.l0_etf_holdings_chunk import *
from etl.L0.l0_etf_history import *
from etl.L0.l0_currency_history import *
from etl.L0.l0_indices_yahoo_history import *
from etl.L0.l0_indices_fred_history import *
from etl.L1.l1_history import *
from etl.L1.l1_history_chunk import *
from etl.L2.l2_grade import *
from etl.DM.dm_upload import *

# import etl.L1 as l1_runner
# import etl.L2 as l2_runner
# import etl.DM as dm_uploader

def main():
    os.chdir('/Users/chungdongwook/dongwook-src/stock-db-builder-2025')

    # L0
    run_l0_etf_holdings()           # 1주일에 1번 해도 될듯
    run_l0_etf_holdings_chunk()     # 1주일에 1번 해도 될듯
    run_l0_etf_history()
    run_l0_currency_history()
    run_l0_indices_yahoo_history()
    run_l0_indices_fred_history()
    
    # L1
    run_l1_etf_history()
    run_l1_currency_history()
    run_l1_indices_yahoo_history()
    run_l1_indices_fred_history()
    
    run_l1_etf_history_chunk()
    run_l1_currency_history_chunk()
    run_l1_indices_yahoo_history_chunk()
    run_l1_indices_fred_history_chunk()
    
    # L2
    run_l2_grade()

    # DM
    upload_l0_etf_holdings_chunk(connection_name="mac", mode="replace")
    upload_l1_etf_history_chunk(connection_name="mac", mode="replace") # 첫번쨰만 replace, 나머지는 append
    upload_l1_currency_history_chunk(connection_name="mac", mode="append")
    upload_l1_indices_yahoo_history_chunk(connection_name="mac", mode="append")
    upload_l1_indices_fred_history_chunk(connection_name="mac", mode="append")
    upload_l2_grade(connection_name="mac", mode="replace")
    upload_l2_grade_pivot(connection_name="mac", mode="replace")

    upload_l0_etf_holdings_chunk(connection_name="win", mode="replace")
    upload_l1_etf_history_chunk(connection_name="win", mode="replace") # 첫번쨰만 replace, 나머지는 append
    upload_l1_currency_history_chunk(connection_name="win", mode="append")
    upload_l1_indices_yahoo_history_chunk(connection_name="win", mode="append")
    upload_l1_indices_fred_history_chunk(connection_name="win", mode="append")
    upload_l2_grade(connection_name="win", mode="replace")
    upload_l2_grade_pivot(connection_name="win", mode="replace")

if __name__ == "__main__":
    main()


