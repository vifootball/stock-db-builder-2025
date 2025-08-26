from etl.L0.l0_etf_holdings import *
from etl.L0.l0_etf_holdings_chunk import *
from etl.L0.l0_etf_history import *
from etl.L0.l0_currency_history import *
from etl.L0.l0_indices_yahoo_history import *
from etl.L0.l0_indices_fred_history import *

# import etl.L1 as l1_runner
# import etl.L2 as l2_runner
# import etl.DM as dm_uploader

def main():
    # L0
    # run_l0_etf_holdings()           # 1주일에 1번 해도 될듯
    # run_l0_etf_holdings_chunk()     # 1주일에 1번 해도 될듯
    # run_l0_etf_history()
    # run_l0_currency_history()
    # run_l0_indices_yahoo_history()
    run_l0_indices_fred_history()
    
    # L1
    # l1_runner.run_l1_etf_history()
    # l1_runner.run_l1_etf_history_chunk()
    # l1_runner.run_l1_currency_history()
    # l1_runner.run_l1_currency_history_chunk()
    # l1_runner.run_l1_indices_yahoo_history()
    # l1_runner.run_l1_indices_yahoo_history_chunk()
    # l1_runner.run_l1_indices_fred_history()
    # l1_runner.run_l1_indices_fred_history_chunk()
    
    # L2
    # l2_runner.run_l2_grade()

    # DM
    # dm_uploader.upload_l0_etf_holdings_chunk(connection_name="mac", mode="replace")
    # dm_uploader.upload_l1_etf_history_chunk(connection_name="mac", mode="replace") # 첫번쨰만 replace, 나머지는 append
    # dm_uploader.upload_l1_currency_history_chunk(connection_name="mac", mode="append")
    # dm_uploader.upload_l1_indices_yahoo_history_chunk(connection_name="mac", mode="append")
    # dm_uploader.upload_l1_indices_fred_history_chunk(connection_name="mac", mode="append")
    # dm_uploader.upload_l2_grade(connection_name="mac", mode="replace")
    # dm_uploader.upload_l2_grade_pivot(connection_name="mac", mode="replace")

if __name__ == "__main__":
    main()


