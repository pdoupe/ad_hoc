# coding: utf-8
import pandas_gbq
import pandas as pd
from pathlib import Path
import os


def main():
    """
    Gets PEYA recommendations from July

    Take top two budget recommendations
        - looks to see if there is ever not an increasing net revenue with budget
          increase (which suffests we're moving beyond profit optimal)

    """
    SQL_PATH = Path(os.getcwd()) / "queries" / "amc_recos.sql"
    with(SQL_PATH, 'r') as f:
        query = f.read()

    df = pandas_gbq.read_gbq(query)
    
    tmp['lower_nr_1'] = (tmp.net_revenue_1 < tmp.net_revenue_3) & (tmp.net_revenue_1 > 0)
    tmp['lower_gmv_1'] = tmp.e_cpc_gmv_1 < tmp.e_cpc_gmv_3
    tmp['lower_budget_1'] = tmp.reco_budget_lc_1 < tmp.reco_budget_lc_3
    tmp['lower_roas_1'] = tmp.e_roas_1 < tmp.e_roas_3

    print(f"""The share of vendors that have a lower budget for reco 1:
          {tmp.lower_budget_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower NR (but positive NR) for
          reco 1: {tmp.lower_nr_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower GMV for reco 1:
          {tmp.lower_gmv_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower ROAS for reco 1:
          {tmp.lower_roas_1.mean():.2f}""")

if __name__=="__main__":

    main()

#EOF
