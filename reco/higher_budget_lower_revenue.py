# coding: utf-8
import pandas_gbq
import pandas as pd


def main():
    """
    Gets PEYA recommendations from July

    Take top two budget recommendations
        - looks to see if there is ever not an increasing net revenue with budget
          increase (which suffests we're moving beyond profit optimal)

    """
    query = """
        SELECT * 
        FROM dhh-ncr-stg.performance_estimation.PEYA_cpc_budget_recos
        WHERE model_name = 'cpc_budget_recos_v1.0_new'
    """

    df = pandas_gbq.read_gbq(query)
    df['net_revenue'] = df['e_cpc_gmv'] - df['reco_budget_lc']

    cols = ['global_entity_id', 'vendor_id', 'e_cpc_gmv', 'reco_budget_lc',
            'e_roas', 'net_revenue', 'reco_date']
    df_1 = df.loc[df.reco_num == 2]
    df_2 = df.loc[df.reco_num == 3]
    tmp = pd.merge(df_1[cols], df_2[cols], how='inner', on=['global_entity_id', 'vendor_id', 'reco_date'], suffixes=['_1', '_2'])
    
    tmp['lower_nr_1'] = (tmp.net_revenue_1 < tmp.net_revenue_2) & (tmp.net_revenue_1 > 0)
    tmp['lower_gmv_1'] = tmp.e_cpc_gmv_1 < tmp.e_cpc_gmv_2
    tmp['lower_budget_1'] = tmp.reco_budget_lc_1 < tmp.reco_budget_lc_2
    tmp['lower_roas_1'] = tmp.e_roas_1 < tmp.e_roas_2

    print(f"""The share of vendors (with recommendation > estimated gmv) that
          have a lower budget for reco 1: {tmp.lower_budget_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower NR for reco 1:
          {tmp.lower_nr_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower GMV for reco 1:
          {tmp.lower_gmv_1.mean():.2f}""")
    print(f"""The share of vendors that have a lower ROAS for reco 1:
          {tmp.lower_roas_1.mean():.2f}""")

if __name__=="__main__":

    main()

#EOF
