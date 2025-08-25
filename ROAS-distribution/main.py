# coding: utf-8
from pathlib import Path
from scipy.stats import percentileofscore

import os
import pandas_gbq
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

CURRENT_DIR = Path(os.getcwd())
QUERIES_DIR = CURRENT_DIR / "queries"
BQ_PROJECT_ID = "dhh-ncr-stg"
MY_DATASET = "patrick_doupe"
MONTH = '2025-07-01'

if __name__=="__main__":

    with open(QUERIES_DIR / "data_collection.sql", 'r') as f:
        query = f.read()
    df = pandas_gbq.read_gbq(query.format(ANALYSIS_MONTH=MONTH))
    
    # ROAS is NaN for no revenue
    df_zero_revenue = df.loc[df.gmv_eur_direct == 0.0]
    df_pos_revenue = df.loc[df.gmv_eur_direct > 0]
    assert len(df_pos_revenue.loc[np.isnan(df_pos_revenue.roas)]) == 0, (
        "NaNs in Roas column where gmv is positive"
        )
    
    df_zero_revenue.groupby("management_entity")['campaign_id'].agg(lambda x: len(set(x)))
    output = (
            df_zero_revenue
            .groupby("management_entity")['campaign_id']
            .agg(lambda x: len(set(x)))
            .reset_index()
            .rename(columns={'campaign_id': 'number_zero_cpc_revenue_campaigns'})
            )
    pandas_gbq.to_gbq(output,
                      destination_table=f'{MY_DATASET}.zero_revenue_cpc_roas_2025_july',
                      project_id=BQ_PROJECT_ID, if_exists='replace')

    # What's the quantile for ROAS of 3?
    ROAS_BENCHMARK = 3.0
    roas_data = df_pos_revenue.roas.values
    three_quantile = percentileofscore(roas_data, 
                                       ROAS_BENCHMARK,
                                       kind='rank')
    print(f"The quantile for a ROAS of 3 is {three_quantile:.2f}")
    print(df_pos_revenue.roas.describe(
        percentiles=[0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        )
    )
    winsorise_value = 100
    
    # getting distribution
    sns.displot([min(x, winsorise_value) for x in roas_data], 
                bins=100,
                kind='hist')
    
    # Add title
    plt.title((f'Campaign level CPC ROAS. July 2025, all markets\n'
               f'Values capped at {winsorise_value}'),
              fontsize=16, 
              fontweight='bold')
    
    # Calculate quantiles
    q10 = np.percentile(roas_data, 10)
    q25 = np.percentile(roas_data, 25)
    q50 = np.percentile(roas_data, 50)
    q75 = np.percentile(roas_data, 75)
    q90 = np.percentile(roas_data, 90)
    
    # Add quantile information as text on the plot
    quantile_text = f'Quantiles:\n10%: {q10:.2f}\n25%: {q25:.2f}\n50%: {q50:.2f}\n75%: {q75:.2f}\n90%: {q90:.2f}'
    plt.text(0.98, 0.5, 
             quantile_text, 
             transform=plt.gca().transAxes,
             horizontalalignment='right',
             verticalalignment='center_baseline', 
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    plt.xlabel("Campaign level ROAS") 
    # Save the figure
    plt.savefig(CURRENT_DIR / "figures" / 'roas_distplot.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # save data
    pandas_gbq.to_gbq(df_pos_revenue,
                      destination_table=f'{MY_DATASET}.positive_revenue_cpc_roas_2025_july',
                      project_id=BQ_PROJECT_ID, if_exists='replace')

# EOF
