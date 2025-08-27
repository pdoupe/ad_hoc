"""

Calculating error metrics for MBR

"""
import pandas_gbq
import os

from pathlib import Path

CURRENT_DIR = Path(os.getcwd())
QUERIES_DIR = CURRENT_DIR / "queries"
OUTCOMES_MONTH = '2025-08-01'
RECOMMENDATIONS_MONTH = '2025-07-01'

assert RECOMMENDATIONS_MONTH < OUTCOMES_MONTH, (
        "Outcomes should happen after recommendations"
        )

if __name__=="__main__":

    with open(QUERIES_DIR / "mbr_metrics.sql", 'r') as f:
        query = f.read()
    
    df = pandas_gbq.read_gbq(query.format(OUTCOMES_MONTH=OUTCOMES_MONTH,
                                          RECOMMENDATIONS_MONTH=RECOMMENDATIONS_MONTH)
                             )
    print("Metrics calculated!\n")
    print(df)

#EOF
