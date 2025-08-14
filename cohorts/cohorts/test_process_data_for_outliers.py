import pytest
import pandas as pd
import numpy as np
from scipy.stats import f_oneway, kruskal

from .cohort_statistics import (
    calculate_anova_components,
    find_outliers_iqr,
    find_outliers_mean_multiple,
    process_dataframes_for_outliers 
)

@pytest.fixture
def sample_dataframes():
    """Provides a dictionary of mock DataFrames for testing."""
    df1 = pd.DataFrame({
        'entity_id': ['e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8', 'e9', 'e10'],
        'vendor_code': ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10'],
        'value': [1, 2, 3, 4, 100, 10, 11, 12, 13, 14],
        'cohort_id': ['A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'B', 'B']
    })

    df2 = pd.DataFrame({
        'entity_id': ['e11', 'e12', 'e13', 'e14', 'e15', 'e16', 'e17', 'e18', 'e19', 'e20'],
        'vendor_code': ['v11', 'v12', 'v13', 'v14', 'v15', 'v16', 'v17', 'v18', 'v19', 'v20'],
        'value': [50, 51, 52, 53, 54, 60, 61, 62, 63, 64],
        'cohort_id': ['X', 'X', 'X', 'X', 'X', 'Y', 'Y', 'Y', 'Y', 'Y']
    })

    df3_single_cohort = pd.DataFrame({
        'entity_id': ['e21', 'e22', 'e23', 'e24', 'e25'],
        'vendor_code': ['v21', 'v22', 'v23', 'v24', 'v25'],
        'value': [1, 2, 3, 4, 5],
        'cohort_id': ['Z', 'Z', 'Z', 'Z', 'Z']
    })

    df4_small_cohorts = pd.DataFrame({
        'entity_id': ['e26', 'e27', 'e28', 'e29'],
        'vendor_code': ['v26', 'v27', 'v28', 'v29'],
        'value': [1, 2, 3, 4],
        'cohort_id': ['P', 'P', 'Q', 'Q']
    })

    return {
        'df1_test': df1,
        'df2_test': df2,
        'df3_single_cohort_test': df3_single_cohort,
        'df4_small_cohorts_test': df4_small_cohorts
    }

@pytest.fixture
def value_column():
    return 'value'

@pytest.fixture
def group_column():
    return 'cohort_id'


def test_process_dataframes_for_outliers_df1(sample_dataframes, value_column, group_column):
    df1 = sample_dataframes['df1_test']

    # Expected values for df1_test
    # Cohort A: [1, 2, 3, 4, 100]
    #   Q1=2, Q3=4, IQR=2. LB=-4, UB=10. Outliers: [100] (1 outlier) -> iqr_outliers=1
    #   Mean=22. Threshold=110. Outliers: None -> mean_5x_outliers=0
    # Cohort B: [10, 11, 12, 13, 14]
    #   Q1=11, Q3=13, IQR=2. LB=5, UB=19. Outliers: None -> iqr_outliers=0
    #   Mean=12. Threshold=60. Outliers: None -> mean_5x_outliers=0

    # Total for df1_test:
    expected_iqr_total_outliers = 1
    expected_mean_5x_total_outliers = 0
    expected_overlap_total_vendors = 0
    expected_share_cohorts_with_outlier_IQR = 0.5 # 1 out of 2 cohorts (A has outlier)
    expected_share_cohorts_with_outlier_5x = 0.0 # 0 out of 2 cohorts

    # Calculate expected ANOVA and Kruskal-Wallis values for df1_test
    cohort_A = df1[df1['cohort_id'] == 'A']['value'].values
    cohort_B = df1[df1['cohort_id'] == 'B']['value'].values

    f_scipy_expected, _ = f_oneway(cohort_A, cohort_B)
    f_manual_expected, ms_b_manual_expected, ms_w_manual_expected = calculate_anova_components(cohort_A, cohort_B)
    kw_h_expected, kw_p_expected = kruskal(cohort_A, cohort_B)
    epsilon_squared_expected = kw_h_expected / (len(df1) + 1) # N + 1 for epsilon-squared

    results_df = process_dataframes_for_outliers(
        {'df1_test': df1},
        value_column,
        group_column
    )

    assert not results_df.empty
    assert results_df.iloc[0]['df_name'] == 'df1_test'
    assert results_df.iloc[0]['iqr_total_outliers'] == expected_iqr_total_outliers
    assert results_df.iloc[0]['mean_5x_total_outliers'] == expected_mean_5x_total_outliers
    assert results_df.iloc[0]['overlap_total_vendors'] == expected_overlap_total_vendors
    assert abs(results_df.iloc[0]['share_cohorts_with_outlier_IQR'] - expected_share_cohorts_with_outlier_IQR) < 1e-5
    assert abs(results_df.iloc[0]['share_cohorts_with_outlier_5x'] - expected_share_cohorts_with_outlier_5x) < 1e-5

    assert abs(results_df.iloc[0]['f_stat_scipy'] - f_scipy_expected) < 1e-5
    assert abs(results_df.iloc[0]['f_stat_manual'] - f_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['ms_b_manual'] - ms_b_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['ms_w_manual'] - ms_w_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['KW_H'] - kw_h_expected) < 1e-5
    assert abs(results_df.iloc[0]['p_value'] - kw_p_expected) < 1e-5
    assert abs(results_df.iloc[0]['epsilon_squared'] - epsilon_squared_expected) < 1e-5


def test_process_dataframes_for_outliers_df2(sample_dataframes, value_column, group_column):
    df2 = sample_dataframes['df2_test']
    # Expected values for df2_test (no outliers)
    expected_iqr_total_outliers = 0
    expected_mean_5x_total_outliers = 0
    expected_overlap_total_vendors = 0
    expected_share_cohorts_with_outlier_IQR = 0.0
    expected_share_cohorts_with_outlier_5x = 0.0

    # Calculate expected ANOVA and Kruskal-Wallis values for df2_test
    cohort_X = df2[df2['cohort_id'] == 'X']['value'].values
    cohort_Y = df2[df2['cohort_id'] == 'Y']['value'].values

    f_scipy_expected, _ = f_oneway(cohort_X, cohort_Y)
    f_manual_expected, ms_b_manual_expected, ms_w_manual_expected = calculate_anova_components(cohort_X, cohort_Y)
    kw_h_expected, kw_p_expected = kruskal(cohort_X, cohort_Y)
    epsilon_squared_expected = kw_h_expected / (len(df2) + 1)

    results_df = process_dataframes_for_outliers(
        {'df2_test': df2},
        value_column,
        group_column
    )

    assert not results_df.empty
    assert results_df.iloc[0]['df_name'] == 'df2_test'
    assert results_df.iloc[0]['iqr_total_outliers'] == expected_iqr_total_outliers
    assert results_df.iloc[0]['mean_5x_total_outliers'] == expected_mean_5x_total_outliers
    assert results_df.iloc[0]['overlap_total_vendors'] == expected_overlap_total_vendors
    assert abs(results_df.iloc[0]['share_cohorts_with_outlier_IQR'] - expected_share_cohorts_with_outlier_IQR) < 1e-5
    assert abs(results_df.iloc[0]['share_cohorts_with_outlier_5x'] - expected_share_cohorts_with_outlier_5x) < 1e-5

    assert abs(results_df.iloc[0]['f_stat_scipy'] - f_scipy_expected) < 1e-5
    assert abs(results_df.iloc[0]['f_stat_manual'] - f_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['ms_b_manual'] - ms_b_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['ms_w_manual'] - ms_w_manual_expected) < 1e-5
    assert abs(results_df.iloc[0]['KW_H'] - kw_h_expected) < 1e-5
    assert abs(results_df.iloc[0]['p_value'] - kw_p_expected) < 1e-5
    assert abs(results_df.iloc[0]['epsilon_squared'] - epsilon_squared_expected) < 1e-5


def test_process_dataframes_for_outliers_single_cohort(sample_dataframes, value_column, group_column):
    df3_single_cohort = sample_dataframes['df3_single_cohort_test']
    results_df = process_dataframes_for_outliers(
        {'df3_single_cohort_test': df3_single_cohort},
        value_column,
        group_column
    )

    assert not results_df.empty
    assert results_df.iloc[0]['df_name'] == 'df3_single_cohort_test'
    assert np.isnan(results_df.iloc[0]['f_stat_scipy'])
    assert np.isnan(results_df.iloc[0]['f_stat_manual'])
    assert np.isnan(results_df.iloc[0]['ms_b_manual'])
    assert np.isnan(results_df.iloc[0]['ms_w_manual'])
    assert np.isnan(results_df.iloc[0]['KW_H'])
    assert np.isnan(results_df.iloc[0]['p_value'])
    assert np.isnan(results_df.iloc[0]['epsilon_squared'])

    # Outlier calculation should still work for the single cohort
    cohort_Z = df3_single_cohort[df3_single_cohort['cohort_id'] == 'Z']['value']
    iqr_outliers_Z = find_outliers_iqr(cohort_Z, multiplier=3).sum()
    mean_5x_outliers_Z = find_outliers_mean_multiple(cohort_Z, multiplier=5).sum()

    assert results_df.iloc[0]['iqr_total_outliers'] == iqr_outliers_Z
    assert results_df.iloc[0]['mean_5x_total_outliers'] == mean_5x_outliers_Z
    # For a single cohort, share is 1.0 if outliers exist, 0.0 otherwise
    assert results_df.iloc[0]['share_cohorts_with_outlier_IQR'] == (1.0 if iqr_outliers_Z > 0 else 0.0)
    assert results_df.iloc[0]['share_cohorts_with_outlier_5x'] == (1.0 if mean_5x_outliers_Z > 0 else 0.0)


def test_process_dataframes_for_outliers_small_cohorts(sample_dataframes, value_column, group_column):
    df4_small_cohorts = sample_dataframes['df4_small_cohorts_test']
    results_df = process_dataframes_for_outliers(
        {'df4_small_cohorts_test': df4_small_cohorts},
        value_column,
        group_column
    )

    assert not results_df.empty
    assert results_df.iloc[0]['df_name'] == 'df4_small_cohorts_test'

    # All statistical measures should be NaN as no valid groups are found
    assert np.isnan(results_df.iloc[0]['f_stat_scipy'])
    assert np.isnan(results_df.iloc[0]['f_stat_manual'])
    assert np.isnan(results_df.iloc[0]['ms_b_manual'])
    assert np.isnan(results_df.iloc[0]['ms_w_manual'])
    assert np.isnan(results_df.iloc[0]['KW_H'])
    assert np.isnan(results_df.iloc[0]['p_value'])
    assert np.isnan(results_df.iloc[0]['epsilon_squared'])

    # Outlier counts should be 0 because compare_outlier_methods will return an empty DataFrame
    assert results_df.iloc[0]['iqr_total_outliers'] == 0
    assert results_df.iloc[0]['mean_5x_total_outliers'] == 0
    assert results_df.iloc[0]['overlap_total_vendors'] == 0
    assert results_df.iloc[0]['share_cohorts_with_outlier_IQR'] == 0.0
    assert results_df.iloc[0]['share_cohorts_with_outlier_5x'] == 0.0


def test_process_dataframes_for_outliers_multiple_dfs(sample_dataframes, value_column, group_column):
    results_df = process_dataframes_for_outliers(
        sample_dataframes, # Pass the full dictionary
        value_column,
        group_column
    )

    assert not results_df.empty
    assert len(results_df) == len(sample_dataframes)
    assert 'df1_test' in results_df['df_name'].values
    assert 'df2_test' in results_df['df_name'].values
    assert 'df3_single_cohort_test' in results_df['df_name'].values
    assert 'df4_small_cohorts_test' in results_df['df_name'].values

    # Basic check for df1_test row
    df1_row = results_df[results_df['df_name'] == 'df1_test'].iloc[0]
    assert df1_row['iqr_total_outliers'] == 1

    # Basic check for df3_single_cohort_test row
    df3_row = results_df[results_df['df_name'] == 'df3_single_cohort_test'].iloc[0]
    assert np.isnan(df3_row['f_stat_scipy']) # type: ignore