# test_cohort_statistics.py

from typing import Literal
from unittest.mock import patch, MagicMock
import pytest
import json
import numpy as np
import pandas as pd
from scipy.stats import f_oneway
from pandas import DataFrame
import pandas_gbq


# Import all functions from your cohort_statistics.py file
from .cohort_statistics import (
    get_groups,
    find_outliers_iqr,
    find_outliers_mean_multiple,
    compare_outlier_methods,
    calculate_anova_components,
    get_f_stat_components,
    process_dataframes,
    get_top_cohort_items,
    load_dataframes_by_type
)
from .config_manager import TableConfig
from pytest import CaptureFixture


### Test get_groups ###
def test_get_groups_basic():
    df = pd.DataFrame({
        'performance': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'cohort': ['A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'B', 'B']
    })
    groups = get_groups(df, 'performance', 'cohort')
    assert len(groups) == 2
    np.testing.assert_array_equal(groups[0], np.array([1, 2, 3, 4, 5]))
    np.testing.assert_array_equal(groups[1], np.array([6, 7, 8, 9, 10]))

def test_get_groups_filter_small_cohorts():
    df = pd.DataFrame({
        'performance': [1, 2, 3, 4, 5, 6, 7, 8],
        'cohort': ['A', 'A', 'A', 'A', 'A', 'B', 'B', 'B'] # Cohort B < 5
    })
    groups = get_groups(df, 'performance', 'cohort')
    assert len(groups) == 1
    np.testing.assert_array_equal(groups[0], np.array([1, 2, 3, 4, 5]))

def test_get_groups_no_valid_cohorts():
    df = pd.DataFrame({
        'performance': [1, 2, 3, 4],
        'cohort': ['A', 'A', 'A', 'A']
    })
    groups = get_groups(df, 'performance', 'cohort')
    assert len(groups) == 0

### Test find_outliers_iqr ###
def test_find_outliers_iqr_basic():
    data = pd.Series([1, 2, 3, 4, 5, 100]) # 100 should be an outlier
    outliers = find_outliers_iqr(data, multiplier=1.5)
    np.testing.assert_array_equal(outliers, np.array([False, False, False, False, False, True]))

    data_with_low_outlier = pd.Series([1, 2, 3, 4, 5, -50]) # -50 should be an outlier
    outliers_low = find_outliers_iqr(data_with_low_outlier, multiplier=1.5)
    np.testing.assert_array_equal(outliers_low, np.array([False, False, False, False, False, True]))

def test_find_outliers_iqr_no_outliers():
    data = pd.Series([10, 11, 12, 13, 14, 15])
    outliers = find_outliers_iqr(data, multiplier=1.5)
    np.testing.assert_array_equal(outliers, np.array([False, False, False, False, False, False]))

### Test find_outliers_mean_multiple ###
def test_find_outliers_mean_multiple_basic():
    data = pd.Series([1, 2, 3, 4, 100]) # Mean is 22. Some are less than 5*mean
    outliers = find_outliers_mean_multiple(data, multiplier=5)
    np.testing.assert_array_equal(outliers, np.array([False, False, False, False, False]))

def test_find_outliers_mean_multiple_basic_two():
    data = pd.Series([1, 1, 2, 3, 4, 150]) # Mean is 134. Some are less than 5*mean
    outliers = find_outliers_mean_multiple(data, multiplier=5)
    np.testing.assert_array_equal(outliers, np.array([False, False, False, False, False, True]))

def test_find_outliers_mean_multiple_zero_mean():
    data = pd.Series([0, 0, 0, 0, 0])
    outliers = find_outliers_mean_multiple(data, multiplier=5)
    np.testing.assert_array_equal(outliers, np.array([False, False, False, False, False]))

def test_compare_outlier_methods_small_cohorts_skipped():
    df = pd.DataFrame({
        'performance': [1, 2, 3, 4, 5, 6, 7, 8],
        'cohort': ['A', 'A', 'A', 'A', 'A', 'B', 'B', 'B'] # B is < 5
    })
    results_df = compare_outlier_methods(df, 'performance', 'cohort')
    assert len(results_df) == 1 # Only cohort A should be present
    assert results_df['cohort'].iloc[0] == 'A'


def test_calculate_anova_components_basic():
    group1 = np.array([10, 12, 11, 13])
    group2 = np.array([15, 17, 16, 14])
    group3 = np.array([8, 9, 7, 10])

    f_stat, ms_b, ms_w = calculate_anova_components(group1, group2, group3)

    expected_f_stat = 148 / 5 # 29.6
    expected_ms_b = 148 / 3   # 49.333...
    expected_ms_w = 5 / 3     # 1.666...

    assert f_stat == pytest.approx(expected_f_stat)
    assert ms_b == pytest.approx(expected_ms_b)
    assert ms_w == pytest.approx(expected_ms_w)

def test_calculate_anova_components_single_group_raises_error():
    with pytest.raises(ValueError, match="Requires at least two groups"):
        calculate_anova_components(np.array([1, 2, 3, 4, 5]))

def test_calculate_anova_components_insufficient_data_raises_error():
    with pytest.raises(ValueError, match="Insufficient data"):
        calculate_anova_components(np.array([1]), np.array([2]))

def test_calculate_anova_components_compare_scipy():
    group1 = np.array([10, 12, 11, 13])
    group2 = np.array([15, 17, 16, 14])
    group3 = np.array([8, 9, 7, 10])

    f_stat_manual, _, _ = calculate_anova_components(group1, group2, group3)
    f_stat_scipy, _ = f_oneway(group1, group2, group3)

    assert f_stat_manual == pytest.approx(f_stat_scipy, rel=1e-6)


def test_get_f_stat_components_less_than_two_cohorts():
    data = {
        'performance': [10, 12, 11, 13],
        'cohort_id': ['A', 'A', 'A', 'A']
    }
    df = pd.DataFrame(data)

    results = get_f_stat_components(df, 'performance', 'cohort_id')

    assert isinstance(results, dict)
    assert np.isnan(results['f_scipy'])
    assert np.isnan(results['f_manual'])
    assert np.isnan(results['ms_b_manual'])
    assert np.isnan(results['ms_w_manual'])


def test_process_dataframes_basic():
    original_df = pd.DataFrame({
        'entity_id': [1, 2, 3],
        'vendor_code': ['A', 'B', 'C'],
        'other_col': ['x', 'y', 'z']
    })
    
    df_to_process_1 = pd.DataFrame({
        'entity_id': [1, 4], # 4 will be unmatched
        'vendor_code': ['A', 'D'],
        'gmv': [100.0, 0.0],
        'some_data': [10, 20]
    })
    df_to_process_2 = pd.DataFrame({
        'entity_id': [2, 5], # 5 will be unmatched
        'vendor_code': ['B', 'E'],
        'gmv': [200.0, 0.0],
        'another_data': [30, 40]
    })

    dataframes_dict = {
        'test_df_1': df_to_process_1,
        'test_df_2': df_to_process_2
    }

    processed_dfs = process_dataframes(original_df, dataframes_dict)

    assert isinstance(processed_dfs, dict)
    assert 'original' in processed_dfs
    assert 'test_df_1' in processed_dfs
    assert 'test_df_2' in processed_dfs

    # Check 'current' df (should be unchanged)
    pd.testing.assert_frame_equal(processed_dfs['original'], original_df)

    # Check 'test_df_1'
    df1_processed = processed_dfs['test_df_1']
    print(df1_processed.head())
    assert df1_processed['gmv'].iloc[0] == pytest.approx(100.0) # Matched (1,A)
    assert df1_processed['gmv'].iloc[1] == pytest.approx(0.0)   # Unmatched (4,D) -> filled with 0
    assert df1_processed['_merge'].iloc[0] == 'both'
    assert df1_processed['_merge'].iloc[1] == 'left_only'

    # Check 'test_df_2'
    df2_processed = processed_dfs['test_df_2']
    assert df2_processed['gmv'].iloc[0] == pytest.approx(200.0) # Matched (2,B)
    assert df2_processed['gmv'].iloc[1] == pytest.approx(0.0)   # Unmatched (5,E) -> filled with 0
    assert df2_processed['_merge'].iloc[0] == 'both'
    assert df2_processed['_merge'].iloc[1] == 'left_only'

def test_process_dataframes_gmv_contains_non_numeric():
    original_df = pd.DataFrame({
        'entity_id': [1, 2],
        'vendor_code': ['A', 'B'],
        'other_col': ['x', 'y']
    })
    
    df_to_process_1 = pd.DataFrame({
        'entity_id': [1, 2],
        'vendor_code': ['A', 'B'],
        'gmv': [100.0, 'invalid_gmv'], # Invalid GMV
        'another_col': [10, 20]
    })

    processed_dfs = process_dataframes(original_df, {'test_df_1': df_to_process_1})
    df1_processed = processed_dfs['test_df_1']
    print(df1_processed)
    assert df1_processed['gmv'].iloc[0] == pytest.approx(100.0)
    assert df1_processed['gmv'].iloc[1] == pytest.approx(0.0) # 'invalid_gmv' becomes NaN, then 0

### Test get_top_cohort_items ###
def test_get_top_cohort_items_basic():
    df = pd.DataFrame({
        'cohort_id': [1, 1, 2, 2, 3, 3, 1, 2, 3, 4, 4, 4],
        'cohort_features': [
            '{"feat": "A"}', '{"feat": "A"}',
            '{"feat": "B"}', '{"feat": "B"}',
            '{"feat": "C"}', '{"feat": "C"}',
            '{"feat": "A"}', # Another for A
            '{"feat": "B"}', # Another for B
            '{"feat": "C"}', # Another for C
            '{"feat": "D"}', '{"feat": "D"}', '{"feat": "D"}'
        ],
        'vendor_code': ['V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12']
    })
    
    # Cohort 1-A: V1, V2, V7 (3 unique)
    # Cohort 2-B: V3, V4, V8 (3 unique)
    # Cohort 3-C: V5, V6, V9 (3 unique)
    # Cohort 4-D: V10, V11, V12 (3 unique)
    
    # Let's adjust to make unique counts different for sorting
    df_mixed_vendors = pd.DataFrame({
        'cohort_id': [1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4],
        'cohort_features': [
            '{"feat": "A"}', '{"feat": "A"}', '{"feat": "A"}', # 3 vendors
            '{"feat": "B"}', '{"feat": "B"}',                   # 2 vendors
            '{"feat": "C"}', '{"feat": "C"}', '{"feat": "C"}', '{"feat": "C"}', # 4 vendors
            '{"feat": "D"}', '{"feat": "D"}'                    # 2 vendors
        ],
        'vendor_code': ['V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11']
    })

    top_items = get_top_cohort_items(df_mixed_vendors, n=2)
    
    # Expected order: C (4), A (3), then B (2) or D (2)
    # The .items() gives an ItemsView, so convert to list for predictable order
    top_items_list = list(top_items) 
    
    assert len(top_items_list) == 2
    
    # Check top item
    assert top_items_list[0][0][0] == 3 # cohort_id 3
    assert json.loads(top_items_list[0][0][1]) == {"feat": "C"}
    assert top_items_list[0][1] == 4 # 4 unique vendors

    # Check second item
    assert top_items_list[1][0][0] == 1 # cohort_id 1
    assert json.loads(top_items_list[1][0][1]) == {"feat": "A"}
    assert top_items_list[1][1] == 3 # 3 unique vendors

def test_get_top_cohort_items_less_than_n():
    df = pd.DataFrame({
        'cohort_id': [1, 1],
        'cohort_features': ['{"feat": "A"}', '{"feat": "A"}'],
        'vendor_code': ['V1', 'V2']
    })
    # Only one cohort after groupby, with 2 unique vendors
    top_items = get_top_cohort_items(df, n=5)
    top_items_list = list(top_items)
    assert len(top_items_list) == 1
    assert top_items_list[0][1] == 2

## test data loading

@pytest.fixture
def mock_table_config():
    """
    Provides a TableConfig instance with simplified, controlled data for testing.
    This mock config allows us to predict what paths should be generated.
    """
    config_instance = TableConfig()
    # We directly set the _config attribute to control its behavior precisely for tests.
    # This bypasses the actual __post_init__ which loads the large default config.
    object.__setattr__(config_instance, '_config', {
        "Test Cat A": {
            "even": {
                "test_type_X": {
                    "current": "bq_path_A_even_X_current",
                    "original": "bq_path_A_even_X_original"
                },
                "test_type_Y": {
                    "current": "bq_path_A_even_Y_current"
                }
            },
            "uneven": {
                "test_type_X": {
                    "current": "bq_path_A_uneven_X_current"
                }
            }
        },
        "Test Cat B": {
            "even": {
                "test_type_Z": {
                    "current": "bq_path_B_even_Z_current"
                }
            }
        }
    })
    return config_instance

@pytest.fixture
def mock_base_sql_query_template():
    """Provides a generic base SQL query template for testing."""
    return "SELECT * FROM `{table_path}` LIMIT 1"

@pytest.fixture
def mock_project_id():
    """Provides a dummy project ID for testing."""
    return "mock-gcp-project"


def test_load_dataframes_successful_loading(mocker, mock_table_config: TableConfig, mock_base_sql_query_template: Literal['SELECT * FROM `{table_path}` LIMIT 1'], mock_project_id: Literal['mock-gcp-project']):
    """
    Tests successful loading of DataFrames for an existing data type.
    Mocks pandas_gbq.read_gbq to return consistent dummy DataFrames.
    """
    # Create a simple DataFrame that pandas_gbq.read_gbq will return
    mock_df_content = pd.DataFrame({'mock_col': [1, 2], 'data': ['A', 'B']})
    mocker.patch('pandas_gbq.read_gbq', return_value=mock_df_content)

    specific_data_type = "test_type_X" # This type has 3 paths in our mock config
    loaded_dfs = load_dataframes_by_type(specific_data_type, mock_base_sql_query_template, mock_table_config, mock_project_id)

    # Define the expected keys based on our mock_table_config for "test_type_X"
    expected_keys = {
        "Test Cat A-even-test_type_X-current",
        "Test Cat A-even-test_type_X-original",
        "Test Cat A-uneven-test_type_X-current"
    }

    # Assertions
    assert isinstance(loaded_dfs, dict)
    assert len(loaded_dfs) == 3 # We expect 3 DataFrames to be loaded for "test_type_X"
    assert set(loaded_dfs.keys()) == expected_keys # Check if all expected keys are present

    # Verify each value is a DataFrame and has the expected content
    for key, df in loaded_dfs.items():
        assert isinstance(df, pd.DataFrame)
        pd.testing.assert_frame_equal(df, mock_df_content) # Ensure the content is exactly our mock_df


def test_load_dataframes_non_existent_data_type(mocker, mock_table_config: TableConfig, mock_base_sql_query_template: Literal['SELECT * FROM `{table_path}` LIMIT 1'], mock_project_id: Literal['mock-gcp-project']):
    """
    Tests that an empty dictionary is returned when specific_data_type is not found in the config.
    """
    mocker.patch('pandas_gbq.read_gbq', return_value=pd.DataFrame()) # Mock it just in case

    specific_data_type = "non_existent_data_type"
    loaded_dfs = load_dataframes_by_type(specific_data_type, mock_base_sql_query_template, mock_table_config, mock_project_id)

    assert isinstance(loaded_dfs, dict)
    assert len(loaded_dfs) == 0 # Should return an empty dictionary

# --- Test Cases for load_dataframes_by_type ---


def test_load_dataframes_handles_pandas_gbq_exception(mocker, mock_table_config: TableConfig, mock_base_sql_query_template: Literal['SELECT * FROM `{table_path}` LIMIT 1'], mock_project_id: Literal['mock-gcp-project'], capsys: CaptureFixture[str]):
    """
    Tests that the function handles exceptions raised by pandas_gbq.read_gbq gracefully.
    It should return successfully loaded DFs and print error messages for failures.
    """
    # Create a mock DataFrame for successful loads
    mock_df_ok = pd.DataFrame({'ok_col': [1]})
    mock_exception = Exception("Simulated BigQuery connection error")

    # Use side_effect to define different behaviors for consecutive calls to read_gbq
    # Based on the iteration order of our mock_table_config for "test_type_X":
    # 1st call: Test Cat A-even-test_type_X-current -> should raise exception
    # 2nd call: Test Cat A-even-test_type_X-original -> should succeed
    # 3rd call: Test Cat A-uneven-test_type_X-current -> should succeed
    mocker.patch('pandas_gbq.read_gbq', side_effect=[
        mock_exception, # First call will fail
        mock_df_ok,     # Second call will succeed
        mock_df_ok      # Third call will succeed
    ])

    specific_data_type = "test_type_X" # This type has 3 paths in mock config
    loaded_dfs = load_dataframes_by_type(specific_data_type, mock_base_sql_query_template, mock_table_config, mock_project_id)

    # Assertions
    assert isinstance(loaded_dfs, dict)
    assert len(loaded_dfs) == 2 # One path should have failed, two should have succeeded

    # Verify that the keys corresponding to successful loads are present
    assert "Test Cat A-even-test_type_X-original" in loaded_dfs.keys()
    assert "Test Cat A-uneven-test_type_X-current" in loaded_dfs.keys()
    # Verify that the key for the failed load is NOT present
    assert "Test Cat A-even-test_type_X-current" not in loaded_dfs.keys()

    # Capture stdout/stderr to check if error messages were printed
    captured = capsys.readouterr()
    assert "Simulated BigQuery connection error" in captured.out
    assert "Test Cat A-even-test_type_X-current" in captured.out
