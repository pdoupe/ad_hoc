# a collection of helper functions for cohort statistics

import json
import warnings
from collections.abc import ItemsView
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from pandas import DataFrame, Series
from scipy.stats import f_oneway, kruskal


def get_groups(df: DataFrame, 
               performance_col: str, 
               cohort_col: str) -> List[np.ndarray]:
    """
      group data by cohorts for use in downstream functions
    """
    cohort_groups = [group[performance_col].values 
                    for name, group in df.groupby(cohort_col) 
                    if len(group) >= 5] 
    return cohort_groups

## IQR method
def find_outliers_iqr(data: np.ndarray, multiplier: int =3) -> Series:
    """
    Standard: outliers beyond Q1 - 1.5*IQR or Q3 + 1.5*IQR
    Conservative: multiplier = 1.5
    Liberal: multiplier = 3.0
    """
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    outliers = (data < lower_bound) | (data > upper_bound)
    return outliers


def find_outliers_mean_multiple(data: np.ndarray, multiplier: int=5) -> Series:
    """
    Flag values that are X times larger than the mean
    multiplier=5 means values > 5 * mean are outliers
    """
    mean_value = data.mean()
    threshold = multiplier * mean_value
    outliers = data > threshold
    return outliers



def compare_outlier_methods(df: DataFrame, performance_col: str, cohort_col: str) -> DataFrame:
    """
    Compare different outlier detection methods
    """
    results = []
    
    for cohort in df[cohort_col].unique():
        cohort_data = df[df[cohort_col] == cohort][performance_col]
        
        if len(cohort_data) >= 5:  # Minimum size for meaningful comparison
            # Method 1: IQR (1.5x)
            iqr_outliers = find_outliers_iqr(cohort_data, multiplier=3)
            
            # Method 2: 5x Mean
            mean_outliers = find_outliers_mean_multiple(cohort_data, multiplier=5)
            
            # Method 3: MAD  
            # mad_outliers = find_outliers_mad(cohort_data, threshold=3.5)
            
            results.append({
                'cohort': cohort,
                'cohort_size': len(cohort_data),
                'mean': cohort_data.mean(),
                'median': cohort_data.median(),
                'iqr_outliers': iqr_outliers.sum(),
                'mean_5x_outliers': mean_outliers.sum(),
                # 'mad_outliers': mad_outliers.sum(),
                'iqr_pct': (iqr_outliers.sum() / len(cohort_data)) * 100,
                'mean_5x_pct': (mean_outliers.sum() / len(cohort_data)) * 100,
                'overlap_iqr_mean': (iqr_outliers & mean_outliers).sum()
            })
    
    return DataFrame(results)



def calculate_anova_components(*samples: np.ndarray) -> Tuple[float, float, float]:
    """
    Calculates the F-statistic, Mean Square Between (numerator), and
    Mean Square Within (denominator) for a one-way ANOVA.

    This function manually computes the components of the F-statistic, providing
    insight into the variance between and within groups.

    Args:
        *samples: Variable number of NumPy arrays, where each array represents
                  a group's data. All arrays are expected to contain numeric values.

    Returns:
        tuple: A tuple containing:
            - f_statistic (float): The calculated F-statistic.
            - ms_between (float): The Mean Square Between (numerator).
            - ms_within (float): The Mean Square Within (denominator).

    Raises:
        ValueError: If there's only one group (cannot calculate MS_between)
                    or insufficient data (total observations equals number of groups).
    """
    # Combine all sample data to calculate the grand mean
    all_observations = np.concatenate(samples)
    grand_mean = np.mean(all_observations)

    # Number of groups (k) and total number of observations (N)
    k = len(samples)
    N = len(all_observations)

    # Calculate Sum of Squares Between (SS_B): Variance between group means and the grand mean
    ss_between = sum(len(group) * (np.mean(group) - grand_mean)**2 for group in samples)

    # Calculate Sum of Squares Within (SS_W): Variance within each group around its own mean
    ss_within = sum(np.sum((group - np.mean(group))**2) for group in samples)

    # Calculate Degrees of Freedom
    df_between = k - 1
    df_within = N - k

    # Validate degrees of freedom to prevent division by zero or nonsensical results
    if df_between <= 0: # Changed from == 0 to <= 0 for robustness
        raise ValueError("Cannot calculate MS_between: Requires at least two groups.")
    if df_within <= 0:  # Changed from == 0 to <= 0 for robustness
        raise ValueError("Cannot calculate MS_within: Insufficient data (total observations must exceed number of groups).")

    # Calculate Mean Square Between (MS_B) and Mean Square Within (MS_W)
    ms_between = ss_between / df_between
    ms_within = ss_within / df_within

    # Calculate the F-statistic
    f_statistic = ms_between / ms_within

    return f_statistic, ms_between, ms_within




# Assuming calculate_anova_components is defined in the same file or imported

def get_f_stat_components(df: DataFrame, performance_col: str, cohort_col: str) -> Dict[str, float]:
    """
    Calculates various F-statistic components for cohort-based performance data.

    This function groups data by cohorts, filters out small cohorts, and then
    computes the F-statistic using both scipy's f_oneway and a manual ANOVA
    component calculation.

    Args:
        df (DataFrame): The input DataFrame containing performance and cohort data.
        performance_col (str): The name of the column containing performance metrics
                                (expected to be numeric).
        cohort_col (str): The name of the column identifying different cohorts.

    Returns:
        dict: A dictionary containing the F-statistic from scipy, manual F-statistic,
              and manual Mean Square Between (MS_B) and Mean Square Within (MS_W)
              components. Returns a dictionary with all values as np.nan if
              fewer than two valid cohorts are found.
    """
    # Extract performance data for each cohort, skipping cohorts with fewer than 5 observations.
    cohort_groups = get_groups(df, performance_col, cohort_col)

    # If there are fewer than two valid cohorts, ANOVA components cannot be computed meaningfully.
    if len(cohort_groups) < 2:
        return {
            'f_scipy': np.nan,
            'f_manual': np.nan,
            'ms_b_manual': np.nan,
            'ms_w_manual': np.nan
        }

    # Calculate F-statistic using SciPy's built-in function
    f_stat_scipy, _ = f_oneway(*cohort_groups)

    # Calculate ANOVA components manually for deeper insight
    try:
        f_manual, ms_b_manual, ms_w_manual = calculate_anova_components(*cohort_groups)
    except ValueError:
        # Handle cases where manual calculation might fail due to specific data edge cases
        return {
            'f_scipy': f_stat_scipy, # Still return scipy's result if manual fails
            'f_manual': np.nan,
            'ms_b_manual': np.nan,
            'ms_w_manual': np.nan
        }


    return {
        'f_scipy': f_stat_scipy,
        'f_manual': f_manual,
        'ms_b_manual': ms_b_manual,
        'ms_w_manual': ms_w_manual
    }



def process_dataframes_for_outliers(
    dataframes_dict: Dict[str, DataFrame],
    value_column: str,
    group_column: str
    ) -> DataFrame:
    """
    Loops through multiple DataFrames, performs outlier analysis, and compiles
    results into a single summary DataFrame.

    For each DataFrame provided:
    - Compares outlier detection methods (IQR and mean-based).
    - Calculates Kruskal-Wallis H-statistic and epsilon-squared for cohort separation.
    - Determines F-statistic components (scipy and manual calculation).

    Args:
        dataframes_dict (Dict[str, pd.DataFrame]): A dictionary where keys are
                                                    DataFrame names (strings)
                                                    and values are the DataFrames themselves.
        value_column (str): The name of the column containing values for outlier detection
                            and performance analysis.
        group_column (str): The name of the column used for grouping (e.g., 'cohort_id').

    Returns:
        pd.DataFrame: A DataFrame containing summary statistics for each input DataFrame,
                      including outlier counts, shares, ANOVA components, and Kruskal-Wallis
                      results. Each row represents one input DataFrame.
    """
    # Initialize an empty list to store results for each DataFrame
    all_results: List[Dict[str, Union[str, float]]] = []

    # Define the columns for the results DataFrame explicitly for clarity and type consistency
    result_columns = [
        'df_name',
        'share_cohorts_with_outlier_IQR',
        'share_cohorts_with_outlier_5x',
        'iqr_total_outliers',
        'mean_5x_total_outliers',
        'overlap_total_vendors',
        'KW_H',
        'epsilon_squared',
        'p_value',
        'f_stat_scipy',
        'f_stat_manual',
        'ms_b_manual',
        'ms_w_manual'
    ]

    for df_name, df_tmp in dataframes_dict.items():
        print(f"Processing DataFrame: {df_name}...")

        # Perform outlier comparison
        outlier_comparison = compare_outlier_methods(df_tmp, value_column, group_column)
        # Get F-statistic components
        f_stat_components = get_f_stat_components(df_tmp, value_column, group_column)

        cohort_groups = get_groups(df_tmp, value_column, group_column)
        if len(cohort_groups) < 2:
            kw_h_stat, kw_p_value, kw_eps_sq = np.nan, np.nan, np.nan
        else:
            kw_h_stat, kw_p_value = kruskal(*cohort_groups)
            kw_eps_sq = kw_h_stat / (df_tmp.shape[0] + 1)

        # Calculate summary statistics based on outlier comparison results
        if not outlier_comparison.empty:
            share_cohorts_with_outlier_IQR = (outlier_comparison['iqr_outliers'] > 0).mean()
            share_cohorts_with_outlier_5x = (outlier_comparison['mean_5x_outliers'] > 0).mean()
            iqr_total_outliers = outlier_comparison['iqr_outliers'].sum()
            mean_5x_total_outliers = outlier_comparison['mean_5x_outliers'].sum()
            overlap_total_vendors = outlier_comparison['overlap_iqr_mean'].sum()
        else:
            share_cohorts_with_outlier_IQR = 0.0
            share_cohorts_with_outlier_5x = 0.0
            iqr_total_outliers = 0
            mean_5x_total_outliers = 0
            overlap_total_vendors = 0

        # Compile results for the current DataFrame
        current_df_results = {
            'df_name': df_name,
            'share_cohorts_with_outlier_IQR': share_cohorts_with_outlier_IQR,
            'share_cohorts_with_outlier_5x': share_cohorts_with_outlier_5x,
            'iqr_total_outliers': iqr_total_outliers,
            'mean_5x_total_outliers': mean_5x_total_outliers,
            'overlap_total_vendors': overlap_total_vendors,
            'KW_H': kw_h_stat,
            'epsilon_squared': kw_eps_sq,
            'p_value': kw_p_value,
            'f_stat_scipy': f_stat_components['f_scipy'],
            'f_stat_manual': f_stat_components['f_manual'], # Removed /1e6 if the actual values are not this large
            'ms_b_manual': f_stat_components['ms_b_manual'], # Removed /1e6
            'ms_w_manual': f_stat_components['ms_w_manual']  # Removed /1e6
        }
        all_results.append(current_df_results)

    # Convert the list of results dictionaries into a single DataFrame
    results_df = DataFrame(all_results, columns=result_columns)

    return results_df


def process_dataframes_with_gmv(
    original_df: DataFrame,
    dataframes_to_process: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    """
    Merges GMV data into a dictionary of DataFrames and cleans the 'gmv' column.

    This function iterates through a provided dictionary of DataFrames,
    left-merges them with a GMV source DataFrame based on 'entity_id' and
    'vendor_code', converts the 'gmv' column to numeric, and fills NaN values
    with 0. It then compiles all processed DataFrames into a new dictionary.

    Args:
        original_df (DataFrame): The initial 'current' DataFrame to be included
                                 in the output dictionary without further processing.
        dataframes_to_process (Dict[str, DataFrame]): A dictionary where keys are
                                                     desired names (strings) and
                                                     values are the DataFrames to be
                                                     merged with GMV data.

    Returns:
        Dict[str, DataFrame]: A new dictionary where keys are the names and
                              values are the processed DataFrames, including
                              the 'current' DataFrame and all merged DataFrames
                              with cleaned 'gmv' data.
    """
    # Prepare the GMV lookup DataFrame once
    gmv_lookup = original_df[['entity_id', 'vendor_code', 'gmv']].copy()
    
    processed_dataframes = {'current': original_df} # Start with the 'current' df

    print(f"Starting GMV processing for {len(dataframes_to_process)} dataframes...")

    for name, df_to_merge in dataframes_to_process.items():
        # Perform a left merge to bring 'gmv' data into df_to_merge
        # 'how='left'' ensures all rows from df_to_merge are kept.
        merged_df = pd.merge(
            df_to_merge,
            gmv_lookup,
            on=['entity_id', 'vendor_code'],
            how='left',
            indicator=True
        )

        # Convert 'gmv' column to numeric, coercing errors to NaN
        merged_df['gmv'] = pd.to_numeric(merged_df['gmv'], errors='coerce')
        # Fill any resulting NaN values in 'gmv' with 0.
        merged_df.fillna({'gmv': 0}, inplace=True) 
        processed_dataframes[name] = merged_df

        # "lost vendors" due to not being in gmv_lookup:
        # Create a temporary column to mark if gmv was filled by the merge
        num_unmatched_in_gmv_lookup = (merged_df['_merge'] == 'left_only').sum()
        print(f"  {num_unmatched_in_gmv_lookup} vendors from '{name}' were not found in GMV source.")

    print("\nGMV processing complete.")
    return processed_dataframes


def plot_figure_wrapper(
    outlier_summary_results: DataFrame,
    y_val: str,
    y_label: str,
    title: str,
    save_path: Optional[str] = None
) -> None:
    """
    Generates and displays a bar plot for outlier summary results.

    Visualizes a specified Y-value from `outlier_summary_results` against
    DataFrame names. Offers an option to save the plot.

    Args:
        outlier_summary_results (pd.DataFrame): DataFrame containing plot data.
                                                Expected to have 'df_name' and `y_val` columns.
        y_val (str): Column name for the Y-axis values.
        y_label (str): Label for the Y-axis.
        title (str): Plot title.
        save_path (Optional[str], optional): File path to save the plot.
                                             If None, the plot is only displayed.

    Returns:
        None: Displays the plot and optionally saves it.
    """
    plt.figure(figsize=(10, 6))

    # Create the bar plot
    sns.barplot(data=outlier_summary_results, x='df_name', y=y_val, palette='viridis')

    # Add labels and title
    plt.xlabel('Cohort creation rule', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.ylabel(y_label, fontsize=12)
    plt.title(title, fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    if save_path:
        try:
            plt.savefig(save_path, bbox_inches='tight', dpi=300)
            print(f"Plot saved to: {save_path}")
        except Exception as e:
            print(f"Error saving plot to {save_path}: {e}")

    plt.show()


def get_top_cohort_items(df: DataFrame, n: int=5) -> ItemsView:
    return (
        df    
        .groupby(['cohort_id', 'cohort_features'])['vendor_code']
        .nunique()
        .sort_values(ascending=False)
        .head(n)
        .to_dict()
    ).items()


def pretty_print_output(items: ItemsView) -> None:
    for (cohort_id, cohort_features_json_str), count in items:
        try:
            # Parse the JSON string into a Python dict
            features_dict = json.loads(cohort_features_json_str)

            # Pretty-print the Python dict back to a JSON string with indentation
            pretty_features_json = json.dumps(features_dict, indent=4)

            # Print the formatted output
            print(f"Cohort ID: {cohort_id}")
            print(f"Features:\n{pretty_features_json}")
            print(f"Unique Vendors: {count}")
            print("-" * 30) # Separator for readability

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for cohort_id {cohort_id}: {e}")
            print(f"Raw features string: {cohort_features_json_str}")
            print(f"Unique Vendors: {count}")
            print("-" * 30)
    return


