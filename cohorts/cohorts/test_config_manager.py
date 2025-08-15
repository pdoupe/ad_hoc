import pytest
from .config_manager import TableConfig

@pytest.fixture(scope="module")
def config_instance(): # Renamed here
    """Provides a single TableConfig instance for all tests in this module."""
    return TableConfig()

def test_get_path_key_error(config_instance: TableConfig):
    """
    Test that get_path raises a KeyError when a path is not found.
    We'll attempt to access a non-existent category to trigger the error.
    """
    with pytest.raises(KeyError, match="Configuration path not found"):
        config_instance.get_path(
            category="non_existent_category",
            parity="even",
            data_type="cohort data",
            version="current"
        )

def test_get_all_methods(config_instance: TableConfig):
    """
    Test that the utility methods (get_categories, get_parities, get_data_types, get_versions)
    return the correct keys from the config.
    """
    # Test top-level categories
    expected_categories = sorted(["base", "key account segment addition", "no chain logic", "ys_tr"])
    assert sorted(config_instance.get_categories()) == expected_categories

    # Test parities for a specific category
    expected_parities = sorted(["even", "uneven"])
    assert sorted(config_instance.get_parities("base")) == expected_parities

    # Test data types for a specific category and parity
    expected_data_types = sorted(["cohort data", "recommendation", "recommendation (KPIs)"])
    assert sorted(config_instance.get_data_types("base", "even")) == expected_data_types

    # Test versions for a specific path
    expected_versions = sorted(["original", "current"])
    assert sorted(config_instance.get_versions("base", "even", "cohort data")) == expected_versions

    # Test a case where the path doesn't have an "original" version
    expected_uneven_versions = ["current"]
    assert sorted(config_instance.get_versions("base", "uneven", "cohort data")) == sorted(expected_uneven_versions)

    # Test a non-existent path
    assert config_instance.get_versions("base", "uneven", "non_existent_data_type") == []