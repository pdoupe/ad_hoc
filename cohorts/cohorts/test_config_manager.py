import pytest
from .config_manager import TableConfig

@pytest.fixture(scope="module")
def _config_instance_fixture(): # Renamed here
    """Provides a single TableConfig instance for all tests in this module."""
    return TableConfig()

def test_get_existing_path_current_even(config_instance: TableConfig): # Added type hint for clarity
    """Test retrieving a standard 'current' path for an 'even' category."""
    path = config_instance.get_path(
        category="Global version",
        parity="even",
        data_type="cohort data",
        version="current"
    )
    assert path == "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3"

def test_get_existing_path_original_even(config_instance: TableConfig):
    """Test retrieving an 'original' path for an 'even' category."""
    path = config_instance.get_path(
        category="Global version",
        parity="even",
        data_type="recommendation (KPIs)",
        version="original"
    )
    assert path == "logistics-vendor-production.pa_staging.devin.smart_reco_metrics_v4"

def test_get_existing_path_current_uneven(config_instance: TableConfig):
    """Test retrieving a 'current' path for an 'uneven' category."""
    path = config_instance.get_path(
        category="Global version",
        parity="uneven",
        data_type="cohort data",
        version="current"
    )
    assert path == (
        "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_uneven"
    )

def test_get_path_with_default_version(config_instance: TableConfig):
    """Test get_path defaults to 'current' version when not specified."""
    path = config_instance.get_path(
        category="Global no chain logic",
        parity="even",
        data_type="recommendation"
    )
    assert path == "logistics-vendor-production.pa_staging.devin.smart_recommendations_v2_step3"

def test_get_path_raises_key_error_for_non_existent_category(config_instance: TableConfig):
    """Test KeyError is raised for a non-existent main category."""
    with pytest.raises(KeyError) as excinfo:
        config_instance.get_path(
            category="Non Existent Category",
            parity="even",
            data_type="cohort data",
            version="current"
        )
    assert "Configuration path not found" in str(excinfo.value)
    assert "category='Non Existent Category'" in str(excinfo.value)

def test_get_path_raises_key_error_for_non_existent_parity(config_instance: TableConfig):
    """Test KeyError is raised for a non-existent parity within a category."""
    with pytest.raises(KeyError) as excinfo:
        config_instance.get_path(
            category="Global version",
            parity="odd", # 'odd' does not exist
            data_type="cohort data",
            version="current"
        )
    assert "Configuration path not found" in str(excinfo.value)
    assert "parity='odd'" in str(excinfo.value)

def test_get_path_raises_key_error_for_non_existent_data_type(config_instance: TableConfig):
    """Test KeyError is raised for a non-existent data type."""
    with pytest.raises(KeyError) as excinfo:
        config_instance.get_path(
            category="Global version",
            parity="even",
            data_type="non_existent_data_type",
            version="current"
        )
    assert "Configuration path not found" in str(excinfo.value)
    assert "data_type='non_existent_data_type'" in str(excinfo.value)

def test_get_path_raises_key_error_for_non_existent_version(config_instance: TableConfig):
    """Test KeyError is raised when requesting a version that doesn't exist for a data type."""
    with pytest.raises(KeyError) as excinfo:
        config_instance.get_path(
            category="Global version",
            parity="uneven",
            data_type="cohort data",
            version="original" # 'original' does not exist for this specific path
        )
    assert "Configuration path not found" in str(excinfo.value)
    assert "version='original'" in str(excinfo.value)

def test_get_categories(config_instance: TableConfig):
    """Test get_categories returns the correct top-level keys."""
    categories = config_instance.get_categories()
    expected_categories = [
        "Global version",
        "Global key account segment addition",
        "Global no chain logic"
    ]
    assert sorted(categories) == sorted(expected_categories)

def test_get_parities(config_instance: TableConfig):
    """Test get_parities returns correct parities for a category."""
    parities = config_instance.get_parities("Global version")
    assert sorted(parities) == sorted(["even", "uneven"])
    assert config_instance.get_parities("Non Existent Category") == []

def test_get_data_types(config_instance: TableConfig):
    """Test get_data_types returns correct data types for a category and parity."""
    data_types = config_instance.get_data_types("Global version", "even")
    expected_data_types = ["cohort data", "recommendation", "recommendation (KPIs)"]
    assert sorted(data_types) == sorted(expected_data_types)
    assert config_instance.get_data_types("Global version", "non_existent_parity") == []

def test_get_versions(config_instance: TableConfig):
    """Test get_versions returns correct versions for a data type."""
    versions = config_instance.get_versions("Global version", "even", "cohort data")
    assert sorted(versions) == sorted(["original", "current"])
    versions_no_original = config_instance.get_versions("Global version", "uneven", "cohort data")
    assert versions_no_original == ["current"]
    assert config_instance.get_versions("Global version", "even", "non_existent_data_type") == []
