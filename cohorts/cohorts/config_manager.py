from dataclasses import dataclass, field
from typing import Dict, Any, List

@dataclass(frozen=True)
class TableConfig:
    """
    Manages access to various table locations based on category, parity, type, and version.

    This class provides a structured and type-hinted way to retrieve specific table paths
    from a nested configuration dictionary.

    Source of data: (
    https://docs.google.com/spreadsheets/d/
    165JRz9_gM4_seTyhQn-PdexKwWyH7b3mOqZTKONc8O8/edit?gid=371380795#gid=371380795
    )

    """
    _config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """
        Initializes the internal configuration dictionary with predefined table paths.

        This method is called automatically after the TableConfig instance is created.
        It bypasses the frozen=True limitation for initial assignment to _config.
        """
        object.__setattr__(self, '_config', {
            "base": {
                "even": {
                    "cohort data": {
                        "original": "logistics-vendor-production.pa_staging.devin_original_cohort_step3",
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3",
                        "turkey_original": "logistics-vendor-production.pa_staging.devin_original_cohort_step3_tr"
                    },
                    "recommendation": {
                        "original": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v5_step3",
                        "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v1_step3",
                        "turkey_original": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v5_step3_tr"
                    },
                    "recommendation (KPIs)": {
                        "original": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v4",
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v1",
                        "turkey_original": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v4_tr"
                    }

                },
                "uneven": {
                    "cohort data": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_uneven"
                    },
                    "recommendation": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v1_step3_uneven"
                    },
                    "recommendation (KPIs)": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v1_benchmark_uneven"
                    }
                }
            },
            "key account segment addition": {
                "even": {
                    "cohort data": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_vendor_cohorts_new_key_segment"
                    },
                    "recommendation": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v4_step3"
                    },
                    "recommendation (KPIs)": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v3"
                    }
                },
                "uneven": {
                    "cohort data": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_uneven_v2"
                    },
                    "recommendation": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v1_step3_uneven_v2"
                    },
                    "recommendation (KPIs)": {
                        "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v1_benchmark_uneven_v2"
                    },
                }
            },
            "no chain logic": {
                        "even": {
                            "cohort data": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_chainless"
                            },
                            "recommendation": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v2_step3"
                            },
                            "recommendation (KPIs)": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v2"
                            }
                        },
                        "uneven": {
                            "cohort data": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_reco_vendor_cohorts_step_3_uneven_v1"
                            },
                            "recommendation": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_recommendations_v1_step3_uneven_v1"
                            },
                            "recommendation (KPIs)": {
                                "current": "logistics-vendor-production.pa_staging.devin_smart_reco_metrics_v1_benchmark_uneven_v1"
                            }
                        }
            },
            "key account no chain": {
                        "even": {
                            "cohort data": {
                                "current": "logistics-vendor-production.pa_staging.vendor_chort_step3_kasc_no_chain_v1",
                                "turkey": "logistics-vendor-production.pa_staging.vendor_chort_step3_kasc_no_chain_v1_tr"
                            },
                            "recommendation": {
                                "current": "logistics-vendor-production.pa_staging.reco_step3_kasc_no_chain_v1",
                                "turkey": "logistics-vendor-production.pa_staging.reco_step3_kasc_no_chain_v1_tr"
                            },
                            "recommendation (KPIs)": {
                                "current": "logistics-vendor-production.pa_staging.reco_step1_kasc_no_chain",
                                "turkey": "logistics-vendor-production.pa_staging.reco_step1_kasc_no_chain_tr"
                            }
                        }
            }
        }
    )

    def get_path(self, category: str, parity: str, data_type: str, version: str = "current") -> str:
        """
        Retrieves a table path based on specified parameters.

        Args:
            category (str): The main category (e.g., "version").
            parity (str): "even" or "uneven".
            data_type (str): The type of data (e.g., "cohort data", "recommendation (KPIs)").
            version (str): The version of the data ("current" or "original"). Defaults to "current".

        Returns:
            str: The requested table path.

        Raises:
            KeyError: If any part of the path is not found in the configuration.
        """
        try:
            return self._config[category][parity][data_type][version]
        except KeyError as e:
            path_attempted = f"category='{category}', parity='{parity}', data_type='{data_type}', version='{version}'"
            raise KeyError(f"Configuration path not found: {path_attempted}. Missing key: {e}") from e

    def get_categories(self) -> List[str]:
        """
        Returns a list of all top-level categories available in the configuration.

        Returns:
            List[str]: A list of category names (e.g., "version").
        """
        return list(self._config.keys())

    def get_parities(self, category: str) -> List[str]:
        """
        Returns a list of parities ('even', 'uneven') available for a given category.

        Args:
            category (str): The main category to query (e.g., "version").

        Returns:
            List[str]: A list of parity names (e.g., "even", "uneven").
                       Returns an empty list if the category does not exist.
        """
        return list(self._config.get(category, {}).keys())

    def get_data_types(self, category: str, parity: str) -> List[str]:
        """
        Returns a list of data types available for a given category and parity.

        Args:
            category (str): The main category (e.g., "version").
            parity (str): The parity ('even' or 'uneven').

        Returns:
            List[str]: A list of data type names (e.g., "cohort data").
                       Returns an empty list if the category or parity does not exist.
        """
        return list(self._config.get(category, {}).get(parity, {}).keys())

    def get_versions(self, category: str, parity: str, data_type: str) -> List[str]:
        """
        Returns a list of available versions ('current', 'original') for a specific
        category, parity, and data type.

        Args:
            category (str): The main category.
            parity (str): The parity ('even' or 'uneven').
            data_type (str): The data type (e.g., "cohort data").

        Returns:
            List[str]: A list of version names (e.g., "current", "original").
                       Returns an empty list if the path does not exist up to data_type.
        """
        return list(self._config.get(category, {}).get(parity, {}).get(data_type, {}).keys())