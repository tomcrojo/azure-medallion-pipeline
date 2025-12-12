"""Custom Great Expectations for the Azure Medallion Pipeline."""

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    column_condition_partial,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import InvalidExpectationConfigurationError

import pandas as pd
import numpy as np


VALID_SPANISH_REGIONS = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Sevilla",
    "Zaragoza",
    "Málaga",
    "Murcia",
    "Palma",
    "Las Palmas",
    "Bilbao",
    "Alicante",
    "Córdoba",
    "Valladolid",
    "Vigo",
    "Gijón",
    "Granada",
    "A Coruña",
    "Vitoria",
    "Elche",
    "Oviedo",
    "Santa Cruz de Tenerife",
    "Pamplona",
    "Santander",
    "Castellón",
    "Burgos",
    "Albacete",
    "Salamanca",
    "Logroño",
    "Badajoz",
    "Huelva",
    "Tarragona",
    "León",
    "Lleida",
    "Cádiz",
    "Jaén",
    "Ourense",
    "Girona",
    "Lugo",
    "Cáceres",
    "Toledo",
    "Melilla",
    "Ceuta",
]


class ExpectColumnValuesToBeValidSpanishRegion(ColumnMapExpectation):
    """Expect each value in the column to be a valid Spanish region name.

    Args:
        column: The column name to check.
    """

    map_metric = "column_values.valid_spanish_region"
    success_keys = ()
    condition_metric_name = "column_values.valid_spanish_region"
    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.isin(VALID_SPANISH_REGIONS)

    library_metadata = {
        "maturity": "experimental",
        "tags": ["validity", "domain-specific"],
        "contributors": ["@medallion-pipeline"],
    }


class ExpectMedallionLayerConsistency(Expectation):
    """Check that the silver row count is within an acceptable range of the bronze count.

    This catches massive data loss during Bronze -> Silver transformation.
    By default, silver must retain 90-100% of bronze rows.

    Args:
        bronze_row_count: The number of rows in the bronze layer.
        silver_row_count: The number of rows in the silver layer.
        min_retention_rate: Minimum acceptable retention rate (default 0.90).
        max_retention_rate: Maximum acceptable retention rate (default 1.00).
    """

    success_keys = (
        "bronze_row_count",
        "silver_row_count",
        "min_retention_rate",
        "max_retention_rate",
    )
    domain_keys = ()

    def _validate(
        self,
        configuration,
        metrics,
        runtime_configuration=None,
        execution_engine=None,
    ):
        bronze_row_count = self.get_success_kwargs(configuration).get(
            "bronze_row_count"
        )
        silver_row_count = self.get_success_kwargs(configuration).get(
            "silver_row_count"
        )
        min_retention = self.get_success_kwargs(configuration).get(
            "min_retention_rate", 0.90
        )
        max_retention = self.get_success_kwargs(configuration).get(
            "max_retention_rate", 1.00
        )

        if bronze_row_count == 0:
            return {
                "success": False,
                "result": {
                    "observed_value": 0,
                    "details": "Bronze row count is zero — cannot compute retention rate.",
                },
            }

        retention_rate = silver_row_count / bronze_row_count
        success = min_retention <= retention_rate <= max_retention

        return {
            "success": success,
            "result": {
                "observed_value": round(retention_rate, 4),
                "bronze_row_count": bronze_row_count,
                "silver_row_count": silver_row_count,
                "min_retention_rate": min_retention,
                "max_retention_rate": max_retention,
                "details": (
                    f"Silver retains {retention_rate:.2%} of bronze rows "
                    f"({silver_row_count}/{bronze_row_count}). "
                    f"Acceptable range: [{min_retention:.0%}, {max_retention:.0%}]"
                ),
            },
        }

    library_metadata = {
        "maturity": "experimental",
        "tags": ["consistency", "cross-layer"],
        "contributors": ["@medallion-pipeline"],
    }


def validate_layer_consistency(bronze_count: int, silver_count: int) -> dict:
    """Helper function to validate cross-layer row count consistency.

    Args:
        bronze_count: Number of rows in the bronze layer.
        silver_count: Number of rows in the silver layer.

    Returns:
        Dict with success status and details.
    """
    if bronze_count == 0:
        return {"success": False, "message": "Bronze layer has zero rows."}

    retention = silver_count / bronze_count
    return {
        "success": 0.90 <= retention <= 1.00,
        "retention_rate": round(retention, 4),
        "message": f"Silver retains {retention:.2%} of bronze rows ({silver_count}/{bronze_count})",
    }
