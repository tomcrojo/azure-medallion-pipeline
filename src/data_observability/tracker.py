"""
QualityTracker — ties Great Expectations results to observability metrics.

Run after GE validations to automatically:
- Store results in ResultStore
- Record freshness, volume, schema, null metrics
- Flag anomalies
"""

import json
from datetime import datetime
from typing import Optional

from data_observability.result_store import ResultStore
from data_observability.metrics import DataMetrics


class QualityTracker:
    """Unified tracker that combines GE results with observability metrics."""

    def __init__(
        self,
        results_dir: str = "data/quality_results",
        metrics_dir: str = "data/quality_metrics",
    ):
        self.store = ResultStore(results_dir)
        self.metrics = DataMetrics(metrics_dir)

    def track_validation(
        self,
        suite_name: str,
        dataset: str,
        success: bool,
        expectations: list[dict],
        row_count: int = 0,
        latest_timestamp: Optional[str] = None,
        column_null_counts: Optional[dict] = None,
        schema_columns: Optional[list[dict]] = None,
    ) -> dict:
        """Track a validation run and record all relevant metrics."""

        # Store the GE result
        self.store.save_result(
            suite_name=suite_name,
            dataset=dataset,
            success=success,
            expectations=expectations,
            row_count=row_count,
        )

        result = {
            "suite_name": suite_name,
            "dataset": dataset,
            "success": success,
            "metrics": {},
        }

        # Record volume metric
        result["metrics"]["volume"] = self.metrics.record_volume(
            dataset=dataset, row_count=row_count
        )

        # Record freshness if timestamp provided
        if latest_timestamp:
            result["metrics"]["freshness"] = self.metrics.record_freshness(
                dataset=dataset, latest_timestamp=latest_timestamp
            )

        # Record null rates if provided
        if column_null_counts and row_count > 0:
            result["metrics"]["null_rates"] = self.metrics.record_null_rates(
                dataset=dataset,
                column_null_counts=column_null_counts,
                total_rows=row_count,
            )

        # Record schema if provided
        if schema_columns:
            result["metrics"]["schema"] = self.metrics.record_schema(
                dataset=dataset, columns=schema_columns
            )

        return result

    def get_health_dashboard(self) -> dict:
        """Get all data needed for the observability dashboard."""
        return {
            "health_summary": self.metrics.get_health_summary(),
            "latest_runs": self.store.get_latest_by_suite(),
            "pass_rate_trend": self.store.get_pass_rate_trend(days=30),
            "recent_failures": self.store.get_failure_details(limit=50),
        }

    def print_summary(self):
        """Print a quick text summary of current health."""
        summary = self.metrics.get_health_summary()

        if not summary:
            print("No quality data recorded yet.")
            return

        print("\n=== Data Quality Health ===\n")
        for dataset, health in summary.items():
            icon = {
                "healthy": "[OK]",
                "warning": "[!!]",
                "critical": "[XX]",
            }.get(health["overall"], "[??]")

            print(f"  {icon} {dataset}: {health['overall'].upper()}")
            print(f"      Freshness: {health['freshness']}")
            print(f"      Volume:    {health['volume']}")
            print(f"      Schema:    {health['schema']}")
            print(f"      Nulls:     {health['nulls']}")
            print()

        failures = self.store.get_failure_details(limit=5)
        if failures:
            print("  Recent Failures:")
            for f in failures[:5]:
                print(
                    f"    - [{f['timestamp'][:10]}] {f['suite']}: "
                    f"{f['expectation']} on '{f['column']}'"
                )
