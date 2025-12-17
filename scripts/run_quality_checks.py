#!/usr/bin/env python3
"""Run Great Expectations quality checks for all medallion layers.

Validates data at each layer boundary (Bronze -> Silver -> Gold) using
predefined expectation suites. Generates Data Docs HTML reports.
After GE runs, records observability metrics via QualityTracker.

Usage:
    python scripts/run_quality_checks.py              # Run all suites
    python scripts/run_quality_checks.py --suite bronze
    python scripts/run_quality_checks.py --suite silver
    python scripts/run_quality_checks.py --suite gold
"""

import argparse
import sys
import os

import great_expectations as gx
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from data_observability import QualityTracker


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GE_DIR = os.path.join(PROJECT_ROOT, "great_expectations")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


def get_bronze_dataframe() -> pd.DataFrame:
    """Load raw CSV data as the bronze layer."""
    path = os.path.join(DATA_DIR, "sample_orders.csv")
    df = pd.read_csv(path)
    df["ingestion_timestamp"] = pd.Timestamp.now()
    df["source_system"] = "csv_orders"
    df["file_name"] = "sample_orders.csv"
    return df


def get_silver_dataframe() -> pd.DataFrame:
    """Simulate silver transformation and return cleaned data."""
    df = get_bronze_dataframe()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    df = df.dropna(subset=["order_id", "quantity", "unit_price"])
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["total_amount"] = round(df["quantity"] * df["unit_price"], 2)
    df["order_year"] = df["order_date"].dt.year.astype(int)
    df["order_month"] = df["order_date"].dt.month.astype(int)
    df["data_quality_flag"] = "valid"
    df.loc[(df["quantity"] <= 0) | (df["unit_price"] <= 0), "data_quality_flag"] = (
        "warning"
    )
    df["silver_timestamp"] = pd.Timestamp.now()
    return df


def get_gold_dataframe() -> pd.DataFrame:
    """Simulate gold aggregation and return daily sales summary."""
    silver_df = get_silver_dataframe()
    valid_df = silver_df[silver_df["data_quality_flag"] == "valid"].copy()
    gold_df = (
        valid_df.groupby(["order_date", "region"])
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
        )
        .reset_index()
    )
    gold_df["total_revenue"] = gold_df["total_revenue"].round(2)
    gold_df["avg_order_value"] = gold_df["avg_order_value"].round(2)
    gold_df["gold_timestamp"] = pd.Timestamp.now()
    return gold_df


def run_suite(suite_name: str, df: pd.DataFrame, checkpoint_name: str) -> dict:
    """Run a single expectation suite against a dataframe.

    Args:
        suite_name: Name of the expectation suite.
        df: Pandas DataFrame to validate.
        checkpoint_name: Name of the checkpoint to use.

    Returns:
        Dict with 'success' (bool) and 'expectations' (list of dicts).
    """
    print(f"\n{'=' * 60}")
    print(f"Running {suite_name} ({len(df)} rows)")
    print(f"{'=' * 60}")

    context = gx.get_context(project_root_dir=GE_DIR)

    batch_request = {
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {"default_identifier_name": "default"},
        "datasource_name": "local_csv",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": suite_name.replace("_suite", ""),
    }

    try:
        checkpoint_result = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            batch_request=batch_request,
            run_name=f"{suite_name}_local_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
        )
    except Exception as e:
        print(f"ERROR running checkpoint '{checkpoint_name}': {e}")
        return {"success": False, "expectations": []}

    success = checkpoint_result.success
    stats = checkpoint_result.run_results
    all_expectations = []

    for run_id, result in stats.items():
        validation_result = result.get("validation_result", {})
        results = validation_result.get("results", [])
        passed = sum(1 for r in results if r.get("success", False))
        total = len(results)
        print(f"  Expectations: {passed}/{total} passed")

        for r in results:
            if not r.get("success", False):
                exp_type = r.get("expectation_config", {}).get(
                    "expectation_type", "unknown"
                )
                kwargs = r.get("expectation_config", {}).get("kwargs", {})
                col = kwargs.get("column", "")
                print(f"  FAILED: {exp_type} ({col})")
            all_expectations.append(
                {
                    "success": r.get("success", False),
                    "expectation_type": r.get("expectation_config", {}).get(
                        "expectation_type", "unknown"
                    ),
                    "kwargs": r.get("expectation_config", {}).get("kwargs", {}),
                    "result": r.get("result", {}),
                }
            )

    if success:
        print(f"  PASSED")
    else:
        print(f"  FAILED")

    return {"success": success, "expectations": all_expectations}


def main():
    parser = argparse.ArgumentParser(
        description="Run Great Expectations data quality checks."
    )
    parser.add_argument(
        "--suite",
        choices=["bronze", "silver", "gold"],
        help="Run a specific suite only (default: all)",
    )
    args = parser.parse_args()

    suites = {
        "bronze": {
            "suite_name": "bronze_orders_suite",
            "checkpoint_name": "bronze_checkpoint",
            "data_fn": get_bronze_dataframe,
        },
        "silver": {
            "suite_name": "silver_orders_suite",
            "checkpoint_name": "silver_checkpoint",
            "data_fn": get_silver_dataframe,
        },
        "gold": {
            "suite_name": "gold_daily_sales_suite",
            "checkpoint_name": "gold_checkpoint",
            "data_fn": get_gold_dataframe,
        },
    }

    if args.suite:
        suites = {args.suite: suites[args.suite]}

    all_passed = True
    tracker = QualityTracker()

    dataset_names = {
        "bronze": "bronze_orders",
        "silver": "silver_orders",
        "gold": "gold_daily_sales",
    }

    for layer, config in suites.items():
        df = config["data_fn"]()
        result = run_suite(config["suite_name"], df, config["checkpoint_name"])
        if not result["success"]:
            all_passed = False

        # Build column null counts for observability
        null_counts = df.isnull().sum().to_dict()
        null_counts = {k: int(v) for k, v in null_counts.items()}

        # Build schema columns list
        schema_columns = [
            {"name": col, "type": str(df[col].dtype)} for col in df.columns
        ]

        # Get latest timestamp if order_date exists
        latest_ts = None
        if "order_date" in df.columns:
            try:
                latest_ts = str(df["order_date"].max())
            except Exception:
                pass

        dataset = dataset_names.get(layer, layer)

        # Track with QualityTracker (runs AFTER GE, not instead of it)
        tracker.track_validation(
            suite_name=config["suite_name"],
            dataset=dataset,
            success=result["success"],
            expectations=result["expectations"],
            row_count=len(df),
            latest_timestamp=latest_ts,
            column_null_counts=null_counts,
            schema_columns=schema_columns,
        )

    tracker.print_summary()

    print(f"\n{'=' * 60}")
    if all_passed:
        print("All quality checks PASSED")
    else:
        print("Some quality checks FAILED — see details above")
    print(f"{'=' * 60}")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
