"""
Data Observability — reusable quality monitoring for data pipelines.

Provides:
- ResultStore: persist and query validation results over time
- DataMetrics: compute freshness, volume, schema drift, null rates
- run_dashboard: Streamlit dashboard for visualizing data quality

Usage:
    from data_observability import ResultStore, DataMetrics, run_dashboard
"""

from data_observability.result_store import ResultStore
from data_observability.metrics import DataMetrics
from data_observability.tracker import QualityTracker

__all__ = ["ResultStore", "DataMetrics", "QualityTracker"]
