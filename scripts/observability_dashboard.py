#!/usr/bin/env python
"""Launch the data quality observability dashboard."""

import sys

sys.path.insert(0, "src")
from data_observability.dashboard import run_dashboard

if __name__ == "__main__":
    run_dashboard(
        results_dir="data/quality_results",
        metrics_dir="data/quality_metrics",
    )
