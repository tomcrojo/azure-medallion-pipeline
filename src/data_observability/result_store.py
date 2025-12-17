"""
ResultStore — persist and query data quality validation results over time.

Stores each validation run as a JSON file with timestamp, suite name,
dataset, pass/fail status, and individual expectation results.

Supports querying: last N runs, runs by suite, runs by date range,
pass rate trends.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


class ResultStore:
    """Persistent store for Great Expectations validation results."""

    def __init__(self, store_dir: str = "data/quality_results"):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def save_result(
        self,
        suite_name: str,
        dataset: str,
        success: bool,
        expectations: list[dict],
        row_count: int = 0,
        metadata: Optional[dict] = None,
    ) -> str:
        """Save a validation result. Returns the file path."""
        timestamp = datetime.utcnow()
        result = {
            "timestamp": timestamp.isoformat(),
            "suite_name": suite_name,
            "dataset": dataset,
            "success": success,
            "row_count": row_count,
            "total_expectations": len(expectations),
            "passed": sum(1 for e in expectations if e.get("success", False)),
            "failed": sum(1 for e in expectations if not e.get("success", False)),
            "expectations": expectations,
            "metadata": metadata or {},
        }

        filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{suite_name}.json"
        filepath = self.store_dir / filename

        with open(filepath, "w") as f:
            json.dump(result, f, indent=2, default=str)

        return str(filepath)

    def get_runs(
        self,
        suite_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query validation runs with optional filters."""
        results = []
        files = sorted(self.store_dir.glob("*.json"), reverse=True)

        for f in files:
            if len(results) >= limit:
                break
            try:
                with open(f) as fh:
                    data = json.load(fh)
                if suite_name and data.get("suite_name") != suite_name:
                    continue
                if since and datetime.fromisoformat(data["timestamp"]) < since:
                    continue
                results.append(data)
            except (json.JSONDecodeError, KeyError):
                continue

        return results

    def get_pass_rate_trend(
        self,
        suite_name: Optional[str] = None,
        days: int = 30,
    ) -> list[dict]:
        """Get daily pass rate trend for charting."""
        since = datetime.utcnow() - timedelta(days=days)
        runs = self.get_runs(suite_name=suite_name, since=since)

        daily = {}
        for run in runs:
            day = run["timestamp"][:10]
            if day not in daily:
                daily[day] = {"total": 0, "passed": 0}
            daily[day]["total"] += 1
            if run["success"]:
                daily[day]["passed"] += 1

        return [
            {
                "date": day,
                "pass_rate": round(d["passed"] / d["total"] * 100, 1)
                if d["total"]
                else 0,
                "total_runs": d["total"],
                "passed": d["passed"],
                "failed": d["total"] - d["passed"],
            }
            for day, d in sorted(daily.items())
        ]

    def get_latest_by_suite(self) -> dict:
        """Get the most recent run for each suite."""
        latest = {}
        for run in self.get_runs(limit=1000):
            suite = run["suite_name"]
            if suite not in latest:
                latest[suite] = run
        return latest

    def get_failure_details(
        self,
        suite_name: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        """Get details of failed expectations from recent runs."""
        runs = self.get_runs(suite_name=suite_name, limit=limit)
        failures = []
        for run in runs:
            for exp in run.get("expectations", []):
                if not exp.get("success", False):
                    failures.append(
                        {
                            "timestamp": run["timestamp"],
                            "suite": run["suite_name"],
                            "dataset": run["dataset"],
                            "expectation": exp.get("expectation_type", "unknown"),
                            "column": exp.get("kwargs", {}).get("column", ""),
                            "details": exp.get("result", {}),
                        }
                    )
        return failures[:limit]

    def cleanup(self, keep_days: int = 90):
        """Remove results older than keep_days."""
        cutoff = datetime.utcnow() - timedelta(days=keep_days)
        for f in self.store_dir.glob("*.json"):
            try:
                with open(f) as fh:
                    data = json.load(fh)
                if datetime.fromisoformat(data["timestamp"]) < cutoff:
                    f.unlink()
            except (json.JSONDecodeError, KeyError):
                continue
