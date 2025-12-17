"""
DataMetrics — compute observability metrics for data quality monitoring.

Provides:
- Freshness: how long since data was last updated
- Volume: row count trends and anomalies
- Schema drift: column additions/removals/type changes
- Null rates: percentage of nulls per column
- Completeness: overall data completeness score
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import json


class DataMetrics:
    """Compute and track data quality metrics."""

    def __init__(self, metrics_dir: str = "data/quality_metrics"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

    def record_freshness(
        self,
        dataset: str,
        latest_timestamp: str,
        expected_interval_minutes: int = 60,
    ) -> dict:
        """Record and evaluate data freshness."""
        latest = datetime.fromisoformat(latest_timestamp)
        now = datetime.utcnow()
        staleness_minutes = (now - latest).total_seconds() / 60

        metric = {
            "timestamp": now.isoformat(),
            "dataset": dataset,
            "metric": "freshness",
            "latest_data_timestamp": latest_timestamp,
            "staleness_minutes": round(staleness_minutes, 1),
            "expected_interval_minutes": expected_interval_minutes,
            "status": "fresh"
            if staleness_minutes <= expected_interval_minutes * 1.5
            else "stale"
            if staleness_minutes <= expected_interval_minutes * 3
            else "critical",
        }

        self._save_metric("freshness", dataset, metric)
        return metric

    def record_volume(
        self,
        dataset: str,
        row_count: int,
        expected_min: int = 0,
        expected_max: Optional[int] = None,
    ) -> dict:
        """Record and evaluate data volume."""
        now = datetime.utcnow()

        # Compare with previous runs for anomaly detection
        prev = self._get_previous_metric("volume", dataset)
        change_pct = None
        if prev and prev.get("row_count", 0) > 0:
            change_pct = round(
                (row_count - prev["row_count"]) / prev["row_count"] * 100, 1
            )

        anomaly = False
        if change_pct is not None and abs(change_pct) > 50:
            anomaly = True

        metric = {
            "timestamp": now.isoformat(),
            "dataset": dataset,
            "metric": "volume",
            "row_count": row_count,
            "expected_min": expected_min,
            "expected_max": expected_max,
            "change_from_previous_pct": change_pct,
            "anomaly_detected": anomaly,
            "status": "ok"
            if expected_min <= row_count <= (expected_max or float("inf"))
            else "warning",
        }

        self._save_metric("volume", dataset, metric)
        return metric

    def record_schema(
        self,
        dataset: str,
        columns: list[dict],
    ) -> dict:
        """Record schema and detect drift."""
        now = datetime.utcnow()
        current_cols = {c["name"]: c.get("type", "unknown") for c in columns}

        prev = self._get_previous_metric("schema", dataset)
        added, removed, type_changed = [], [], []

        if prev:
            prev_cols = prev.get("columns", {})
            added = [k for k in current_cols if k not in prev_cols]
            removed = [k for k in prev_cols if k not in current_cols]
            type_changed = [
                k
                for k in current_cols
                if k in prev_cols and current_cols[k] != prev_cols[k]
            ]

        metric = {
            "timestamp": now.isoformat(),
            "dataset": dataset,
            "metric": "schema",
            "column_count": len(current_cols),
            "columns": current_cols,
            "drift": {
                "added": added,
                "removed": removed,
                "type_changed": type_changed,
                "has_drift": bool(added or removed or type_changed),
            },
            "status": "ok" if not (added or removed or type_changed) else "drifted",
        }

        self._save_metric("schema", dataset, metric)
        return metric

    def record_null_rates(
        self,
        dataset: str,
        column_null_counts: dict[str, int],
        total_rows: int,
        threshold_pct: float = 5.0,
    ) -> dict:
        """Record null rates per column and flag high-null columns."""
        now = datetime.utcnow()
        rates = {}
        flagged = []

        for col, null_count in column_null_counts.items():
            rate = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
            rates[col] = {"null_count": null_count, "null_rate_pct": rate}
            if rate > threshold_pct:
                flagged.append(col)

        metric = {
            "timestamp": now.isoformat(),
            "dataset": dataset,
            "metric": "null_rates",
            "total_rows": total_rows,
            "threshold_pct": threshold_pct,
            "columns": rates,
            "flagged_columns": flagged,
            "status": "ok" if not flagged else "warning",
        }

        self._save_metric("null_rates", dataset, metric)
        return metric

    def get_health_summary(self) -> dict:
        """Get overall health summary across all datasets."""
        freshness = self._get_latest_by_type("freshness")
        volume = self._get_latest_by_type("volume")
        schema = self._get_latest_by_type("schema")
        nulls = self._get_latest_by_type("null_rates")

        all_datasets = set()
        for metrics in [freshness, volume, schema, nulls]:
            all_datasets.update(metrics.keys())

        summary = {}
        for ds in all_datasets:
            statuses = []
            if ds in freshness:
                statuses.append(freshness[ds]["status"])
            if ds in volume:
                statuses.append(volume[ds]["status"])
            if ds in schema:
                statuses.append(schema[ds]["status"])
            if ds in nulls:
                statuses.append(nulls[ds]["status"])

            if "critical" in statuses:
                overall = "critical"
            elif any(s in ("stale", "drifted", "warning") for s in statuses):
                overall = "warning"
            else:
                overall = "healthy"

            summary[ds] = {
                "overall": overall,
                "freshness": freshness.get(ds, {}).get("status", "unknown"),
                "volume": volume.get(ds, {}).get("status", "unknown"),
                "schema": schema.get(ds, {}).get("status", "unknown"),
                "nulls": nulls.get(ds, {}).get("status", "unknown"),
            }

        return summary

    def _save_metric(self, metric_type: str, dataset: str, data: dict):
        """Save metric to JSON file."""
        subdir = self.metrics_dir / metric_type
        subdir.mkdir(exist_ok=True)
        filename = f"{dataset}.jsonl"
        filepath = subdir / filename
        with open(filepath, "a") as f:
            f.write(json.dumps(data, default=str) + "\n")

    def _get_previous_metric(self, metric_type: str, dataset: str) -> Optional[dict]:
        """Get the previous metric for a dataset."""
        filepath = self.metrics_dir / metric_type / f"{dataset}.jsonl"
        if not filepath.exists():
            return None
        lines = filepath.read_text().strip().split("\n")
        if len(lines) < 2:
            return None
        return json.loads(lines[-2])

    def _get_latest_by_type(self, metric_type: str) -> dict:
        """Get latest metric for each dataset of a given type."""
        subdir = self.metrics_dir / metric_type
        if not subdir.exists():
            return {}
        latest = {}
        for f in subdir.glob("*.jsonl"):
            lines = f.read_text().strip().split("\n")
            if lines:
                ds = f.stem
                latest[ds] = json.loads(lines[-1])
        return latest
