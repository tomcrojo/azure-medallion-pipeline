"""
Data Quality Observability Dashboard — Streamlit app.

Run with: streamlit run src/data_observability/dashboard.py

Shows:
- Health overview (traffic light per dataset)
- Pass rate trend over time
- Volume trends with anomaly detection
- Freshness status
- Recent failures with details
- Schema drift alerts
- Null rate heatmap
"""

import json
import sys
from pathlib import Path

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def load_results(results_dir: str = "data/quality_results") -> list[dict]:
    """Load all validation results from JSON files."""
    p = Path(results_dir)
    if not p.exists():
        return []
    results = []
    for f in sorted(p.glob("*.json"), reverse=True):
        try:
            with open(f) as fh:
                results.append(json.load(fh))
        except (json.JSONDecodeError, KeyError):
            continue
    return results


def load_metrics(metrics_dir: str = "data/quality_metrics") -> dict:
    """Load all metrics from JSONL files."""
    p = Path(metrics_dir)
    if not p.exists():
        return {}
    metrics = {}
    for subdir in p.iterdir():
        if subdir.is_dir():
            metric_type = subdir.name
            metrics[metric_type] = {}
            for f in subdir.glob("*.jsonl"):
                lines = f.read_text().strip().split("\n")
                if lines:
                    dataset = f.stem
                    metrics[metric_type][dataset] = [
                        json.loads(line) for line in lines if line.strip()
                    ]
    return metrics


def render_health_overview(results: list[dict]):
    """Render the health overview section."""
    st.header("Health Overview")

    if not results:
        st.info("No validation results yet. Run quality checks first.")
        return

    # Get latest run per suite
    latest = {}
    for r in results:
        suite = r["suite_name"]
        if suite not in latest:
            latest[suite] = r

    cols = st.columns(min(len(latest), 4))
    for i, (suite, run) in enumerate(latest.items()):
        with cols[i % len(cols)]:
            status = "PASS" if run["success"] else "FAIL"
            color = "green" if run["success"] else "red"
            st.metric(
                label=suite.replace("_", " ").title(),
                value=status,
                delta=f"{run['passed']}/{run['total_expectations']} checks",
                delta_color="normal" if run["success"] else "inverse",
            )


def render_pass_rate_trend(results: list[dict]):
    """Render pass rate trend chart."""
    st.header("Pass Rate Trend")

    if len(results) < 2:
        st.info("Need more validation runs to show trends.")
        return

    # Build daily pass rates
    daily = {}
    for r in results:
        day = r["timestamp"][:10]
        if day not in daily:
            daily[day] = {"passed": 0, "failed": 0}
        if r["success"]:
            daily[day]["passed"] += 1
        else:
            daily[day]["failed"] += 1

    df = pd.DataFrame(
        [
            {
                "date": day,
                "pass_rate": round(d["passed"] / (d["passed"] + d["failed"]) * 100, 1),
                "runs": d["passed"] + d["failed"],
            }
            for day, d in sorted(daily.items())
        ]
    )

    fig = px.line(
        df,
        x="date",
        y="pass_rate",
        markers=True,
        title="Daily Pass Rate (%)",
        labels={"pass_rate": "Pass Rate %", "date": "Date"},
    )
    fig.add_hline(
        y=95, line_dash="dash", line_color="orange", annotation_text="Target: 95%"
    )
    fig.update_layout(yaxis_range=[0, 105])
    st.plotly_chart(fig, use_container_width=True)


def render_volume_trends(metrics: dict):
    """Render volume trend charts."""
    st.header("Volume Trends")

    volume_data = metrics.get("volume", {})
    if not volume_data:
        st.info("No volume metrics recorded yet.")
        return

    for dataset, records in volume_data.items():
        if len(records) < 2:
            continue

        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        fig = go.Figure()

        # Volume line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["row_count"],
                mode="lines+markers",
                name="Row Count",
                line=dict(color="#1f77b4"),
            )
        )

        # Highlight anomalies
        anomalies = df[df["anomaly_detected"] == True]
        if not anomalies.empty:
            fig.add_trace(
                go.Scatter(
                    x=anomalies["timestamp"],
                    y=anomalies["row_count"],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color="red", size=12, symbol="x"),
                )
            )

        fig.update_layout(
            title=f"Volume: {dataset}",
            xaxis_title="Time",
            yaxis_title="Row Count",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_freshness_status(metrics: dict):
    """Render freshness status cards."""
    st.header("Data Freshness")

    freshness_data = metrics.get("freshness", {})
    if not freshness_data:
        st.info("No freshness metrics recorded yet.")
        return

    for dataset, records in freshness_data.items():
        if not records:
            continue
        latest = records[-1]

        status = latest["status"]
        staleness = latest["staleness_minutes"]
        expected = latest["expected_interval_minutes"]

        if status == "fresh":
            st.success(
                f"**{dataset}** — Fresh (last update {staleness:.0f}min ago, expected every {expected}min)"
            )
        elif status == "stale":
            st.warning(
                f"**{dataset}** — Stale (last update {staleness:.0f}min ago, expected every {expected}min)"
            )
        else:
            st.error(
                f"**{dataset}** — CRITICAL (last update {staleness:.0f}min ago, expected every {expected}min)"
            )


def render_null_rates(metrics: dict):
    """Render null rate heatmap."""
    st.header("Null Rates by Column")

    null_data = metrics.get("null_rates", {})
    if not null_data:
        st.info("No null rate metrics recorded yet.")
        return

    for dataset, records in null_data.items():
        if not records:
            continue
        latest = records[-1]

        columns = latest.get("columns", {})
        if not columns:
            continue

        df = pd.DataFrame(
            [
                {
                    "column": col,
                    "null_rate_pct": data["null_rate_pct"],
                    "null_count": data["null_count"],
                    "flagged": col in latest.get("flagged_columns", []),
                }
                for col, data in columns.items()
            ]
        ).sort_values("null_rate_pct", ascending=False)

        fig = px.bar(
            df,
            x="column",
            y="null_rate_pct",
            color="flagged",
            color_discrete_map={True: "red", False: "#1f77b4"},
            title=f"Null Rates: {dataset}",
            labels={"null_rate_pct": "Null Rate %", "column": "Column"},
        )
        threshold = latest.get("threshold_pct", 5.0)
        fig.add_hline(
            y=threshold,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Threshold: {threshold}%",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_schema_drift(metrics: dict):
    """Render schema drift alerts."""
    st.header("Schema Drift")

    schema_data = metrics.get("schema", {})
    if not schema_data:
        st.info("No schema metrics recorded yet.")
        return

    for dataset, records in schema_data.items():
        if not records:
            continue
        latest = records[-1]
        drift = latest.get("drift", {})

        if drift.get("has_drift"):
            st.warning(f"**{dataset}** — Schema drift detected!")
            col1, col2, col3 = st.columns(3)
            with col1:
                if drift["added"]:
                    st.write("Added columns:")
                    for c in drift["added"]:
                        st.write(f"- `{c}`")
            with col2:
                if drift["removed"]:
                    st.write("Removed columns:")
                    for c in drift["removed"]:
                        st.write(f"- ~~{c}~~")
            with col3:
                if drift["type_changed"]:
                    st.write("Type changes:")
                    for c in drift["type_changed"]:
                        st.write(f"- `{c}`")
        else:
            st.success(
                f"**{dataset}** — No schema drift ({latest['column_count']} columns stable)"
            )


def render_recent_failures(results: list[dict]):
    """Render recent failure details."""
    st.header("Recent Failures")

    failures = []
    for r in results:
        for exp in r.get("expectations", []):
            if not exp.get("success", False):
                failures.append(
                    {
                        "timestamp": r["timestamp"][:19],
                        "suite": r["suite_name"],
                        "dataset": r["dataset"],
                        "expectation": exp.get("expectation_type", "unknown"),
                        "column": exp.get("kwargs", {}).get("column", "N/A"),
                    }
                )

    if not failures:
        st.success("No recent failures!")
        return

    df = pd.DataFrame(failures[:100])
    st.dataframe(df, use_container_width=True, hide_index=True)


def run_dashboard(
    results_dir: str = "data/quality_results",
    metrics_dir: str = "data/quality_metrics",
):
    """Run the Streamlit dashboard."""
    st.set_page_config(
        page_title="Data Quality Observability",
        page_icon="Data",
        layout="wide",
    )

    st.title("Data Quality Observability")

    # Sidebar
    st.sidebar.header("Controls")
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        import time

        time.sleep(30)
        st.rerun()

    if st.sidebar.button("Refresh Now"):
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data directories:**")
    st.sidebar.code(f"Results: {results_dir}")
    st.sidebar.code(f"Metrics: {metrics_dir}")

    # Load data
    results = load_results(results_dir)
    metrics = load_metrics(metrics_dir)

    # Render sections
    render_health_overview(results)
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        render_pass_rate_trend(results)
    with col2:
        render_freshness_status(metrics)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        render_volume_trends(metrics)
    with col2:
        render_null_rates(metrics)

    st.divider()
    render_schema_drift(metrics)
    st.divider()
    render_recent_failures(results)


if __name__ == "__main__":
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "data/quality_results"
    metrics_dir = sys.argv[2] if len(sys.argv) > 2 else "data/quality_metrics"
    run_dashboard(results_dir, metrics_dir)
