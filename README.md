# Azure Medallion Pipeline

A production-grade e-commerce data pipeline built on Azure, implementing the **Medallion Architecture** (Bronze → Silver → Gold) with Delta Lake. Infrastructure is provisioned with Terraform; transformations run as PySpark notebooks in Databricks; ingestion is orchestrated by Azure Data Factory.

**Dataset scale:** 7M+ orders | 200K customers | 500 products — generated with `faker` (Spanish locale).

## Architecture

```
                          ┌─────────────────────────────────────────────────────┐
                          │                  Azure Cloud                        │
                          │                                                     │
  ┌──────────┐            │  ┌──────────┐      ┌────────────────────────────┐   │
  │  REST /   │─── HTTP ──┼─▶│  Azure   │      │   ADLS Gen2 Storage       │   │
  │  CSV /    │            │  │  Data    │─ADF─▶│                           │   │
  │  Blob     │            │  │  Factory │      │  ┌────────┐ ┌────────┐   │   │
  └──────────┘            │  └──────────┘      │  │ Bronze │ │ Silver │   │   │
                          │       │            │  │ (raw)  │─│(clean) │   │   │
  ┌──────────────┐        │       │ Trigger    │  └────┬───┘ └───┬────┘   │   │
  │ Data Generator│       │       ▼            │       │         │         │   │
  │ (scripts/)   │───CSV──┼─▶  ┌──────────┐   │  ┌────▼─────────▼────┐   │   │
  └──────────────┘        │  │Databricks│──R/W─┼─▶│   Gold (aggr.)    │   │   │
                          │  │ PySpark  │      │  └───────────────────┘   │   │
                          │  └──────────┘      └────────────────────────────┘   │
                          │       │                               │              │
                          │       ▼                               ▼              │
                          │  ┌──────────┐               ┌──────────────┐        │
                          │  │ Unity    │               │  Analytics   │        │
                          │  │ Catalog  │               │  Consumers   │        │
                          │  └──────────┘               │(Power BI/API)│        │
                          │                              └──────────────┘        │
                          └─────────────────────────────────────────────────────┘
```

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Terraform | ≥ 1.5 | Infrastructure provisioning |
| Azure CLI | ≥ 2.50 | Authentication |
| Python | ≥ 3.10 | Notebooks, tests & data generation |
| Databricks CLI | ≥ 0.215 | Workspace management |

- An Azure subscription (even a free trial works)
- Sufficient permissions to create resource groups, storage, Databricks workspaces, and Data Factory instances
- A Service Principal with `Contributor` role on the target subscription

## Quick Start

### 1. Authenticate with Azure

```bash
az login
az account set --subscription <YOUR_SUBSCRIPTION_ID>
```

### 2. Provision Infrastructure

```bash
cd terraform
terraform init
terraform plan -var="environment=dev"
terraform apply -var="environment=dev" -auto-approve
```

### 3. Generate Data

The pipeline ships with a data generator that produces Big Data volumes:

```bash
pip install faker tqdm

# Full dataset: 7M orders, 200K customers, 500 products (~1.5 GB)
python scripts/generate_large_data.py

# Quick test: 10K orders, 1K customers
python scripts/generate_large_data.py --sample

# Custom counts
python scripts/generate_large_data.py --orders 5000000 --customers 100000
```

See [Data Generation](#data-generation) for details.

### 4. Upload Data

```bash
STORAGE_ACCOUNT="<STORAGE_ACCOUNT>"

az storage blob upload --account-name $STORAGE_ACCOUNT \
  --container-name bronze --name raw/orders.csv \
  --file data/sample_orders.csv --auth-mode login

az storage blob upload --account-name $STORAGE_ACCOUNT \
  --container-name bronze --name raw/sample_customers.csv \
  --file data/sample_customers.csv --auth-mode login

az storage blob upload --account-name $STORAGE_ACCOUNT \
  --container-name bronze --name raw/sample_products.csv \
  --file data/sample_products.csv --auth-mode login
```

### 5. Run Notebooks in Order

Open the Databricks workspace (URL from `terraform output`) and execute:

1. `notebooks/01_bronze_ingestion.py`
2. `notebooks/02_silver_transformation.py`
3. `notebooks/03_gold_aggregation.py`

Or trigger the ADF pipeline manually from the Azure portal.

### 6. Validate

```bash
cd tests
pytest -v --cov=../notebooks
```

## Local Development with Docker

Run the entire Medallion pipeline locally without Azure credentials. Docker Compose spins up MinIO (S3-compatible storage), a Spark cluster, and Jupyter — mirroring the Azure architecture on your machine.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│  Local Docker Environment                           │
│                                                     │
│  ┌──────────┐   ┌──────────┐   ┌───────────────┐  │
│  │  MinIO   │◄──│  Spark   │──►│   Jupyter     │  │
│  │ (S3 mock)│   │ Master + │   │  (notebooks)  │  │
│  │          │   │ Worker   │   │               │  │
│  └──────────┘   └──────────┘   └───────────────┘  │
│       │              │                │            │
│       ▼              ▼                ▼            │
│  ┌──────────────────────────────────────────────┐  │
│  │  MinIO Buckets: bronze/ silver/ gold/ logs/  │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Prerequisites

- Docker Desktop with **4 GB+ RAM** allocated
- ~2 GB free disk space for images and volumes

### Quick Start

```bash
# Start everything
./scripts/start_local.sh

# When done
./scripts/stop_local.sh
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9011 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9010 | `minioadmin` / `minioadmin` |
| Jupyter Lab | http://localhost:8889 | no token |
| GE Data Docs | http://localhost:8880 | `--profile docs` |
| Observability Dashboard | http://localhost:8502 | `--profile observability` |

### Running Notebooks

1. Open Jupyter at http://localhost:8889
2. Run notebooks in order:
   - `notebooks/01_bronze_ingestion.py` — reads CSVs from MinIO `bronze/` bucket
   - `notebooks/02_silver_transformation.py` — cleans and writes to `silver/`
   - `notebooks/03_gold_aggregation.py` — aggregates into `gold/`
3. Browse data in MinIO Console at http://localhost:9011

### Running Quality Checks

```bash
# All suites (includes observability tracking)
docker compose run --rm -T jupyter python scripts/run_quality_checks.py

# Single layer
docker compose run --rm -T jupyter python scripts/run_quality_checks.py --suite bronze
```

Quality results are saved to `data/quality_results/` and metrics to `data/quality_metrics/` for the observability dashboard.

### Viewing GE Data Docs

```bash
docker compose --profile docs up -d ge-docs
open http://localhost:8880
```

### Port Mapping

Ports are offset from the defaults to avoid conflicts with the streaming project:

| Service | Local Port | Default |
|---------|-----------|---------|
| MinIO S3 | 9010 | 9000 |
| MinIO Console | 9011 | 9001 |
| Spark Master UI | 8082 | 8080 |
| Spark Master RPC | 7078 | 7077 |
| Jupyter | 8889 | 8888 |

### Tear Down

```bash
# Stop services (preserves data)
./scripts/stop_local.sh

# Stop and delete all data
docker compose --profile docs down -v
```

## Data Generation

The `scripts/generate_large_data.py` script uses `Faker` (Spanish locale) to produce three CSV files with full referential integrity:

| File | Default Rows | Description |
|------|-------------|-------------|
| `data/sample_orders.csv` | 7,000,000 | Transactional orders with order_id, customer_id, product_id, quantity, unit_price, order_date, region |
| `data/sample_customers.csv` | 200,000 | Customer profiles with loyalty tiers (bronze/silver/gold/platinum) |
| `data/sample_products.csv` | 500 | Product catalog with categories and supplier countries |

**Key features:**
- Written in 500K-row chunks to keep memory under 2 GB
- `--sample` flag generates 10K orders for quick testing
- `--seed` flag for reproducible datasets
- Every `customer_id` and `product_id` in orders.csv references existing records
- Spanish cities, names, and product patterns

## Data Quality with Great Expectations

Data quality is enforced at every medallion layer boundary using [Great Expectations](https://greatexpectations.io/). Each layer has a dedicated expectation suite that validates data before it flows to the next stage.

### Validation Strategy

| Layer | Suite | What It Validates |
|-------|-------|-------------------|
| **Bronze** | `bronze_orders_suite` | Raw schema (10 columns), `order_id` uniqueness + format (`ORD-\d+`), null checks on key columns, quantity (1-50), unit_price (0.01-10000), valid Spanish regions |
| **Silver** | `silver_orders_suite` | All columns non-null, `order_id` unique, correct types (float/int), `total_amount == quantity * unit_price`, min 100 rows, quality flags in {valid, warning} |
| **Gold** | `gold_daily_sales_suite` | Non-zero rows, non-null date/region, positive revenue/orders/avg_value, unique (date, region) pairs, valid regions |

### Running Quality Checks Locally

```bash
pip install great-expectations pandas

# Run all suites
python scripts/run_quality_checks.py

# Run a single layer
python scripts/run_quality_checks.py --suite bronze
python scripts/run_quality_checks.py --suite silver
python scripts/run_quality_checks.py --suite gold
```

### In CI

The `quality` job in GitHub Actions runs automatically on every push/PR. It executes all three suites and uploads Data Docs as a build artifact for review.

### In Databricks Notebooks

Each notebook has commented Great Expectations validation blocks at the end. Uncomment them to enable quality gates that halt the pipeline on failure:

```python
import great_expectations as gx
context = gx.get_context()
checkpoint_result = context.run_checkpoint(
    checkpoint_name="bronze_checkpoint",
    batch_request={"runtime_parameters": {"path": bronze_path}}
)
if not checkpoint_result.success:
    raise ValueError("Bronze validation failed")
```

### Custom Expectations

- **`expect_column_values_to_be_valid_spanish_region`** — validates region names against a list of 42 Spanish cities/autonomous cities
- **`validate_layer_consistency`** — helper that checks silver row count is 90-100% of bronze, catching massive data loss during transformation

### Data Docs

After running checks locally, open the HTML report:

```bash
open great_expectations/uncommitted/data_docs/local_site/index.html
```

## Data Quality Observability

The `data_observability` package runs alongside Great Expectations to track quality metrics over time. After each GE validation run, a `QualityTracker` records:

- **Pass rate** — per-suite pass/fail trends across runs
- **Volume** — row counts with anomaly detection (>50% change flagged)
- **Freshness** — staleness of the latest `order_date` timestamp
- **Null rates** — per-column null percentages with threshold alerts
- **Schema drift** — column additions, removals, and type changes between runs

### Dashboard

A Streamlit dashboard visualizes all collected metrics:

| Panel | Shows |
|-------|-------|
| Health Overview | Traffic-light status per dataset (healthy / warning / critical) |
| Pass Rate Trend | Daily pass rate line chart with 95% target threshold |
| Volume Trends | Row count over time with anomaly markers |
| Data Freshness | Staleness cards per dataset (fresh / stale / critical) |
| Null Rates | Bar chart per column, red-flagged if above threshold |
| Schema Drift | Added/removed/typed-changed columns since last run |
| Recent Failures | Table of last 100 failed expectations |

### Launching

```bash
# Via Docker (recommended)
docker-compose --profile observability up observability
# Opens at http://localhost:8502

# Standalone
pip install streamlit plotly pandas
python scripts/observability_dashboard.py
# Opens at http://localhost:8501
```

### Data Storage

- `data/quality_results/` — JSON files per GE validation run
- `data/quality_metrics/` — JSONL files organized by metric type (volume/, freshness/, null_rates/, schema/)

These directories are gitignored and persist across container restarts via the mounted `./data` volume.

## Cost Estimate (Dev Environment)

| Resource | Tier | Est. Monthly Cost |
|----------|------|-------------------|
| Databricks Workspace | Standard | $25–40 |
| ADLS Gen2 | Hot tier, < 1 GB | $1–2 |
| Azure Data Factory | ~50 pipeline runs | $5–10 |
| VNet / Private Endpoints | 1 endpoint | $15–20 |
| **Total** | | **~$50–80/month** |

> Shut down the Databricks compute cluster when not in use to minimize costs.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Infrastructure | Terraform, Azure Resource Manager |
| Storage | Azure Data Lake Storage Gen2 |
| Ingestion | Azure Data Factory |
| Processing | Databricks (PySpark) |
| Lakehouse Format | Delta Lake |
| Data Quality | Great Expectations, pytest, data_observability |
| CI/CD | GitHub Actions |
| Linting | tflint, ruff |

## Project Structure

```
azure-medallion-pipeline/
├── README.md
├── docker-compose.yml           # Local dev environment (MinIO + Spark + Jupyter)
├── Dockerfile                   # Multi-purpose pipeline image
├── requirements.txt             # Python dependencies
├── scripts/
│   ├── start_local.sh           # Start local environment
│   ├── stop_local.sh            # Stop local environment
│   ├── generate_large_data.py   # Faker-based data generator (7M+ rows)
│   ├── run_quality_checks.py    # GE quality checks + observability tracking
│   └── observability_dashboard.py  # Standalone dashboard launcher
├── src/
│   └── data_observability/      # Reusable quality monitoring package
│       ├── __init__.py
│       ├── tracker.py           # QualityTracker (ties GE to metrics)
│       ├── metrics.py           # DataMetrics (freshness, volume, schema, nulls)
│       ├── result_store.py      # ResultStore (persist validation results)
│       └── dashboard.py         # Streamlit dashboard
├── great_expectations/
│   ├── great_expectations.yml   # GE config (datasources, stores, docs)
│   ├── expectations/
│   │   ├── bronze_orders_suite.json
│   │   ├── silver_orders_suite.json
│   │   └── gold_daily_sales_suite.json
│   ├── checkpoints/
│   │   ├── bronze_checkpoint.yml
│   │   ├── silver_checkpoint.yml
│   │   └── gold_checkpoint.yml
│   └── plugins/
│       └── custom_expectations.py
├── terraform/                    # Infrastructure as Code
│   ├── main.tf                   # Resource group & core config
│   ├── variables.tf              # Input variables
│   ├── outputs.tf                # Output values
│   ├── providers.tf              # Azure provider
│   ├── storage.tf                # ADLS Gen2 + containers
│   ├── databricks.tf             # Databricks workspace
│   ├── data_factory.tf           # ADF + linked services
│   └── networking.tf             # VNet & private endpoints
├── notebooks/                    # Databricks PySpark notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregation.py
├── adf/
│   └── pipeline_ingestion.json
├── tests/                        # Pytest unit tests
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── data/
│   ├── sample_orders.csv         # Generated: 7M+ orders
│   ├── sample_customers.csv      # Generated: 200K customers
│   ├── sample_products.csv       # Generated: 500 products
│   ├── quality_results/          # GE validation run JSONs (gitignored)
│   └── quality_metrics/          # Observability metrics JSONL (gitignored)
├── .github/workflows/
│   └── ci.yml                    # GitHub Actions CI
├── .tflint.hcl
├── .gitignore
└── requirements.txt
```

## License

MIT
