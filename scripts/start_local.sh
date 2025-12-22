#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cd "$PROJECT_DIR"

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    err "Docker is not running. Start Docker Desktop and try again."
    exit 1
fi

# Check available memory (recommend 4GB+)
if command -v docker &> /dev/null; then
    MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    MEM_GB=$((MEM_BYTES / 1073741824))
    if [ "$MEM_GB" -lt 4 ] && [ "$MEM_GB" -ne 0 ]; then
        warn "Docker has ${MEM_GB}GB RAM allocated. Recommend 4GB+ for stable operation."
    fi
fi

log "Starting local Medallion pipeline environment..."

# Bring up core services (not docs profile)
docker compose up -d minio minio-init spark-master spark-worker jupyter

log "Waiting for MinIO to be healthy..."
RETRIES=30
until docker compose exec -T minio mc ready local > /dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
        err "MinIO failed to start within timeout."
        docker compose logs minio
        exit 1
    fi
    sleep 2
done
log "MinIO is healthy."

log "Uploading sample data to bronze bucket..."
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin

for f in data/sample_orders.csv data/sample_customers.csv data/sample_products.csv; do
    if [ -f "$f" ]; then
        BASENAME=$(basename "$f")
        docker compose cp "$f" minio:/tmp/"$BASENAME"
        docker compose exec -T minio mc cp /tmp/"$BASENAME" local/bronze/raw/"$BASENAME"
        log "  Uploaded $BASENAME -> bronze/raw/"
    else
        warn "  $f not found, skipping. Run 'python scripts/generate_large_data.py --sample' to generate."
    fi
done

echo ""
log "Local environment is ready!"
echo ""
echo "  Service            URL                          Credentials"
echo "  ─────────────────  ────────────────────────────  ──────────────────"
echo "  Spark Master UI    http://localhost:8082         (none)"
echo "  MinIO Console      http://localhost:9011         minioadmin / minioadmin"
echo "  MinIO S3 API       http://localhost:9010         minioadmin / minioadmin"
echo "  Jupyter Lab        http://localhost:8889         (no token)"
echo ""
echo "  GE Data Docs       docker compose --profile docs up -d ge-docs"
echo "                     then open http://localhost:8880"
echo ""
echo "  Observability:     http://localhost:8502 (--profile observability)"
echo ""
echo "Next steps:"
echo "  1. Open Jupyter at http://localhost:8889"
echo "  2. Run notebooks in order:"
echo "     - notebooks/01_bronze_ingestion.py"
echo "     - notebooks/02_silver_transformation.py"
echo "     - notebooks/03_gold_aggregation.py"
echo "  3. Browse data in MinIO console at http://localhost:9011"
echo "  4. Run quality checks: docker compose run --rm -T jupyter python scripts/run_quality_checks.py"
echo ""
echo "To stop: ./scripts/stop_local.sh"
