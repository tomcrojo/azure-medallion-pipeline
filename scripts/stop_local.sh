#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

GREEN='\033[0;32m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $*"; }

cd "$PROJECT_DIR"

log "Stopping local Medallion pipeline environment..."
docker compose --profile docs down

log "All services stopped."
echo ""
echo "To remove persistent volumes (MinIO data, Spark apps):"
echo "  docker compose --profile docs down -v"
