#!/bin/bash
# Stop Kafka cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping Kafka cluster..."
docker compose down

echo ""
echo "Kafka cluster stopped."
echo ""
echo "To also remove volumes (clears all data), run:"
echo "  docker compose down -v"
