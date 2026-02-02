#!/bin/bash
# Start Kafka cluster for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting Kafka cluster..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 5

# Wait for Kafka to be ready
echo "Checking Kafka health..."
MAX_ATTEMPTS=30
ATTEMPT=0
while ! docker exec kafka-test-broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        echo "Kafka failed to start after $MAX_ATTEMPTS attempts"
        exit 1
    fi
    echo "Waiting for Kafka to be ready... (attempt $ATTEMPT/$MAX_ATTEMPTS)"
    sleep 2
done

echo ""
echo "=========================================="
echo "Kafka cluster is up and running!"
echo "=========================================="
echo ""
echo "Services:"
echo "  - Kafka Broker:     localhost:9092"
echo "  - Zookeeper:        localhost:2181"
echo "  - Kafka UI:         http://localhost:8080"
echo "  - Schema Registry:  http://localhost:8081"
echo ""
echo "To produce mock data, run:"
echo "  cd producer && pip install -r requirements.txt"
echo "  python financial_data_producer.py --rate 5"
echo ""
echo "To stop the cluster, run: ./stop.sh"
