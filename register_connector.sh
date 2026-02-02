#!/bin/bash
# Register the Snowflake Kafka Connector

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Read the private key from existing Snow CLI config location
PRIVATE_KEY=$(cat ~/.ssh/snowflake_rsa_key | grep -v "PRIVATE KEY" | tr -d '\n')

# Snowflake account details - using org-account format with dashes (underscores can cause issues)
SNOWFLAKE_URL="sfseeurope-eu-demo241.snowflakecomputing.com"
SNOWFLAKE_USER="teiko"

echo "Waiting for Kafka Connect to be ready..."
MAX_ATTEMPTS=60
ATTEMPT=0
while ! curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        echo "Kafka Connect failed to start after $MAX_ATTEMPTS attempts"
        exit 1
    fi
    echo "Waiting for Kafka Connect... (attempt $ATTEMPT/$MAX_ATTEMPTS)"
    sleep 5
done

echo ""
echo "Kafka Connect is ready!"
echo ""

# Check if connector already exists
if curl -s http://localhost:8083/connectors/snowflake-sink | grep -q "snowflake-sink"; then
    echo "Connector 'snowflake-sink' already exists. Deleting..."
    curl -X DELETE http://localhost:8083/connectors/snowflake-sink
    sleep 2
fi

echo "Registering Snowflake Kafka Connector..."

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "snowflake-sink",
    "config": {
        "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
        "tasks.max": "1",
        "topics": "financial_transactions",
        "snowflake.url.name": "'"${SNOWFLAKE_URL}"'",
        "snowflake.user.name": "'"${SNOWFLAKE_USER}"'",
        "snowflake.private.key": "'"${PRIVATE_KEY}"'",
        "snowflake.database.name": "KAFKA_DEMO",
        "snowflake.schema.name": "FINANCIAL_DATA",
        "snowflake.role.name": "ACCOUNTADMIN",
        "snowflake.topic2table.map": "financial_transactions:RAW_FINANCIAL_TRANSACTIONS",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "buffer.count.records": "100",
        "buffer.flush.time": "10",
        "buffer.size.bytes": "1000000",
        "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true"
    }
}'

echo ""
echo ""
echo "Connector registered! Checking status..."
sleep 3

curl -s http://localhost:8083/connectors/snowflake-sink/status | python3 -m json.tool

echo ""
echo "=========================================="
echo "Snowflake Kafka Connector is configured!"
echo "=========================================="
echo ""
echo "Monitor connector: curl http://localhost:8083/connectors/snowflake-sink/status"
echo "View in Kafka UI: http://localhost:8080"
echo ""
echo "Check data in Snowflake:"
echo "  SELECT COUNT(*) FROM KAFKA_DEMO.FINANCIAL_DATA.RAW_FINANCIAL_TRANSACTIONS;"
echo "  SELECT * FROM KAFKA_DEMO.FINANCIAL_DATA.FINANCIAL_TRANSACTIONS_V LIMIT 10;"
