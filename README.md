# Kafka Connector Test - Snowpipe Streaming

A local Kafka cluster setup for testing the Snowflake Kafka Connector with **SNOWPIPE_STREAMING** ingestion method. Generates mock financial transaction data and streams it directly into Snowflake tables.

## Architecture

```
┌─────────────────┐    ┌─────────────┐    ┌──────────────────┐    ┌────────────┐
│ Python Producer │───>│    Kafka    │───>│  Kafka Connect   │───>│ Snowflake  │
│ (Mock Data)     │    │   Broker    │    │ (SF Connector)   │    │   Table    │
└─────────────────┘    └─────────────┘    └──────────────────┘    └────────────┘
```

## Prerequisites

- Docker Desktop
- Python 3.8+
- Snowflake account with key-pair authentication configured
- RSA private key at `~/.ssh/snowflake_rsa_key`

## Quick Start

### 1. Start Kafka Infrastructure

```bash
docker compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka Broker (port 9092)
- Schema Registry (port 8081)
- Kafka Connect with Snowflake Connector v3.5.3 (port 8083)
- Kafka UI (port 8080)

### 2. Setup Snowflake Objects

Run the SQL in `snowflake/setup_connector.sql` or execute:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS KAFKA_DEMO;
CREATE SCHEMA IF NOT EXISTS KAFKA_DEMO.FINANCIAL_DATA;

-- Create target table (Snowpipe Streaming format)
CREATE OR REPLACE TABLE KAFKA_DEMO.FINANCIAL_DATA.RAW_FINANCIAL_TRANSACTIONS (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT
);
```

### 3. Register the Connector

```bash
./register_connector.sh
```

### 4. Start Producing Data

```bash
cd producer

# Install dependencies (first time only)
pip install kafka-python faker

# Send a burst of transactions
python financial_data_producer.py --burst 100

# Or continuous streaming at N transactions per second
python financial_data_producer.py --rate 2
```

## Configuration

### RSA Key Pair Setup

The Snowflake Kafka Connector requires key-pair authentication. Follow these steps to set it up:

**1. Generate an RSA key pair:**

```bash
# Create the .ssh directory if it doesn't exist
mkdir -p ~/.ssh

# Generate a 2048-bit RSA private key (unencrypted for connector use)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake_rsa_key -nocrypt

# Generate the public key from the private key
openssl rsa -in ~/.ssh/snowflake_rsa_key -pubout -out ~/.ssh/snowflake_rsa_key.pub

# Set appropriate permissions on the private key
chmod 600 ~/.ssh/snowflake_rsa_key
```

**2. Get the public key content (without headers):**

```bash
grep -v "PUBLIC KEY" ~/.ssh/snowflake_rsa_key.pub | tr -d '\n'
```

**3. Assign the public key to your Snowflake user:**

```sql
ALTER USER <YOUR_USERNAME> SET RSA_PUBLIC_KEY='<paste_public_key_content_here>';
```

**4. Verify key-pair authentication works:**

```sql
DESCRIBE USER <YOUR_USERNAME>;
-- Check that RSA_PUBLIC_KEY_FP shows a fingerprint
```

### Snowflake Connection

Edit `register_connector.sh` to update:

```bash
SNOWFLAKE_URL="<YOUR_ORG>-<YOUR_ACCOUNT>.snowflakecomputing.com"  # org-account format
SNOWFLAKE_USER="<YOUR_USERNAME>"
SNOWFLAKE_ROLE="<YOUR_ROLE>"  # e.g., ACCOUNTADMIN
```

The connector reads the RSA private key from `~/.ssh/snowflake_rsa_key`.

### Connector Properties

Key settings in the connector configuration:

| Property | Value | Description |
|----------|-------|-------------|
| `snowflake.ingestion.method` | `SNOWPIPE_STREAMING` | Uses streaming ingest API |
| `snowflake.role.name` | `<YOUR_ROLE>` | Role for ingestion |
| `buffer.flush.time` | `10` | Flush interval in seconds |
| `buffer.count.records` | `100` | Records before flush |

## Monitoring

### Connector Status

```bash
curl http://localhost:8083/connectors/snowflake-sink/status | jq
```

### Kafka UI

Open http://localhost:8080 to view topics, messages, and consumer groups.

### Snowflake Queries

```sql
-- Row count
SELECT COUNT(*) FROM KAFKA_DEMO.FINANCIAL_DATA.RAW_FINANCIAL_TRANSACTIONS;

-- Latest transactions
SELECT 
    RECORD_CONTENT:transaction_id::string as txn_id,
    RECORD_CONTENT:transaction_type::string as type,
    RECORD_CONTENT:amount::number(10,2) as amount,
    RECORD_CONTENT:currency::string as currency,
    RECORD_CONTENT:status::string as status
FROM KAFKA_DEMO.FINANCIAL_DATA.RAW_FINANCIAL_TRANSACTIONS
ORDER BY RECORD_METADATA:offset DESC
LIMIT 10;

-- Streaming channel status
SHOW CHANNELS IN TABLE KAFKA_DEMO.FINANCIAL_DATA.RAW_FINANCIAL_TRANSACTIONS;

-- Historical metrics (2+ hour latency)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY
ORDER BY CREATED_ON DESC LIMIT 10;
```

## Mock Data Schema

The producer generates financial transactions with the following structure:

```json
{
  "transaction_id": "uuid",
  "transaction_type": "purchase|withdrawal|transfer|deposit|refund",
  "amount": 123.45,
  "currency": "USD|EUR|GBP|...",
  "account_id": "ACC-123456-X",
  "status": "completed|pending|failed",
  "timestamp": "2024-01-29T12:00:00Z",
  "merchant": {
    "merchant_id": "MER-ABC123",
    "merchant_name": "Store Name",
    "merchant_category": "Retail",
    "mcc_code": "5411"
  },
  "card_info": {
    "card_type": "credit|debit",
    "card_network": "Visa|Mastercard|Amex",
    "card_last_four": "1234"
  },
  "location": {
    "city": "City Name",
    "country": "US",
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "metadata": {
    "channel": "web|mobile_app|pos_terminal|branch",
    "device_id": "abc123",
    "ip_address": "192.168.1.1"
  }
}
```

## File Structure

```
kafka-connector-test/
├── docker-compose.yml          # Kafka infrastructure
├── register_connector.sh       # Connector registration script
├── start.sh                    # Start all services
├── stop.sh                     # Stop all services
├── producer/
│   ├── financial_data_producer.py  # Mock data generator
│   └── requirements.txt
├── secrets/                    # Mounted to Kafka Connect (optional)
└── snowflake/
    └── setup_connector.sql     # Snowflake DDL
```

## Troubleshooting

### Connector Task Failed

Check logs:
```bash
docker logs kafka-test-connect 2>&1 | tail -50
```

Common issues:
- **"Database does not exist or is not authorized"**: Ensure using connector v3.5.3+ and org-account URL format
- **Authentication errors**: Verify RSA key is correct and user has key-pair auth enabled

### No Data in Snowflake

1. Check connector status: `curl http://localhost:8083/connectors/snowflake-sink/status`
2. Verify topic has messages: Check Kafka UI at http://localhost:8080
3. Check Kafka Connect logs for errors

### Restart Connector

```bash
# Delete and recreate
curl -X DELETE http://localhost:8083/connectors/snowflake-sink
./register_connector.sh
```

## Cleanup

```bash
# Stop all containers
docker compose down

# Remove volumes (deletes all Kafka data)
docker compose down -v
```

## Key Learnings

1. **Connector Version Matters**: v2.x had issues with SNOWPIPE_STREAMING; use v3.5.3+
2. **URL Format**: Use `orgname-accountname.snowflakecomputing.com` (not the account locator format)
3. **No COPY History**: Snowpipe Streaming writes directly to tables, no staging involved
4. **Monitoring Latency**: Account Usage views have ~2 hour delay; use `SHOW CHANNELS` for real-time status
