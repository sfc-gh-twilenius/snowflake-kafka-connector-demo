-- ============================================================================
-- Snowflake Kafka Connector Setup
-- ============================================================================
-- This script sets up the Snowflake objects needed for the Kafka connector
-- to ingest financial transaction data.
-- ============================================================================

-- 1. Create Database and Schema for Kafka data
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS KAFKA_DEMO;
USE DATABASE KAFKA_DEMO;

CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA;
USE SCHEMA FINANCIAL_DATA;

-- 2. Create landing table for raw Kafka messages
-- ----------------------------------------------------------------------------
-- The Kafka connector writes data with RECORD_CONTENT (the message) and 
-- RECORD_METADATA (Kafka metadata like partition, offset, timestamp)
CREATE OR REPLACE TABLE RAW_FINANCIAL_TRANSACTIONS (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);

-- 3. Create a view to parse the JSON transaction data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW FINANCIAL_TRANSACTIONS_V AS
SELECT
    RECORD_CONTENT:transaction_id::STRING AS transaction_id,
    RECORD_CONTENT:timestamp::TIMESTAMP_TZ AS transaction_timestamp,
    RECORD_CONTENT:account_id::STRING AS account_id,
    RECORD_CONTENT:transaction_type::STRING AS transaction_type,
    RECORD_CONTENT:amount::DECIMAL(18,2) AS amount,
    RECORD_CONTENT:currency::STRING AS currency,
    RECORD_CONTENT:status::STRING AS status,
    
    -- Location fields
    RECORD_CONTENT:location:city::STRING AS location_city,
    RECORD_CONTENT:location:country::STRING AS location_country,
    RECORD_CONTENT:location:latitude::FLOAT AS location_latitude,
    RECORD_CONTENT:location:longitude::FLOAT AS location_longitude,
    
    -- Merchant fields (null for non-purchase transactions)
    RECORD_CONTENT:merchant:merchant_id::STRING AS merchant_id,
    RECORD_CONTENT:merchant:merchant_name::STRING AS merchant_name,
    RECORD_CONTENT:merchant:merchant_category::STRING AS merchant_category,
    RECORD_CONTENT:merchant:mcc_code::STRING AS mcc_code,
    
    -- Card info (null for non-card transactions)
    RECORD_CONTENT:card_info:card_last_four::STRING AS card_last_four,
    RECORD_CONTENT:card_info:card_network::STRING AS card_network,
    RECORD_CONTENT:card_info:card_type::STRING AS card_type,
    
    -- Transfer-specific fields
    RECORD_CONTENT:recipient_account_id::STRING AS recipient_account_id,
    RECORD_CONTENT:transfer_reference::STRING AS transfer_reference,
    
    -- Metadata
    RECORD_CONTENT:metadata:channel::STRING AS channel,
    RECORD_CONTENT:metadata:device_id::STRING AS device_id,
    RECORD_CONTENT:metadata:ip_address::STRING AS ip_address,
    
    -- Kafka metadata
    RECORD_METADATA:offset::INTEGER AS kafka_offset,
    RECORD_METADATA:partition::INTEGER AS kafka_partition,
    RECORD_METADATA:topic::STRING AS kafka_topic,
    RECORD_METADATA:CreateTime::TIMESTAMP_NTZ AS kafka_create_time,
    
    -- Raw data for debugging
    RECORD_CONTENT AS raw_content,
    RECORD_METADATA AS raw_metadata
FROM RAW_FINANCIAL_TRANSACTIONS;

-- 4. Create aggregation views for analytics
-- ----------------------------------------------------------------------------

-- Transaction summary by type
CREATE OR REPLACE VIEW TRANSACTION_SUMMARY_BY_TYPE AS
SELECT
    transaction_type,
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM FINANCIAL_TRANSACTIONS_V
GROUP BY transaction_type, currency;

-- Transaction summary by merchant category
CREATE OR REPLACE VIEW TRANSACTION_SUMMARY_BY_MERCHANT AS
SELECT
    merchant_category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    COUNT(DISTINCT account_id) AS unique_customers
FROM FINANCIAL_TRANSACTIONS_V
WHERE merchant_category IS NOT NULL
GROUP BY merchant_category;

-- Hourly transaction volume
CREATE OR REPLACE VIEW HOURLY_TRANSACTION_VOLUME AS
SELECT
    DATE_TRUNC('HOUR', transaction_timestamp) AS hour,
    transaction_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM FINANCIAL_TRANSACTIONS_V
GROUP BY DATE_TRUNC('HOUR', transaction_timestamp), transaction_type
ORDER BY hour DESC;

-- ============================================================================
-- KAFKA CONNECTOR CONFIGURATION
-- ============================================================================
-- To set up the Snowflake Kafka Connector, you need to:
--
-- 1. Create a key pair for authentication:
--    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- 2. Assign the public key to your Snowflake user:
--    ALTER USER <your_user> SET RSA_PUBLIC_KEY='<public_key_content>';
--
-- 3. Configure the Kafka Connect properties file with these settings:
-- ============================================================================

-- Example connector configuration (save as snowflake-connector.properties):
/*
name=snowflake-kafka-connector
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=1

# Kafka settings
topics=financial_transactions
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

# Snowflake connection settings
snowflake.url.name=<your_account>.snowflakecomputing.com
snowflake.user.name=<your_user>
snowflake.private.key=<your_private_key>
snowflake.database.name=KAFKA_DEMO
snowflake.schema.name=FINANCIAL_DATA

# Table mapping
snowflake.topic2table.map=financial_transactions:RAW_FINANCIAL_TRANSACTIONS

# Buffer settings
buffer.count.records=10000
buffer.flush.time=60
buffer.size.bytes=5000000

# Error handling
errors.tolerance=all
errors.log.enable=true
errors.log.include.messages=true
*/

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check if data is flowing
-- SELECT COUNT(*) FROM RAW_FINANCIAL_TRANSACTIONS;

-- Check latest transactions
-- SELECT * FROM FINANCIAL_TRANSACTIONS_V ORDER BY transaction_timestamp DESC LIMIT 10;

-- Check Kafka lag (difference between Kafka timestamp and current time)
-- SELECT 
--     kafka_topic,
--     kafka_partition,
--     MAX(kafka_offset) as max_offset,
--     MAX(kafka_create_time) as latest_kafka_time,
--     DATEDIFF('second', MAX(kafka_create_time), CURRENT_TIMESTAMP()) as lag_seconds
-- FROM FINANCIAL_TRANSACTIONS_V
-- GROUP BY kafka_topic, kafka_partition;
