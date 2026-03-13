# CDC SCD Type 2 Pipeline

Real-time Change Data Capture pipeline with SCD Type 2 history tracking for gaming analytics.

## Architecture

```
PostgreSQL RDS → Debezium → MSK Kafka → S3 → Redshift Serverless (SCD2)
```

## Components

| Layer | Technology | Purpose |
|-------|------------|---------|
| Source | PostgreSQL RDS | OLTP database with logical replication |
| CDC | Debezium | Capture changes from WAL logs |
| Streaming | AWS MSK | Kafka message broker |
| Storage | S3 | Data lake (JSON files) |
| Analytics | Redshift Serverless | SCD Type 2 dimension tables |

---

## Detailed Setup Guide

### Prerequisites
- AWS account with RDS, MSK, S3, Redshift Serverless
- Python 3.8+ with `psycopg`, `faker`, `python-dotenv` packages
- `jq` CLI tool for JSON processing
- AWS CLI v2 configured with appropriate permissions

---

### Step 1: PostgreSQL CDC Configuration

#### 1.1 Enable Logical Replication on RDS

Create a custom parameter group and set:
```
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
rds.logical_replication = 1
```

Apply parameter group to RDS instance and **reboot** the instance.

#### 1.2 Create Database Schema and Tables

```bash
# Connect to RDS PostgreSQL
psql -h <rds-endpoint> -U postgres -d postgres

# Run setup script
\i src/sql/postgres-cdc-setup.sql
```

This creates:
- Schema: `gaming_oltp`
- Tables: `dim_user`, `dim_game`, `dim_session`, `fact_game_event`
- Publication: `my_cdc_pub` (for all tables)
- Replication slot will be created by Debezium

#### 1.3 Verify CDC Setup

```sql
-- Check publication exists
SELECT * FROM pg_publication WHERE pubname = 'my_cdc_pub';

-- Check tables in publication
SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'my_cdc_pub';

-- Verify wal_level
SHOW wal_level;  -- Should return 'logical'
```

---

### Step 2: MSK Connect Plugin Setup

MSK Connect requires custom plugins (ZIP files) uploaded to S3.

#### 2.1 Download Connector Plugins

```bash
# Create plugins directory
mkdir -p connect-plugins && cd connect-plugins

# Download Debezium PostgreSQL connector
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.0.Final/debezium-connector-postgres-2.7.0.Final-plugin.zip

# Download Confluent S3 Sink connector
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc-kafka-connect-s3/versions/10.5.0/confluentinc-kafka-connect-s3-10.5.0.zip

cd ..
```

#### 2.2 Upload Plugins to S3

```bash
# Create S3 bucket for plugins
aws s3 mb s3://msk-connect-plugins-<account-id> --region us-east-1

# Upload plugins
aws s3 cp connect-plugins/debezium-connector-postgres-2.7.0.Final-plugin.zip \
    s3://msk-connect-plugins-<account-id>/debezium-postgres-plugin.zip

aws s3 cp connect-plugins/confluentinc-kafka-connect-s3-10.5.0.zip \
    s3://msk-connect-plugins-<account-id>/confluent-s3-sink-plugin.zip
```

#### 2.3 Create Custom Plugins in MSK Connect

```bash
# Create Debezium PostgreSQL plugin
aws kafkaconnect create-custom-plugin \
    --name "debezium-postgres-2.7" \
    --location S3Location="{bucketArn=arn:aws:s3:::msk-connect-plugins-<account-id>,fileKey=debezium-postgres-plugin.zip}" \
    --region us-east-1

# Create Confluent S3 Sink plugin
aws kafkaconnect create-custom-plugin \
    --name "confluent-s3-sink-10.5" \
    --location S3Location="{bucketArn=arn:aws:s3:::msk-connect-plugins-<account-id>,fileKey=confluent-s3-sink-plugin.zip}" \
    --region us-east-1
```

Wait for plugins to become `ACTIVE` (check with `aws kafkaconnect list-custom-plugins`).

---

### Step 3: Create IAM Roles

#### 3.1 MSK Connect Execution Role

Create `msk-connect-execution-role.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "kafkaconnect.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Create role with permissions:
```bash
# Create role
aws iam create-role \
    --role-name MSKConnectExecutionRole \
    --assume-role-policy-document file://msk-connect-execution-role.json

# Attach policies (S3, CloudWatch Logs, MSK)
aws iam attach-role-policy --role-name MSKConnectExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name MSKConnectExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

#### 3.2 Redshift IAM Role

For Redshift to read from S3:
```bash
aws iam create-role \
    --role-name RedshiftS3ReadRole \
    --assume-role-policy-document file://redshift-trust-policy.json

aws iam attach-role-policy --role-name RedshiftS3ReadRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
```

Associate role with Redshift namespace in console.

---

### Step 4: Deploy Connectors

#### 4.1 Create Debezium Source Connector

Using the provided script:
```bash
./create-postgres-source-connector.sh
```

Or manually via AWS Console:
1. Go to MSK Connect → Create connector
2. Select custom plugin: `debezium-postgres-2.7`
3. Configuration from `config/pg-debezium-source-connector-config.properties`
4. Set network: VPC, subnets, security group (same as MSK cluster)
5. Set IAM role: `MSKConnectExecutionRole`

#### 4.2 Create S3 Sink Connector

```bash
./create-s3-sink-connector.sh
```

Configuration from `config/s3-sink-connector-config.properties`.

#### 4.3 Verify Connectors Running

```bash
aws kafkaconnect list-connectors --region us-east-1
```

Both connectors should show `RUNNING` state.

---

### Step 5: Test with Data Simulator

#### 5.1 Configure Simulator

Edit `config/data-simulator.env`:
```properties
DB_HOST=<your-rds-endpoint>
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=<your-password>
DB_SCHEMA=gaming_oltp

# Simulation settings
CYCLE_DELAY=0.5          # Seconds between operations
LOG_LEVEL=INFO
STATS_INTERVAL=30        # Print stats every 30 seconds

# Operation probabilities (should sum to ~1.0)
PROB_NEW_USER=0.08
PROB_UPDATE_USER=0.25
PROB_START_SESSION=0.15
PROB_END_SESSION=0.12
PROB_GENERATE_EVENTS=0.40
```

#### 5.2 Install Dependencies

```bash
pip install psycopg[binary] faker python-dotenv
```

#### 5.3 Run Simulator

```bash
python data-simulator.py
```

Output:
```
============================================================
🎮 Gaming Data Simulator for CDC Testing
📍 Database: <rds-endpoint>
📊 Stats interval: 30 seconds
============================================================
Press Ctrl+C to stop

✓ Connected to <rds-endpoint>
✓ Loaded: 5 users, 5 games, 2 active sessions
→ New user: johnsmith1234 from Germany
→ Updated jdoe99: +1.5hrs
→ jdoe99 started playing Space Warriors on PC
...
```

#### 5.4 Verify Data Flow

```bash
# Check Kafka topics have messages
kafka-console-consumer.sh --bootstrap-server <broker>:9098 \
    --topic cdc-gaming.gaming_oltp.dim_user \
    --from-beginning --command-config client.properties

# Check S3 files
aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive
```

---

### Step 6: Redshift SCD2 Tables

#### 6.1 Upload JSONPath Files

```bash
aws s3 cp src/json/jsonpath-dim-user.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-dim-game.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-dim-session.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-fact-game-event.json s3://gaming-cdc-data-lake-2025/jsonpath/
```

#### 6.2 Create Tables and Load Data

Run in Redshift Query Editor:
```sql
-- Create schema and tables
\i src/sql/redshift-serverless-scd2.sql
```

This creates:
- Schema: `bronze`
- Staging tables: `stg_dim_user`, `stg_dim_game`, `stg_dim_session`, `stg_fact_game_event`
- SCD2 dimension tables: `dim_user`, `dim_game`, `dim_session`
- Fact table: `fact_game_event`

#### 6.3 Execute SCD2 Merge

The SQL file includes COPY commands and SCD2 merge logic. Run the entire script to:
1. Load JSON from S3 to staging tables
2. Deduplicate by `__source_lsn`
3. Close expired records
4. Insert new versions
5. Handle deletes

## SCD Type 2 Implementation

### Dimension Tables
Each dimension table includes:
- `valid_from` - version start timestamp
- `valid_to` - version end timestamp (NULL = current)
- `is_current` - boolean flag for current version

### Operations
| CDC Op | Action |
|--------|--------|
| `c`/`r` (create/read) | Insert new record |
| `u` (update) | Close current, insert new version |
| `d` (delete) | Close current record |

## Project Structure

```
cdc-scd2-pipeline/
├── config/                              # Configuration files
│   ├── pg-debezium-source-connector-config.properties
│   ├── s3-sink-connector-config.properties
│   ├── client.properties
│   └── data-simulator.env
├── docs/                                # Documentation
│   ├── architecture.md
│   ├── runbook.md
│   ├── SCD2-DELETE-HANDLING-EXPLAINED.md
│   ├── SCD2-TRACE-EXAMPLE.md
│   └── SQL-VALIDATION-CHECKLIST.md
├── src/
│   ├── json/                            # JSONPath mappings for Redshift
│   │   ├── jsonpath-dim-user.json
│   │   ├── jsonpath-dim-game.json
│   │   ├── jsonpath-dim-session.json
│   │   └── jsonpath-fact-game-event.json
│   ├── python/                          # Python scripts
│   │   ├── data-simulator.py
│   │   └── utils/
│   ├── scripts/                          # Shell scripts
│   │   ├── create-postgres-source-connector.sh
│   │   ├── create-s3-sink-connector.sh
│   │   └── ...
│   └── sql/                              # SQL scripts
│       ├── postgres-cdc-setup.sql
│       └── redshift-serverless-scd2.sql
├── readme.md
└── requirements.txt
```

## Testing

Run the data simulator to generate test events:
```bash
python src/python/data-simulator.py --env config/data-simulator.env
```

## Query Examples

### Current State
```sql
SELECT * FROM bronze.dim_user WHERE is_current = TRUE;
```

### Point-in-Time
```sql
SELECT * FROM bronze.dim_user
WHERE user_id = 'xxx'
  AND valid_from <= '2025-01-01'
  AND (valid_to > '2025-01-01' OR valid_to IS NULL);
```

## Monitoring

- **Debezium:** CloudWatch Logs `/aws/msk-connect/PGSourceConnector`
- **S3 Sink:** CloudWatch Logs `/aws/msk-connect/S3SinkConnector`
- **Replication Lag:** `pg_replication_slots` view in PostgreSQL
