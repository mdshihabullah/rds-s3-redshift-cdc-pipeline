# CDC SCD2 Pipeline Runbook

Complete operational guide for building, running, and troubleshooting the CDC SCD Type 2 pipeline.

---

## Table of Contents

1. [Infrastructure Setup](#infrastructure-setup)
2. [Daily Operations](#daily-operations)
3. [Troubleshooting](#troubleshooting)
4. [Maintenance](#maintenance)
5. [Emergency Procedures](#emergency-procedures)

---

## Infrastructure Setup

### AWS Resources Required

| Resource | Type | Purpose |
|----------|------|---------|
| RDS PostgreSQL | db.t4g.micro | Source database |
| MSK Cluster | Provisioned | Kafka broker |
| S3 Bucket | Standard | Data lake |
| Redshift Serverless | Workgroup | Analytics |
| IAM Roles | - | Service permissions |

### Step 1: RDS PostgreSQL Setup

#### 1.1 Create Parameter Group for Logical Replication

```bash
# Create custom parameter group
aws rds create-db-parameter-group \
    --db-parameter-group-name cdc-postgres-params \
    --db-parameter-group-family postgres14 \
    --description "CDC parameters for PostgreSQL"

# Set logical replication parameters
aws rds modify-db-parameter-group \
    --db-parameter-group-name cdc-postgres-params \
    --parameters "ParameterName=wal_level,ParameterValue=logical,ApplyMethod=pending-reboot" \
                 "ParameterName=max_replication_slots,ParameterValue=10,ApplyMethod=pending-reboot" \
                 "ParameterName=max_wal_senders,ParameterValue=10,ApplyMethod=pending-reboot" \
                 "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"
```

#### 1.2 Apply to RDS Instance

```bash
# Modify RDS instance to use parameter group
aws rds modify-db-instance \
    --db-instance-identifier cdc-scd2-gaming-app-cluster \
    --db-parameter-group-name cdc-postgres-params \
    --apply-immediately

# Reboot required for wal_level change
aws rds reboot-db-instance \
    --db-instance-identifier cdc-scd2-gaming-app-cluster
```

#### 1.3 Initialize Database Schema

```bash
# Connect to RDS
psql -h <rds-endpoint> -U postgres -d postgres

# Run setup script
\i src/sql/postgres-cdc-setup.sql
```

**What this creates:**
- Schema: `gaming_oltp`
- Tables: `dim_user`, `dim_game`, `dim_session`, `fact_game_event`
- Publication: `my_cdc_pub`
- Grant replication permissions

#### 1.4 Verify CDC Configuration

```sql
-- Check wal_level
SHOW wal_level;  -- Must be 'logical'

-- Check publication
SELECT * FROM pg_publication WHERE pubname = 'my_cdc_pub';

-- Check tables in publication
SELECT schemaname, tablename 
FROM pg_publication_tables 
WHERE pubname = 'my_cdc_pub';

-- Verify replication permission
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'postgres';
```

---

### Step 2: MSK Cluster Setup

#### 2.1 Create MSK Cluster

```bash
aws kafka create-cluster-v2 \
    --cluster-name cdc-scd2-kafka-cluster \
    --kafka-version "3.7.x" \
    --number-of-broker-nodes 2 \
    --broker-node-group-info clientSubnets=subnet-xxx,subnet-yyy,instanceType=kafka.t3.small \
    --region us-east-1
```

Wait for cluster status to become `ACTIVE`.

#### 2.2 Get Bootstrap Servers

```bash
aws kafka get-bootstrap-brokers \
    --cluster-arn <cluster-arn> \
    --region us-east-1
```

#### 2.3 Create Kafka Topics (Optional - Debezium auto-creates)

```bash
# Create client.properties for SASL/IAM
cat > client.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

# Create topics
kafka-topics.sh --bootstrap-server <broker>:9098 \
    --create --topic cdc-gaming.gaming_oltp.dim_user \
    --partitions 3 --replication-factor 2 \
    --command-config client.properties
```

---

### Step 3: MSK Connect Plugins

#### 3.1 Download Plugins

```bash
mkdir -p connect-plugins && cd connect-plugins

# Debezium PostgreSQL connector
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.0.Final/debezium-connector-postgres-2.7.0.Final-plugin.zip

# Confluent S3 Sink connector
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc-kafka-connect-s3/versions/10.5.0/confluentinc-kafka-connect-s3-10.5.0.zip

cd ..
```

#### 3.2 Upload to S3

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws s3 mb s3://msk-connect-plugins-${ACCOUNT_ID} --region us-east-1

aws s3 cp connect-plugins/debezium-connector-postgres-2.7.0.Final-plugin.zip \
    s3://msk-connect-plugins-${ACCOUNT_ID}/debezium-postgres-plugin.zip

aws s3 cp connect-plugins/confluentinc-kafka-connect-s3-10.5.0.zip \
    s3://msk-connect-plugins-${ACCOUNT_ID}/confluent-s3-sink-plugin.zip
```

#### 3.3 Register Plugins with MSK Connect

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Debezium plugin
aws kafkaconnect create-custom-plugin \
    --name "debezium-postgres-2.7" \
    --location S3Location="{bucketArn=arn:aws:s3:::msk-connect-plugins-${ACCOUNT_ID},fileKey=debezium-postgres-plugin.zip}" \
    --region us-east-1

# S3 Sink plugin  
aws kafkaconnect create-custom-plugin \
    --name "confluent-s3-sink-10.5" \
    --location S3Location="{bucketArn=arn:aws:s3:::msk-connect-plugins-${ACCOUNT_ID},fileKey=confluent-s3-sink-plugin.zip}" \
    --region us-east-1
```

**Verify plugins are ACTIVE:**
```bash
aws kafkaconnect list-custom-plugins --region us-east-1
```

---

### Step 4: IAM Roles

#### 4.1 MSK Connect Execution Role

```bash
# Create trust policy
cat > msk-connect-trust.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "kafkaconnect.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create role
aws iam create-role \
    --role-name MSKConnectExecutionRole \
    --assume-role-policy-document file://msk-connect-trust.json

# Attach managed policies
aws iam attach-role-policy --role-name MSKConnectExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name MSKConnectExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
aws iam attach-role-policy --role-name MSKConnectExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonMSKConnectServiceRolePolicy
```

#### 4.2 Redshift S3 Read Role

```bash
# Create trust policy for Redshift
cat > redshift-trust.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "redshift.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
    --role-name RedshiftS3ReadRole \
    --assume-role-policy-document file://redshift-trust.json

aws iam attach-role-policy --role-name RedshiftS3ReadRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
```

**Associate with Redshift in console:** Redshift → Namespaces → Select namespace → IAM roles → Associate.

---

### Step 5: Deploy Connectors

#### 5.1 Debezium Source Connector

**Using automation script:**
```bash
./create-postgres-source-connector.sh
```

**Manual deployment via AWS Console:**
1. MSK Connect → Create connector
2. Select plugin: `debezium-postgres-2.7`
3. Paste configuration from `config/pg-debezium-source-connector-config.properties`
4. Network: Same VPC/subnets as MSK cluster
5. IAM role: `MSKConnectExecutionRole`
6. Capacity: 1 MCU, auto-scale 1-2 workers

**Key configuration:**
```properties
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=<rds-endpoint>
database.port=5432
database.user=postgres
database.password=<password>
database.dbname=postgres
schema.include.list=gaming_oltp
publication.name=my_cdc_pub
slot.name=debezium_slot
topic.prefix=cdc-gaming
transforms=unwrap
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.add.fields=op,ts_ms,source.ts_ms,source.lsn
```

#### 5.2 S3 Sink Connector

**Using automation script:**
```bash
./create-s3-sink-connector.sh
```

**Key configuration:**
```properties
connector.class=io.confluent.connect.s3.S3SinkConnector
topics.regex=cdc-gaming\.gaming_oltp\..*
s3.bucket.name=gaming-cdc-data-lake-2025
topics.dir=raw/cdc
format.class=io.confluent.connect.s3.format.json.JsonFormat
flush.size=100
rotate.interval.ms=60000
```

#### 5.3 Verify Connectors

```bash
aws kafkaconnect list-connectors --region us-east-1
```

Both should show `connectorState=RUNNING`.

---

### Step 6: Redshift Setup

#### 6.1 Create S3 Bucket and Upload JSONPath

```bash
aws s3 mb s3://gaming-cdc-data-lake-2025 --region us-east-1

# Upload JSONPath files
aws s3 cp src/json/jsonpath-dim-user.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-dim-game.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-dim-session.json s3://gaming-cdc-data-lake-2025/jsonpath/
aws s3 cp src/json/jsonpath-fact-game-event.json s3://gaming-cdc-data-lake-2025/jsonpath/
```

#### 6.2 Create Redshift Tables

Run in Redshift Query Editor:
```sql
\i src/sql/redshift-serverless-scd2.sql
```

---

### Step 7: Test with Data Simulator

#### 7.1 Configure Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install psycopg[binary] faker python-dotenv
```

#### 7.2 Configure Simulator

Edit `config/data-simulator.env`:
```properties
DB_HOST=<rds-endpoint>.us-east-1.rds.amazonaws.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=<password>
DB_SCHEMA=gaming_oltp

CYCLE_DELAY=0.5
LOG_LEVEL=INFO
STATS_INTERVAL=30

PROB_NEW_USER=0.08
PROB_UPDATE_USER=0.25
PROB_START_SESSION=0.15
PROB_END_SESSION=0.12
PROB_GENERATE_EVENTS=0.40
```

#### 7.3 Run Simulator

```bash
python src/python/data-simulator.py
```

Expected output:
```
============================================================
🎮 Gaming Data Simulator for CDC Testing
📍 Database: <rds-endpoint>
📊 Stats interval: 30 seconds
============================================================
✓ Connected to <rds-endpoint>
✓ Loaded: 5 users, 5 games, 0 active sessions
→ New user: johndoe847 from United States
→ Updated johndoe847: +2.5hrs
→ johndoe847 started playing Space Warriors on PC
```

#### 7.4 Verify End-to-End Flow

```bash
# 1. Check replication slot active
psql -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name='debezium_slot'"

# 2. Check Kafka messages
kafka-console-consumer.sh --bootstrap-server <broker>:9098 \
    --topic cdc-gaming.gaming_oltp.dim_user \
    --from-beginning --command-config client.properties \
    --max-messages 5

# 3. Check S3 files
aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive | head

# 4. Check Redshift staging
# Run in Redshift: SELECT COUNT(*) FROM bronze.stg_dim_user;
```

---

## Daily Operations

### Health Checks

#### Check Connector Status
```bash
aws kafkaconnect list-connectors --region us-east-1 \
    --query 'connectors[*].{Name:connectorName,State:connectorState}' --output table
```

#### Check PostgreSQL Replication Lag
```sql
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
```

**Alert if:** Lag > 100 MB

#### Verify S3 Data Arrival
```bash
# Check latest files
aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive | tail -20

# Count files by topic
aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive | \
    grep -o 'cdc-gaming.gaming_oltp.[a-z_]*' | sort | uniq -c
```

#### Check Redshift Data
```sql
-- Row counts
SELECT 'dim_user' as table_name, COUNT(*) as rows FROM bronze.dim_user
UNION ALL SELECT 'dim_game', COUNT(*) FROM bronze.dim_game
UNION ALL SELECT 'dim_session', COUNT(*) FROM bronze.dim_session
UNION ALL SELECT 'fact_game_event', COUNT(*) FROM bronze.fact_game_event;

-- Current records
SELECT COUNT(*) as current_users FROM bronze.dim_user WHERE is_current = TRUE;
```

---

## Troubleshooting

### Debezium Connector Not Running

**Symptoms:** No messages in Kafka topics, connector shows FAILED

**Diagnosis:**
```bash
# Check connector state
aws kafkaconnect describe-connector --connector-arn <arn> --region us-east-1

# Check CloudWatch logs
aws logs tail /aws/msk-connect/PostgresSourceConnectorDebezium --region us-east-1
```

**Common Causes:**

| Issue | Solution |
|-------|----------|
| Replication slot missing | `SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');` |
| Publication missing | `CREATE PUBLICATION my_cdc_pub FOR ALL TABLES;` |
| Network connectivity | Check security group allows MSK → RDS on port 5432 |
| IAM permissions | Verify role has S3, CloudWatch, MSK permissions |
| Plugin not found | Verify custom plugin is ACTIVE |

**Reset Debezium:**
```sql
-- Drop and recreate slot
SELECT pg_drop_replication_slot('debezium_slot');
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
```

Then restart connector.

---

### S3 Sink Connector Not Writing

**Symptoms:** No files in S3 bucket

**Diagnosis:**
```bash
# Check connector logs
aws logs tail /aws/msk-connect/S3SinkConnector --region us-east-1

# Check if topics exist
kafka-topics.sh --bootstrap-server <broker>:9098 --list \
    --command-config client.properties
```

**Common Causes:**

| Issue | Solution |
|-------|----------|
| Topic regex mismatch | Verify `topics.regex=cdc-gaming\.gaming_oltp\..*` |
| IAM permissions | Role needs S3 write access |
| Bucket not found | Verify bucket name in config |
| No messages | Check Debezium is running first |

---

### Redshift COPY Failing

**Symptoms:** `COPY` command errors, empty staging tables

**Diagnosis:**
```sql
-- Check load errors
SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;

-- Check S3 files exist
-- Run in shell:
-- aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/cdc-gaming.gaming_oltp.dim_user/
```

**Common Causes:**

| Issue | Solution |
|-------|----------|
| JSONPath file missing | Upload to `s3://bucket/jsonpath/` |
| IAM role not associated | Associate RedshiftS3ReadRole with namespace |
| Invalid JSON | Check S3 file contents |
| Column mismatch | Verify JSONPath mapping matches table columns |

---

### SCD2 Duplicate Current Records

**Symptoms:** Multiple `is_current=TRUE` for same business key

**Diagnosis:**
```sql
SELECT user_id, COUNT(*) as current_count
FROM bronze.dim_user
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1;
```

**Fix:**
```sql
-- Close all but the latest record
WITH latest AS (
    SELECT user_id, MAX(valid_from) as max_valid_from
    FROM bronze.dim_user WHERE is_current = TRUE
    GROUP BY user_id
)
UPDATE bronze.dim_user d
SET is_current = FALSE, valid_to = l.max_valid_from
FROM latest l
WHERE d.user_id = l.user_id
  AND d.is_current = TRUE
  AND d.valid_from < l.max_valid_from;
```

**Prevention:** Ensure staging data is deduplicated before merge (included in `redshift-serverless-scd2.sql`).

---

## Maintenance

### Reset Pipeline (Full Reload)

Use when data is corrupted or need fresh start.

```bash
# 1. Stop connectors in MSK Connect console

# 2. Clear S3 data
aws s3 rm s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive

# 3. Drop replication slot
psql -c "SELECT pg_drop_replication_slot('debezium_slot');"

# 4. Truncate Redshift tables
psql -h <redshift-endpoint> -U admin -d gaming_dwh << EOF
TRUNCATE bronze.dim_user;
TRUNCATE bronze.dim_game;
TRUNCATE bronze.dim_session;
TRUNCATE bronze.fact_game_event;
TRUNCATE bronze.stg_dim_user;
TRUNCATE bronze.stg_dim_game;
TRUNCATE bronze.stg_dim_session;
TRUNCATE bronze.stg_fact_game_event;
EOF

# 5. Recreate slot
psql -c "SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');"

# 6. Restart connectors
```

### Scale MSK Connect Workers

```bash
# Update connector capacity
aws kafkaconnect update-connector \
    --connector-arn <arn> \
    --capacity UpdateCapacity="{ProvisionedCapacity={mcuCount=2,workerCount=4}}" \
    --region us-east-1
```

### Vacuum Redshift Tables

```sql
-- Run weekly
VACUUM bronze.dim_user;
VACUUM bronze.dim_game;
VACUUM bronze.dim_session;
VACUUM bronze.fact_game_event;

ANALYZE bronze.dim_user;
ANALYZE bronze.dim_game;
ANALYZE bronze.dim_session;
ANALYZE bronze.fact_game_event;
```

---

## Emergency Procedures

### Pipeline Completely Down

1. **Check AWS service health:** https://status.aws.amazon.com
2. **Verify network:** Security groups, VPC endpoints
3. **Check IAM:** Role permissions not revoked
4. **Review logs:** CloudWatch for all connectors

### Data Loss Recovery

1. **From Kafka:** Messages retained for 7 days (default)
   ```bash
   kafka-console-consumer.sh --bootstrap-server <broker>:9098 \
       --topic cdc-gaming.gaming_oltp.dim_user \
       --from-beginning --command-config client.properties
   ```

2. **From S3:** Files retained indefinitely
   ```bash
   aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive
   ```

3. **Reload to Redshift:** Re-run COPY commands from `src/sql/redshift-serverless-scd2.sql`

---

## Key Metrics & Alerts

| Metric | Threshold | CloudWatch Alarm |
|--------|-----------|------------------|
| Replication Lag | > 100 MB | `pg_replication_slot_lag` |
| Connector State | != RUNNING | `MSKConnectConnectorStatus` |
| S3 File Age | > 10 min | Custom Lambda check |
| Kafka Consumer Lag | > 1000 | `Kafka-Consumer-Lag` |

---

## Useful Commands Reference

### PostgreSQL
```sql
-- Monitor replication
SELECT * FROM pg_replication_slots;

-- Check publication tables
SELECT * FROM pg_publication_tables WHERE pubname = 'my_cdc_pub';

-- Force disconnect replication
SELECT pg_terminate_backend(active_pid) 
FROM pg_replication_slots 
WHERE slot_name = 'debezium_slot' AND active = true;
```

### Kafka
```bash
# List topics
kafka-topics.sh --bootstrap-server <broker>:9098 --list \
    --command-config client.properties

# Consume messages
kafka-console-consumer.sh --bootstrap-server <broker>:9098 \
    --topic cdc-gaming.gaming_oltp.dim_user \
    --from-beginning --command-config client.properties

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server <broker>:9098 \
    --describe --group connect-S3SinkConnector \
    --command-config client.properties
```

### AWS CLI
```bash
# List connectors
aws kafkaconnect list-connectors --region us-east-1

# Describe connector
aws kafkaconnect describe-connector --connector-arn <arn> --region us-east-1

# List plugins
aws kafkaconnect list-custom-plugins --region us-east-1

# Check S3 files
aws s3 ls s3://gaming-cdc-data-lake-2025/raw/cdc/ --recursive --summarize
```
