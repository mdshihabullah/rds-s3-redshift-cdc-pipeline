-- ====================================================================
-- Redshift Serverless SCD Type 2 Setup - JSON FORMAT
-- Database: gaming_dwh
-- Schema: bronze (SCD Type 2 with valid_from, valid_to, is_current)
-- ====================================================================
-- DATA PIPELINE:
-- 1. RDS PostgreSQL → Debezium Source (flattened with ExtractNewRecordState)
-- 2. MSK Kafka → S3 Sink Connector (JSON format)
-- 3. S3 JSON files → Redshift COPY (JSON auto-detection)
--
-- SCD TYPE 2 LOGIC:
-- - __op='c' or 'r': Insert new record (valid_from=__ts_ms, is_current=TRUE)
-- - __op='u': Close old record (valid_to=__ts_ms), insert new record
-- - __op='d': Close old record (valid_to=__ts_ms, is_current=FALSE)
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- ====================================================================
-- SCD TYPE 2 DIMENSION TABLES
-- ====================================================================

-- SCD2: dim_user
CREATE TABLE IF NOT EXISTS bronze.dim_user (
    user_id VARCHAR(36) NOT NULL,
    username VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(50),
    account_level VARCHAR(20),
    total_playtime_hours DOUBLE PRECISION,
    last_login_at TIMESTAMPTZ,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source_lsn BIGINT
)
DISTSTYLE KEY
DISTKEY(user_id)
SORTKEY(user_id, is_current, valid_from);

-- SCD2: dim_game
CREATE TABLE IF NOT EXISTS bronze.dim_game (
    game_id VARCHAR(36) NOT NULL,
    game_name VARCHAR(200),
    game_category VARCHAR(50),
    difficulty_level VARCHAR(20),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source_lsn BIGINT
)
DISTSTYLE KEY
DISTKEY(game_id)
SORTKEY(game_id, is_current);

-- SCD2: dim_session
CREATE TABLE IF NOT EXISTS bronze.dim_session (
    session_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(36) NOT NULL,
    game_id VARCHAR(36) NOT NULL,
    session_start_at TIMESTAMPTZ,
    session_end_at TIMESTAMPTZ,
    device_type VARCHAR(50),
    is_active BOOLEAN,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source_lsn BIGINT
)
DISTSTYLE KEY
DISTKEY(user_id)
SORTKEY(user_id, session_start_at, is_current);

-- Fact table (no SCD2 - facts are immutable)
CREATE TABLE IF NOT EXISTS bronze.fact_game_event (
    event_id VARCHAR(36) NOT NULL,
    session_id VARCHAR(36),
    user_id VARCHAR(36),
    game_id VARCHAR(36),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMPTZ,
    score_delta INTEGER,
    coins_earned INTEGER,
    level_achieved INTEGER,
    metadata VARCHAR(MAX),
    source_lsn BIGINT,
    loaded_at TIMESTAMPTZ DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY(user_id)
SORTKEY(event_timestamp, user_id);

-- ====================================================================
-- STAGING TABLES (for JSON loading)
-- ====================================================================

CREATE TABLE IF NOT EXISTS bronze.stg_dim_user (
    user_id VARCHAR(36),
    username VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(50),
    account_level VARCHAR(20),
    total_playtime_hours DOUBLE PRECISION,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    __op VARCHAR(10),
    __ts_ms BIGINT,
    __source_ts_ms BIGINT,
    __source_lsn BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.stg_dim_game (
    game_id VARCHAR(36),
    game_name VARCHAR(200),
    game_category VARCHAR(50),
    difficulty_level VARCHAR(20),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    __op VARCHAR(10),
    __ts_ms BIGINT,
    __source_ts_ms BIGINT,
    __source_lsn BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.stg_dim_session (
    session_id VARCHAR(36),
    user_id VARCHAR(36),
    game_id VARCHAR(36),
    session_start_at TIMESTAMPTZ,
    session_end_at TIMESTAMPTZ,
    device_type VARCHAR(50),
    is_active BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    __op VARCHAR(10),
    __ts_ms BIGINT,
    __source_ts_ms BIGINT,
    __source_lsn BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.stg_fact_game_event (
    event_id VARCHAR(36),
    session_id VARCHAR(36),
    user_id VARCHAR(36),
    game_id VARCHAR(36),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMPTZ,
    score_delta INTEGER,
    coins_earned INTEGER,
    level_achieved INTEGER,
    metadata VARCHAR(MAX),
    __op VARCHAR(10),
    __ts_ms BIGINT,
    __source_ts_ms BIGINT,
    __source_lsn BIGINT
);

-- ====================================================================
-- LOAD DATA FROM S3 JSON FILES
-- ====================================================================

-- Load dim_user (JSON format with JSONPath mapping)
-- Upload jsonpath-dim-user.json to S3 first: 
-- aws s3 cp jsonpath-dim-user.json s3://gaming-cdc-data-lake-2025/jsonpath/
TRUNCATE TABLE bronze.stg_dim_user;

COPY bronze.stg_dim_user
FROM 's3://gaming-cdc-data-lake-2025/raw/cdc/cdc-gaming.gaming_oltp.dim_user/'
IAM_ROLE DEFAULT
FORMAT AS JSON 's3://gaming-cdc-data-lake-2025/jsonpath/jsonpath-dim-user.json'
REGION 'us-east-1'
TIMEFORMAT 'auto'
MAXERROR 100
ACCEPTINVCHARS
EMPTYASNULL
BLANKSASNULL
TRUNCATECOLUMNS;

-- Load dim_game (JSON format with JSONPath mapping)
-- Upload jsonpath-dim-game.json to S3 first:
-- aws s3 cp jsonpath-dim-game.json s3://gaming-cdc-data-lake-2025/jsonpath/
TRUNCATE TABLE bronze.stg_dim_game;

COPY bronze.stg_dim_game
FROM 's3://gaming-cdc-data-lake-2025/raw/cdc/cdc-gaming.gaming_oltp.dim_game/'
IAM_ROLE DEFAULT
FORMAT AS JSON 's3://gaming-cdc-data-lake-2025/jsonpath/jsonpath-dim-game.json'
REGION 'us-east-1'
TIMEFORMAT 'auto'
MAXERROR 100
ACCEPTINVCHARS
EMPTYASNULL
BLANKSASNULL
TRUNCATECOLUMNS;

-- Load dim_session (JSON format with JSONPath mapping)
-- Upload jsonpath-dim-session.json to S3 first:
-- aws s3 cp jsonpath-dim-session.json s3://gaming-cdc-data-lake-2025/jsonpath/
TRUNCATE TABLE bronze.stg_dim_session;

COPY bronze.stg_dim_session
FROM 's3://gaming-cdc-data-lake-2025/raw/cdc/cdc-gaming.gaming_oltp.dim_session/'
IAM_ROLE DEFAULT
FORMAT AS JSON 's3://gaming-cdc-data-lake-2025/jsonpath/jsonpath-dim-session.json'
REGION 'us-east-1'
TIMEFORMAT 'auto'
MAXERROR 100
ACCEPTINVCHARS
EMPTYASNULL
BLANKSASNULL
TRUNCATECOLUMNS;

-- Load fact_game_event (JSON format with JSONPath mapping)
-- Upload jsonpath-fact-game-event.json to S3 first:
-- aws s3 cp jsonpath-fact-game-event.json s3://gaming-cdc-data-lake-2025/jsonpath/
TRUNCATE TABLE bronze.stg_fact_game_event;

COPY bronze.stg_fact_game_event
FROM 's3://gaming-cdc-data-lake-2025/raw/cdc/cdc-gaming.gaming_oltp.fact_game_event/'
IAM_ROLE DEFAULT
FORMAT AS JSON 's3://gaming-cdc-data-lake-2025/jsonpath/jsonpath-fact-game-event.json'
REGION 'us-east-1'
TIMEFORMAT 'auto'
MAXERROR 100
ACCEPTINVCHARS
EMPTYASNULL
BLANKSASNULL
TRUNCATECOLUMNS;

-- ====================================================================
-- SCD2 MERGE LOGIC FOR dim_user
-- ====================================================================

-- Step 0: Deduplicate staging - keep only the LATEST record per user_id (by __source_lsn)
-- This prevents processing duplicate records that cause multiple is_current=TRUE
CREATE TEMP TABLE stg_dim_user_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY __source_lsn DESC, __ts_ms DESC) as rn
    FROM bronze.stg_dim_user
    WHERE __op IN ('r', 'c', 'u')
) WHERE rn = 1;

-- Step 1: Close current records ONLY when data actually changes
-- This prevents closing records unnecessarily when data is identical
UPDATE bronze.dim_user d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM stg_dim_user_dedup s
WHERE d.user_id = s.user_id
  AND d.is_current = TRUE
  AND (
      COALESCE(d.username, '') != COALESCE(s.username, '') OR
      COALESCE(d.email, '') != COALESCE(s.email, '') OR
      COALESCE(d.country, '') != COALESCE(s.country, '') OR
      COALESCE(d.account_level, '') != COALESCE(s.account_level, '') OR
      COALESCE(d.total_playtime_hours, 0) != COALESCE(s.total_playtime_hours, 0) OR
      COALESCE(d.last_login_at, '1900-01-01'::TIMESTAMPTZ) != COALESCE(s.last_login_at, '1900-01-01'::TIMESTAMPTZ)
  );

-- Step 2: Insert new versions ONLY if data actually changed or record doesn't exist
INSERT INTO bronze.dim_user (
    user_id, username, email, country, account_level,
    total_playtime_hours, last_login_at,
    valid_from, valid_to, is_current, event_timestamp, source_lsn
)
SELECT 
    s.user_id,
    s.username,
    s.email,
    s.country,
    s.account_level,
    s.total_playtime_hours,
    s.last_login_at,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS event_timestamp,
    s.__source_lsn
FROM stg_dim_user_dedup s
LEFT JOIN bronze.dim_user d 
    ON s.user_id = d.user_id AND d.is_current = TRUE
WHERE (
      d.user_id IS NULL  -- New record
      OR COALESCE(d.username, '') != COALESCE(s.username, '') 
      OR COALESCE(d.email, '') != COALESCE(s.email, '')
      OR COALESCE(d.country, '') != COALESCE(s.country, '')
      OR COALESCE(d.account_level, '') != COALESCE(s.account_level, '')
      OR COALESCE(d.total_playtime_hours, 0) != COALESCE(s.total_playtime_hours, 0)
      OR COALESCE(d.last_login_at, '1900-01-01'::TIMESTAMPTZ) != COALESCE(s.last_login_at, '1900-01-01'::TIMESTAMPTZ)
  );

DROP TABLE stg_dim_user_dedup;

-- Step 3: Handle deletes (tombstones) - deduplicate deletes too
UPDATE bronze.dim_user d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM (
    SELECT 
        user_id,
        MAX(__ts_ms) as __ts_ms  -- Use MAX for delete timestamp
    FROM bronze.stg_dim_user
    WHERE __op = 'd'
    GROUP BY user_id
) s
WHERE d.user_id = s.user_id
  AND d.is_current = TRUE;

-- Step 4: Clear staging
TRUNCATE TABLE bronze.stg_dim_user;

-- ====================================================================
-- SCD2 MERGE LOGIC FOR dim_game
-- ====================================================================

-- Step 0: Deduplicate staging
CREATE TEMP TABLE stg_dim_game_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY __source_lsn DESC, __ts_ms DESC) as rn
    FROM bronze.stg_dim_game
    WHERE __op IN ('r', 'c', 'u')
) WHERE rn = 1;

-- Step 1: Close current records ONLY when data actually changes
UPDATE bronze.dim_game d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM stg_dim_game_dedup s
WHERE d.game_id = s.game_id
  AND d.is_current = TRUE
  AND (
      COALESCE(d.game_name, '') != COALESCE(s.game_name, '') OR
      COALESCE(d.game_category, '') != COALESCE(s.game_category, '') OR
      COALESCE(d.difficulty_level, '') != COALESCE(s.difficulty_level, '')
  );

-- Step 2: Insert new versions
INSERT INTO bronze.dim_game (
    game_id, game_name, game_category, difficulty_level,
    valid_from, valid_to, is_current, event_timestamp, source_lsn
)
SELECT 
    s.game_id,
    s.game_name,
    s.game_category,
    s.difficulty_level,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS event_timestamp,
    s.__source_lsn
FROM stg_dim_game_dedup s
LEFT JOIN bronze.dim_game d 
    ON s.game_id = d.game_id AND d.is_current = TRUE
WHERE (
      d.game_id IS NULL
      OR COALESCE(d.game_name, '') != COALESCE(s.game_name, '') 
      OR COALESCE(d.game_category, '') != COALESCE(s.game_category, '')
      OR COALESCE(d.difficulty_level, '') != COALESCE(s.difficulty_level, '')
  );

DROP TABLE stg_dim_game_dedup;

-- Step 3: Handle deletes
UPDATE bronze.dim_game d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM (
    SELECT game_id, MAX(__ts_ms) as __ts_ms
    FROM bronze.stg_dim_game
    WHERE __op = 'd'
    GROUP BY game_id
) s
WHERE d.game_id = s.game_id AND d.is_current = TRUE;

TRUNCATE TABLE bronze.stg_dim_game;

-- ====================================================================
-- SCD2 MERGE LOGIC FOR dim_session
-- ====================================================================

-- Step 0: Deduplicate staging
CREATE TEMP TABLE stg_dim_session_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY __source_lsn DESC, __ts_ms DESC) as rn
    FROM bronze.stg_dim_session
    WHERE __op IN ('r', 'c', 'u')
) WHERE rn = 1;

-- Step 1: Close current records ONLY when data actually changes
UPDATE bronze.dim_session d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM stg_dim_session_dedup s
WHERE d.session_id = s.session_id
  AND d.is_current = TRUE
  AND (
      COALESCE(d.session_end_at, '1900-01-01'::TIMESTAMPTZ) != COALESCE(s.session_end_at, '1900-01-01'::TIMESTAMPTZ) OR
      COALESCE(d.device_type, '') != COALESCE(s.device_type, '') OR
      COALESCE(d.is_active, FALSE) != COALESCE(s.is_active, FALSE)
  );

-- Step 2: Insert new versions
INSERT INTO bronze.dim_session (
    session_id, user_id, game_id, session_start_at, session_end_at,
    device_type, is_active, valid_from, valid_to, is_current, event_timestamp, source_lsn
)
SELECT 
    s.session_id,
    s.user_id,
    s.game_id,
    s.session_start_at,
    s.session_end_at,
    s.device_type,
    s.is_active,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second' AS event_timestamp,
    s.__source_lsn
FROM stg_dim_session_dedup s
LEFT JOIN bronze.dim_session d 
    ON s.session_id = d.session_id AND d.is_current = TRUE
WHERE (
      d.session_id IS NULL
      OR COALESCE(d.session_end_at, '1900-01-01'::TIMESTAMPTZ) != COALESCE(s.session_end_at, '1900-01-01'::TIMESTAMPTZ)
      OR COALESCE(d.device_type, '') != COALESCE(s.device_type, '')
      OR COALESCE(d.is_active, FALSE) != COALESCE(s.is_active, FALSE)
  );

DROP TABLE stg_dim_session_dedup;

-- Step 3: Handle deletes
UPDATE bronze.dim_session d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM (
    SELECT session_id, MAX(__ts_ms) as __ts_ms
    FROM bronze.stg_dim_session
    WHERE __op = 'd'
    GROUP BY session_id
) s
WHERE d.session_id = s.session_id AND d.is_current = TRUE;

TRUNCATE TABLE bronze.stg_dim_session;

-- ====================================================================
-- LOAD FACT TABLE (append-only)
-- ====================================================================

INSERT INTO bronze.fact_game_event (
    event_id, session_id, user_id, game_id, event_type,
    event_timestamp, score_delta, coins_earned, level_achieved, metadata, source_lsn
)
SELECT 
    event_id, session_id, user_id, game_id, event_type,
    event_timestamp, score_delta, coins_earned, level_achieved, metadata, __source_lsn
FROM bronze.stg_fact_game_event
WHERE event_id NOT IN (SELECT event_id FROM bronze.fact_game_event);

TRUNCATE TABLE bronze.stg_fact_game_event;

-- ====================================================================
-- VERIFICATION QUERIES
-- ====================================================================

-- Current state of all users
SELECT 
    *
FROM bronze.dim_user
WHERE is_current = TRUE
ORDER BY valid_from DESC;

-- Check data loaded from JSON
SELECT COUNT(*) as total_users FROM bronze.dim_user;
SELECT COUNT(*) as total_games FROM bronze.dim_game;
SELECT COUNT(*) as total_sessions FROM bronze.dim_session;
SELECT COUNT(*) as total_events FROM bronze.fact_game_event;

-- Verify no duplicate current records (should return 0)
SELECT user_id, COUNT(*) as current_count
FROM bronze.dim_user
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1;