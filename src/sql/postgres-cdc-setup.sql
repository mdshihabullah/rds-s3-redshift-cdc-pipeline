/* 2025-11-16 13:04:31 [633 ms] */ 
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
/* 2025-11-16 13:04:41 [113 ms] */ 
CREATE SCHEMA IF NOT EXISTS gaming_oltp;
/* 2025-11-16 13:04:44 [108 ms] */ 
SET search_path TO gaming_oltp;
/* 2025-11-16 13:12:47 [680 ms] */ 
CREATE TABLE dim_user (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    country VARCHAR(50),
    account_level VARCHAR(20) DEFAULT 'bronze',
    total_playtime_hours DECIMAL(10,2) DEFAULT 0,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
/* 2025-11-16 13:12:54 [117 ms] */ 
CREATE INDEX idx_dim_user_username ON dim_user(username);
/* 2025-11-16 13:12:55 [116 ms] */ 
CREATE INDEX idx_dim_user_updated_at ON dim_user(updated_at);
/* 2025-11-16 13:13:36 [119 ms] */ 
CREATE TABLE dim_game (
    game_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_name VARCHAR(200) NOT NULL,
    game_category VARCHAR(50),
    difficulty_level VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
/* 2025-11-16 13:14:06 [141 ms] */ 
SET search_path TO gaming_oltp;
/* 2025-11-16 13:14:09 [113 ms] */ 
CREATE TABLE dim_user (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    country VARCHAR(50),
    account_level VARCHAR(20) DEFAULT 'bronze',
    total_playtime_hours DECIMAL(10,2) DEFAULT 0,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
/* 2025-11-16 13:14:12 [110 ms] */ 
CREATE INDEX idx_dim_user_username ON dim_user(username);
/* 2025-11-16 13:14:14 [132 ms] */ 
CREATE INDEX idx_dim_user_updated_at ON dim_user(updated_at);
/* 2025-11-16 13:14:15 [111 ms] */ 
CREATE TABLE dim_game (
    game_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_name VARCHAR(200) NOT NULL,
    game_category VARCHAR(50),
    difficulty_level VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
/* 2025-11-16 13:14:17 [118 ms] */ 
CREATE INDEX idx_dim_game_category ON dim_game(game_category);
/* 2025-11-16 13:14:19 [116 ms] */ 
CREATE TABLE dim_session (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES dim_user(user_id),
    game_id UUID NOT NULL REFERENCES dim_game(game_id),
    session_start_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    session_end_at TIMESTAMPTZ,
    device_type VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
/* 2025-11-16 13:14:21 [119 ms] */ 
CREATE INDEX idx_dim_session_user_id ON dim_session(user_id);
/* 2025-11-16 13:14:23 [116 ms] */ 
CREATE INDEX idx_dim_session_start_at ON dim_session(session_start_at);
/* 2025-11-16 13:14:24 [114 ms] */ 
CREATE TABLE fact_game_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES dim_session(session_id),
    user_id UUID NOT NULL REFERENCES dim_user(user_id),
    game_id UUID NOT NULL REFERENCES dim_game(game_id),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score_delta INTEGER DEFAULT 0,
    coins_earned INTEGER DEFAULT 0,
    level_achieved INTEGER,
    metadata JSONB
);
/* 2025-11-16 13:14:29 [118 ms] */ 
CREATE INDEX idx_fact_event_user_game_time ON fact_game_event(user_id, game_id, event_timestamp);
/* 2025-11-16 13:14:30 [120 ms] */ 
CREATE INDEX idx_fact_event_type ON fact_game_event(event_type);
/* 2025-11-16 13:14:31 [110 ms] */ 
CREATE INDEX idx_fact_event_timestamp ON fact_game_event(event_timestamp);
/* 2025-11-16 13:14:34 [120 ms] */ 
INSERT INTO dim_game (game_name, game_category, difficulty_level) VALUES
    ('Space Warriors', 'Action', 'hard'),
    ('Puzzle Master', 'Puzzle', 'medium'),
    ('Racing Thunder', 'Racing', 'easy'),
    ('Fantasy Quest', 'RPG', 'hard'),
    ('Card Battle', 'Strategy', 'medium');
/* 2025-11-16 13:14:38 [121 ms] */ 
SELECT 'Schema created successfully!' as status;
/* 2025-11-16 17:14:33 [113 ms] */ 
show wal_level;
/* 2025-11-16 18:30:51 [607 ms] */ 
CREATE PUBLICATION my_cdc_pub FOR ALL TABLES;
/* 2025-11-16 20:33:50 [629 ms] */ 
show wal_level;
/* 2025-11-16 20:33:55 [112 ms] */ 
SHOW rds.logical_replication;
/* 2025-11-16 20:34:00 [105 ms] */ 
select * from pg_publication LIMIT 100;
/* 2025-11-16 20:34:49 [126 ms] */ 
CREATE TABLE IF NOT EXISTS gaming_oltp.debezium_heartbeat (
    id INTEGER PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
/* 2025-11-16 20:34:54 [108 ms] */ 
GRANT rds_replication TO postgres;
/* 2025-11-16 20:34:55 [108 ms] */ 
GRANT USAGE ON SCHEMA gaming_oltp TO postgres;
/* 2025-11-16 20:35:01 [106 ms] */ 
GRANT SELECT ON ALL TABLES IN SCHEMA gaming_oltp TO postgres;
/* 2025-11-17 15:23:52 [709 ms] */ 
SELECT * FROM pg_publication WHERE pubname = 'my_cdc_pub' LIMIT 100;
/* 2025-11-17 15:23:57 [120 ms] */ 
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'my_cdc_pub' LIMIT 100;
/* 2025-11-17 15:25:17 [111 ms] */ 
SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot' LIMIT 100;
/* 2025-11-17 15:25:28 [110 ms] */ 
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as replication_lag
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot' LIMIT 100;
/* 2025-11-19 14:29:47 [114 ms] */ 
SELECT 
    slot_name,
    plugin,
    slot_type,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot' LIMIT 100;
/* 2025-11-19 14:42:18 [605 ms] */ 
SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'debezium_slot' LIMIT 100;
/* 2025-11-19 14:42:25 [111 ms] */ 
SELECT pg_terminate_backend(active_pid) 
FROM pg_replication_slots 
WHERE slot_name = 'debezium_slot' AND active = true LIMIT 100;
/* 2025-11-19 14:42:29 [113 ms] */ 
SELECT pg_drop_replication_slot('debezium_slot');


-- PostgreSQL CDC Setup for Debezium
-- Verify these after connector is running

-- 1. Check if publication exists
SELECT * FROM pg_publication WHERE pubname = 'my_cdc_pub';

-- 2. Check tables in publication
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'my_cdc_pub';

-- 3. Check replication slot status
SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot';

-- 4. Monitor replication lag
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as replication_lag
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
