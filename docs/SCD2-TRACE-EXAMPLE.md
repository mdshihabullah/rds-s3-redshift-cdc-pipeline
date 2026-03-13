# SCD Type 2 Data Flow Trace - Complete Example

## Scenario: User Account Lifecycle

Let's trace a complete example for `dim_user` table showing how SCD2 handles:
1. **Initial Create** (__op='c')
2. **Update** (__op='u') 
3. **Another Update** (__op='u')
4. **Delete** (__op='d')

---

## Initial State

**bronze.dim_user** (empty):
```
(no records)
```

---

## BATCH 1: Initial User Creation

### Step 0: Data Arrives in Staging

**bronze.stg_dim_user** (after COPY from S3):
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "bronze"
total_playtime_hours: 0.0
last_login_at: NULL
__op: "c"
__ts_ms: 1700000000000  (2023-11-14 22:13:20 UTC)
__source_lsn: 1000
```

### Step 0: Deduplication
```sql
CREATE TEMP TABLE stg_dim_user_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY __source_lsn DESC, __ts_ms DESC) as rn
    FROM bronze.stg_dim_user
    WHERE __op IN ('r', 'c', 'u')
) WHERE rn = 1;
```

**Result**: Only 1 record (no duplicates), so it passes through.

**stg_dim_user_dedup**:
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "bronze"
total_playtime_hours: 0.0
last_login_at: NULL
__op: "c"
__ts_ms: 1700000000000
__source_lsn: 1000
```

### Step 1: Close Current Records
```sql
UPDATE bronze.dim_user d
SET is_current = FALSE, valid_to = ...
FROM (SELECT user_id, MIN(__ts_ms) as __ts_ms FROM stg_dim_user_dedup GROUP BY user_id) s
WHERE d.user_id = s.user_id AND d.is_current = TRUE;
```

**Result**: No records updated (user doesn't exist yet)

**bronze.dim_user**:
```
(no records)
```

### Step 2: Insert New Version
```sql
INSERT INTO bronze.dim_user (...)
SELECT ... FROM stg_dim_user_dedup s
LEFT JOIN bronze.dim_user d ON s.user_id = d.user_id AND d.is_current = TRUE
WHERE (d.user_id IS NULL OR ...);
```

**Condition Check**:
- `d.user_id IS NULL` → **TRUE** (user doesn't exist)
- **INSERT HAPPENS**

**bronze.dim_user** (after Step 2):
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "bronze"
total_playtime_hours: 0.0
last_login_at: NULL
valid_from: 2023-11-14 22:13:20 UTC
valid_to: NULL
is_current: TRUE
event_timestamp: 2023-11-14 22:13:20 UTC
source_lsn: 1000
```

### Step 3: Handle Deletes
**Result**: No deletes in this batch

### Step 4: Clear Staging
**Result**: `stg_dim_user` is truncated

---

## BATCH 2: User Updates Account Level

### Step 0: Data Arrives in Staging

**bronze.stg_dim_user**:
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "silver"  ← CHANGED from "bronze"
total_playtime_hours: 25.5  ← CHANGED from 0.0
last_login_at: 2023-11-15 10:30:00 UTC  ← CHANGED from NULL
__op: "u"
__ts_ms: 1700052000000  (2023-11-15 10:30:00 UTC)
__source_lsn: 2000
```

### Step 0: Deduplication
**Result**: 1 record (no duplicates)

**stg_dim_user_dedup**:
```
user_id: "user-123"
account_level: "silver"
total_playtime_hours: 25.5
last_login_at: 2023-11-15 10:30:00 UTC
__op: "u"
__ts_ms: 1700052000000
__source_lsn: 2000
```

### Step 1: Close Current Records
```sql
UPDATE bronze.dim_user d
SET is_current = FALSE, 
    valid_to = TIMESTAMPTZ 'epoch' + 1700052000000 * INTERVAL '0.001 second'
FROM (SELECT user_id, MIN(__ts_ms) as __ts_ms FROM stg_dim_user_dedup GROUP BY user_id) s
WHERE d.user_id = s.user_id AND d.is_current = TRUE;
```

**Result**: 1 record updated

**bronze.dim_user** (after Step 1):
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "bronze"
total_playtime_hours: 0.0
last_login_at: NULL
valid_from: 2023-11-14 22:13:20 UTC
valid_to: 2023-11-15 10:30:00 UTC  ← UPDATED
is_current: FALSE  ← UPDATED
event_timestamp: 2023-11-14 22:13:20 UTC
source_lsn: 1000
```

### Step 2: Insert New Version
**Condition Check**:
- `d.user_id IS NULL` → **FALSE** (user exists)
- `COALESCE(d.account_level, '') != COALESCE(s.account_level, '')` → **TRUE** ("bronze" != "silver")
- **INSERT HAPPENS** (data changed)

**bronze.dim_user** (after Step 2):
```
Row 1 (OLD - closed):
user_id: "user-123"
account_level: "bronze"
total_playtime_hours: 0.0
valid_from: 2023-11-14 22:13:20 UTC
valid_to: 2023-11-15 10:30:00 UTC
is_current: FALSE

Row 2 (NEW - current):
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "silver"  ← NEW VALUE
total_playtime_hours: 25.5  ← NEW VALUE
last_login_at: 2023-11-15 10:30:00 UTC  ← NEW VALUE
valid_from: 2023-11-15 10:30:00 UTC
valid_to: NULL
is_current: TRUE
event_timestamp: 2023-11-15 10:30:00 UTC
source_lsn: 2000
```

---

## BATCH 3: User Updates Again (Same Data - No Change)

### Step 0: Data Arrives in Staging

**bronze.stg_dim_user**:
```
user_id: "user-123"
username: "john_doe"
email: "john@example.com"
country: "USA"
account_level: "silver"  ← SAME as current
total_playtime_hours: 25.5  ← SAME as current
last_login_at: 2023-11-15 10:30:00 UTC  ← SAME as current
__op: "u"
__ts_ms: 1700055000000  (2023-11-15 11:15:00 UTC)
__source_lsn: 3000
```

### Step 0: Deduplication
**Result**: 1 record

### Step 1: Close Current Records
**Result**: 1 record closed (even though data is the same!)

**bronze.dim_user** (after Step 1):
```
Row 1: is_current: FALSE, valid_to: 2023-11-14 22:13:20 UTC
Row 2: is_current: FALSE, valid_to: 2023-11-15 11:15:00 UTC  ← CLOSED
```

### Step 2: Insert New Version
**Condition Check**:
- `d.user_id IS NULL` → **FALSE**
- All field comparisons → **FALSE** (all data is identical)
- **NO INSERT** (data hasn't changed)

**bronze.dim_user** (after Step 2):
```
Row 1: is_current: FALSE, valid_to: 2023-11-14 22:13:20 UTC
Row 2: is_current: FALSE, valid_to: 2023-11-15 11:15:00 UTC  ← STILL CLOSED, NO NEW ROW
```

**✅ FIXED**: Step 1 now only closes records when data actually changes!

**Updated Step 1 Logic**:
```sql
UPDATE bronze.dim_user d
SET is_current = FALSE, valid_to = ...
FROM stg_dim_user_dedup s
WHERE d.user_id = s.user_id
  AND d.is_current = TRUE
  AND (data changed conditions);  ← Only closes if data changed
```

**Result**: No records closed (data is identical)

**bronze.dim_user** (after Step 1):
```
Row 1: is_current: FALSE, valid_to: 2023-11-14 22:13:20 UTC
Row 2: is_current: TRUE, valid_to: NULL  ← STILL CURRENT (not closed)
```

### Step 2: Insert New Version
**Condition Check**:
- `d.user_id IS NULL` → **FALSE**
- All field comparisons → **FALSE** (all data is identical)
- **NO INSERT** (data hasn't changed, and current record still exists)

**bronze.dim_user** (after Step 2):
```
Row 1: is_current: FALSE, valid_to: 2023-11-14 22:13:20 UTC
Row 2: is_current: TRUE, valid_to: NULL  ← STILL CURRENT (correct!)
```

**✅ CORRECT**: Current record remains because data didn't change!

---

## BATCH 4: User Deletes Account

### Step 0: Data Arrives in Staging

**bronze.stg_dim_user**:
```
user_id: "user-123"
__op: "d"
__ts_ms: 1700060000000  (2023-11-15 12:53:20 UTC)
__source_lsn: 4000
```

### Step 3: Handle Deletes
```sql
UPDATE bronze.dim_user d
SET is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + 1700060000000 * INTERVAL '0.001 second'
FROM (SELECT user_id, MAX(__ts_ms) as __ts_ms FROM bronze.stg_dim_user WHERE __op = 'd' GROUP BY user_id) s
WHERE d.user_id = s.user_id AND d.is_current = TRUE;
```

**Result**: Closes any current record (but we have none from Batch 3, so nothing happens)

**If we had a current record**:
```
Row 2: is_current: FALSE, valid_to: 2023-11-15 12:53:20 UTC
```

---

## BATCH 4: User Deletes Account (Corrected)

### Step 0: Data Arrives in Staging

**bronze.stg_dim_user**:
```
user_id: "user-123"
__op: "d"
__ts_ms: 1700060000000  (2023-11-15 12:53:20 UTC)
__source_lsn: 4000
```

### Step 3: Handle Deletes
```sql
UPDATE bronze.dim_user d
SET is_current = FALSE,
    valid_to = TIMESTAMPTZ 'epoch' + 1700060000000 * INTERVAL '0.001 second'
FROM (SELECT user_id, MAX(__ts_ms) as __ts_ms FROM bronze.stg_dim_user WHERE __op = 'd' GROUP BY user_id) s
WHERE d.user_id = s.user_id AND d.is_current = TRUE;
```

**Result**: Closes current record

**bronze.dim_user** (after Step 3):
```
Row 1: is_current: FALSE, valid_to: 2023-11-14 22:13:20 UTC
Row 2: is_current: FALSE, valid_to: 2023-11-15 12:53:20 UTC  ← CLOSED (deleted)
```

**✅ CORRECT**: User is soft-deleted (no current record exists)

---

## Final State Summary

**bronze.dim_user** (complete history):
```
Row 1: 
  valid_from: 2023-11-14 22:13:20 UTC
  valid_to:   2023-11-15 10:30:00 UTC
  is_current: FALSE
  account_level: "bronze"
  → Version 1: Initial creation

Row 2:
  valid_from: 2023-11-15 10:30:00 UTC
  valid_to:   2023-11-15 12:53:20 UTC
  is_current: FALSE
  account_level: "silver"
  → Version 2: Upgraded to silver, then deleted

No current record (user deleted)
```

---

## Key Logic Points

### ✅ Deduplication (Step 0)
- **Purpose**: Prevents processing duplicate records in same batch
- **Method**: Keep only latest record per entity (by `__source_lsn DESC, __ts_ms DESC`)
- **Result**: Only 1 record per entity processed per batch

### ✅ Close Records (Step 1)
- **Purpose**: Close old version before inserting new one
- **Method**: Only close when data actually changes
- **Result**: Prevents unnecessary versioning when data is identical

### ✅ Insert New Version (Step 2)
- **Purpose**: Create new version with updated data
- **Method**: Only insert if data changed OR record doesn't exist
- **Result**: Maintains complete history without duplicates

### ✅ Handle Deletes (Step 3)
- **Purpose**: Soft-delete records (close current version)
- **Method**: Close current record, set `valid_to` to delete timestamp
- **Result**: Historical data preserved, no current record exists

---

## Edge Cases Handled

1. **Duplicate records in staging** → Deduplicated in Step 0
2. **Identical data updates** → No new version created (Step 1 doesn't close, Step 2 doesn't insert)
3. **Multiple updates in same batch** → Only latest processed (deduplication)
4. **Delete without current record** → No error (UPDATE affects 0 rows)
5. **Create for existing user** → Treated as update (Step 2 inserts if data different)

---

## Data Integrity Guarantees

✅ **At most 1 current record per entity** (enforced by Step 1 closing before Step 2 inserting)
✅ **Complete history preserved** (all versions kept with valid_from/valid_to)
✅ **No duplicate versions** (deduplication + change detection)
✅ **Correct timestamp ordering** (uses __source_lsn for ordering)

