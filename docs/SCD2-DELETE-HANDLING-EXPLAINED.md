# SCD Type 2 Delete Handling - `__deleted` vs `__op`

## Question
**Will the absence of `__deleted` column break SCD versioning with `is_current` flag?**

## Answer: NO - Here's Why

The SCD Type 2 logic for handling deletes is **fully functional** using only the `__op` field. The `__deleted` field is **redundant** in your setup.

---

## How CDC Delete Events Work in Your Pipeline

### Debezium Configuration
```properties
# In pg-debezium-source-connector-config.properties:
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.add.fields=op,ts_ms,source.ts_ms,source.lsn
transforms.unwrap.delete.handling.mode=rewrite
```

### What Happens When a Row is Deleted

**Step 1: PostgreSQL Delete**
```sql
DELETE FROM gaming_oltp.dim_user WHERE user_id = 'abc-123';
```

**Step 2: Debezium Captures Delete Event**
```json
{
  "user_id": "abc-123",
  "username": "john_doe",
  "email": "john@example.com",
  "country": "USA",
  "account_level": "gold",
  "total_playtime_hours": 45.5,
  "last_login_at": "2025-11-19T10:30:00Z",
  "created_at": "2025-11-01T08:00:00Z",
  "updated_at": "2025-11-19T10:30:00Z",
  "__op": "d",                           ← DELETE OPERATION
  "__ts_ms": 1763572607616,              ← TIMESTAMP
  "__source_ts_ms": 1763572607363,
  "__source_lsn": 62948524432
}
```

**Key Point**: The `__op = "d"` field is **sufficient** to identify delete events!

---

## SCD Type 2 Delete Logic (Already Implemented)

### Step 3 in `redshift-serverless-scd2.sql`:

```sql
-- Handle deletes (tombstones)
UPDATE bronze.dim_user d
SET 
    is_current = FALSE,
    valid_to = TIMESTAMP 'epoch' + s.__ts_ms * INTERVAL '0.001 second'
FROM (
    SELECT 
        user_id,
        __ts_ms
    FROM bronze.stg_dim_user
    WHERE __op = 'd'  ← Uses __op field to detect deletes
) s
WHERE d.user_id = s.user_id
  AND d.is_current = TRUE;
```

### What This Does:
1. **Identifies delete events**: `WHERE __op = 'd'`
2. **Closes the current record**: Sets `is_current = FALSE`
3. **Sets end date**: `valid_to = DELETE_TIMESTAMP`
4. **Preserves history**: Old record remains in table

---

## Complete SCD Type 2 Lifecycle Example

### Scenario: User Record Changes Over Time

#### T1: Initial Insert (`__op = 'c'`)
```sql
user_id | username  | account_level | valid_from          | valid_to | is_current
abc-123 | john_doe  | bronze        | 2025-11-01 08:00:00 | NULL     | TRUE
```

#### T2: User Upgrades Account (`__op = 'u'`)
**Old record closed:**
```sql
user_id | username  | account_level | valid_from          | valid_to            | is_current
abc-123 | john_doe  | bronze        | 2025-11-01 08:00:00 | 2025-11-10 14:30:00 | FALSE
```

**New record inserted:**
```sql
user_id | username  | account_level | valid_from          | valid_to | is_current
abc-123 | john_doe  | gold          | 2025-11-10 14:30:00 | NULL     | TRUE
```

#### T3: User Deleted (`__op = 'd'`)
**Current record closed (NO DELETE PHYSICALLY):**
```sql
user_id | username  | account_level | valid_from          | valid_to            | is_current
abc-123 | john_doe  | bronze        | 2025-11-01 08:00:00 | 2025-11-10 14:30:00 | FALSE
abc-123 | john_doe  | gold          | 2025-11-10 14:30:00 | 2025-11-19 16:00:00 | FALSE ← Updated
```

**Result**: Complete history preserved, current state shows user no longer exists (`is_current = FALSE` for all records)

---

## Why `__deleted` Field is NOT Needed

### `__deleted` Field Purpose:
The `__deleted` field was designed for scenarios where you want **additional metadata** about deleted records beyond just the operation type. For example:
- `__deleted = "true"` → This is a tombstone record
- `__deleted = "false"` → This is a normal record

### Your Configuration:
```properties
transforms.unwrap.delete.handling.mode=rewrite
```
This mode **rewrites delete events as updates** with `__op = 'd'`, which is exactly what you need for SCD Type 2.

### Alternatives (NOT in your config):
- `drop` → Delete events are dropped entirely (not suitable for SCD2)
- `none` → Delete events produce tombstone records with null values

---

## Querying Current vs Historical Data

### Get Current State (Active Users Only)
```sql
SELECT 
    user_id,
    username,
    account_level,
    total_playtime_hours
FROM bronze.dim_user
WHERE is_current = TRUE;
```

### Get Complete History (Including Deleted)
```sql
SELECT 
    user_id,
    username,
    account_level,
    valid_from,
    valid_to,
    is_current,
    CASE 
        WHEN is_current = FALSE AND valid_to IS NOT NULL THEN 'CLOSED'
        WHEN is_current = TRUE THEN 'ACTIVE'
    END as record_status
FROM bronze.dim_user
WHERE user_id = 'abc-123'
ORDER BY valid_from;
```

### Identify Deleted Users
```sql
-- Users where ALL records have is_current = FALSE
SELECT user_id
FROM bronze.dim_user
GROUP BY user_id
HAVING MAX(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) = 0;
```

---

## All CDC Operations Handled

| Operation | `__op` Value | SCD2 Action | `is_current` | `valid_to` |
|-----------|--------------|-------------|--------------|------------|
| **Create** | `'c'` or `'r'` | Insert new record | `TRUE` | `NULL` |
| **Update** | `'u'` | Close old + Insert new | Old: `FALSE`, New: `TRUE` | Old: `update_time`, New: `NULL` |
| **Delete** | `'d'` | Close current record | `FALSE` | `delete_time` |

---

## Verification Queries

### Check Delete Handling Works
```sql
-- Find records that were deleted (closed without a newer version)
SELECT 
    user_id,
    username,
    account_level,
    valid_from,
    valid_to,
    is_current
FROM bronze.dim_user
WHERE is_current = FALSE
  AND valid_to IS NOT NULL
  AND user_id NOT IN (
      SELECT user_id 
      FROM bronze.dim_user 
      WHERE is_current = TRUE
  )
ORDER BY valid_to DESC
LIMIT 10;
```

### Verify No Duplicate Current Records
```sql
-- Should return 0 rows
SELECT user_id, COUNT(*) as current_count
FROM bronze.dim_user
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1;
```

---

## Summary

✅ **Delete handling is FULLY FUNCTIONAL** using `__op = 'd'`  
✅ **SCD Type 2 versioning is INTACT** with `is_current` flag  
✅ **Complete history is PRESERVED** including deleted records  
✅ **No data loss** when records are deleted from source  
✅ **Point-in-time queries** are possible for auditing  

The `__deleted` field would be redundant because:
1. `__op = 'd'` already identifies delete events
2. `is_current = FALSE` + `valid_to != NULL` indicates a closed record
3. Absence of `is_current = TRUE` for a user_id indicates deletion

**Conclusion**: Your SCD Type 2 implementation is correct and complete without the `__deleted` column!



