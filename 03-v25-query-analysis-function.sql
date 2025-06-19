USE schedules;


CREATE OR REPLACE FUNCTION workload_test.inspect_contention_from_exception(
  exception_str  STRING,
  in_app_name    STRING DEFAULT NULL,
  in_schema_name STRING DEFAULT NULL,
  in_option      STRING DEFAULT 'same_app'
)
RETURNS TABLE (
  ord                        INT,
  role                       STRING,
  status                     STRING,
  collection_ts              TIMESTAMPTZ,
  aggregated_ts              TIMESTAMPTZ,
  app_name                   STRING,
  database_name              STRING,
  schema_name                STRING,
  table_name                 STRING,
  index_name                 STRING,
  txn_metadata               JSONB,
  txn_statistics             JSONB,
  contention_type            STRING,
  contention                 BOOL,
  fingerprint_id             BYTES,
  transaction_fingerprint_id BYTES,
  plan_hash                  BYTES,
  stmt_metadata              JSONB,
  stmt_statistics            JSONB,
  sampled_plan               JSONB,
  aggregation_interval       INTERVAL,
  index_recommendations      STRING[]
) AS $$
WITH params AS (
  SELECT
    exception_str,
    in_app_name,
    in_schema_name,
    in_option,
    
    -- extract the four pieces from the exception
    
    substring(exception_str
      FROM 'TransactionRetryWithProtoRefreshError:[[:space:]]*([A-Za-z_()]+):'
    ) AS retry_error_type,

    substring(exception_str
      FROM '[[:space:]]*key=([^ ]+)'
    ) AS contention_key,

    to_timestamp(
      substring(exception_str
        FROM '[[:space:]]*ts=([0-9]+\.[0-9]+)'
      )::FLOAT8
    )::timestamptz AS conflict_ts,

    substring(exception_str
      FROM '"sql txn" meta=\{id=([0-9A-Fa-f]+)'
    ) AS txn_id_prefix
),
contention AS (
  SELECT
    e.collection_ts,
    e.blocking_txn_fingerprint_id,
    e.waiting_txn_fingerprint_id,
    e.database_name,
    in_app_name AS app_name,
    e.schema_name,
    e.table_name,
    e.index_name,
    e.contention_type,
    e.waiting_stmt_fingerprint_id AS stmt_fingerprint_id
  FROM schedules.test_run_transaction_contention_events AS e, params
  WHERE e.waiting_txn_id::STRING LIKE params.txn_id_prefix || '%'
    AND e.contending_pretty_key = params.contention_key
    AND e.collection_ts BETWEEN params.conflict_ts AND params.conflict_ts + INTERVAL '60 seconds'
    AND (params.in_schema_name IS NULL OR e.schema_name = params.in_schema_name)
  ORDER BY e.collection_ts
  LIMIT 1
),
insights AS (
  SELECT
    ci.start_time::timestamptz AS collection_ts,
    NULL AS blocking_txn_fingerprint_id,
    ci.txn_fingerprint_id AS waiting_txn_fingerprint_id,
    ci.database_name,
    ci.app_name,
    in_schema_name AS schema_name,
    NULL AS table_name,
    NULL AS index_name,
    NULL AS contention_type,
    ci.stmt_fingerprint_id
  FROM schedules.test_run_cluster_execution_insights AS ci, params
  WHERE ci.txn_id::STRING LIKE params.txn_id_prefix || '%'
    AND ci.status = 'Failed'
    AND ci.last_error_redactable LIKE '%' || params.retry_error_type || '%'
    AND ci.start_time BETWEEN params.conflict_ts - INTERVAL '30 seconds' AND params.conflict_ts + INTERVAL '30 seconds'
    AND (params.in_app_name IS NULL OR ci.app_name = params.in_app_name)
  ORDER BY ci.start_time
  LIMIT 1
),
failed AS (
  -- take the real contention row, but pull in app_name from insights if there
  SELECT
    c.collection_ts,
    c.blocking_txn_fingerprint_id,
    c.waiting_txn_fingerprint_id,
    c.database_name,
    COALESCE(i.app_name, c.app_name) AS app_name,
    c.schema_name,
    c.table_name,
    c.index_name,
    c.contention_type,
	COALESCE(i.stmt_fingerprint_id, c.stmt_fingerprint_id) AS stmt_fingerprint_id
  FROM contention AS c
  LEFT JOIN insights AS i
    ON i.waiting_txn_fingerprint_id = c.waiting_txn_fingerprint_id
   AND i.stmt_fingerprint_id = c.stmt_fingerprint_id

  UNION ALL

  -- if *no* contention row existed, fall back to insights
  SELECT *
  FROM insights
  WHERE NOT EXISTS (SELECT 1 FROM contention)
  
  UNION ALL

  -- if *no* failed record exists in insights, find closest match
  SELECT
    start_time::TIMESTAMPTZ AS collection_ts,
    NULL AS blocking_txn_fingerprint_id,
    txn_fingerprint_id AS waiting_txn_fingerprint_id,
    database_name,
    app_name,
    in_schema_name AS schema_name,
    NULL AS table_name,
    NULL AS index_name,
    NULL AS contention_type,
    stmt_fingerprint_id
  FROM schedules.test_run_cluster_execution_insights, params
  WHERE txn_id::STRING LIKE params.txn_id_prefix || '%'
	AND start_time BETWEEN params.conflict_ts - INTERVAL '30 seconds' AND params.conflict_ts + INTERVAL '30 seconds'
	AND (params.in_app_name IS NULL OR app_name = params.in_app_name)
	AND query NOT LIKE 'SHOW%'
	AND NOT EXISTS (SELECT 1 FROM insights)
  ORDER BY 1 DESC
  LIMIT 1
),
transactions AS (
  SELECT
    CASE
      WHEN tx.fingerprint_id = f.blocking_txn_fingerprint_id
      THEN 'blocking'
      ELSE 'waiting'
    END AS role,
    f.collection_ts,
    tx.aggregated_ts,
    tx.fingerprint_id,
    tx.app_name,
    f.database_name,
    f.schema_name,
    f.table_name,
    f.index_name,
    tx.metadata,
    tx.statistics,
    tx.aggregation_interval,
    f.contention_type,
    f.stmt_fingerprint_id
  FROM schedules.test_run_transaction_statistics AS tx
  JOIN failed AS f
    ON tx.fingerprint_id IN (f.blocking_txn_fingerprint_id, f.waiting_txn_fingerprint_id)
   AND tx.aggregated_ts BETWEEN date_trunc('hour', f.collection_ts) AND date_trunc('hour', f.collection_ts) + INTERVAL '1 hour'
   AND (
     (
       tx.fingerprint_id = f.blocking_txn_fingerprint_id
       AND (
         (in_option = 'same_app' AND tx.app_name = f.app_name)
         OR (in_option = 'diff_app' AND tx.app_name <> f.app_name)
         OR  in_option = 'any_app'
       )
     )
     OR (
       (tx.fingerprint_id <> f.blocking_txn_fingerprint_id
        OR f.blocking_txn_fingerprint_id IS NULL)
       AND tx.app_name = f.app_name
     )
   )
),
stmt AS (
  SELECT
    t.fingerprint_id,
    t.metadata->'stmtFingerprintIDs' AS stmt_ids
  FROM transactions AS t
),
exploded AS (
  SELECT
    fingerprint_id AS txn_id,
    decode(jsonb_array_elements_text(stmt_ids), 'hex') AS stmt_id,
    row_number() OVER (PARTITION BY fingerprint_id) AS ord
  FROM stmt
)
-- final projection
SELECT
  ex.ord,
  tx.role,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN 'failed'
    ELSE NULL
  END AS status,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.collection_ts
    ELSE NULL
  END AS collection_ts,
  tx.aggregated_ts,
  tx.app_name,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.database_name
    ELSE NULL
  END AS database_name,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.schema_name
    ELSE NULL
  END AS schema_name,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.table_name
    ELSE NULL
  END AS table_name,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.index_name
    ELSE NULL
  END AS index_name,
  tx.metadata AS txn_metadata,
  tx.statistics AS txn_statistics,
  CASE
    WHEN tx.role = 'waiting'
     AND tx.stmt_fingerprint_id = st.fingerprint_id
    THEN tx.contention_type
    ELSE NULL
  END AS contention_type,
  CASE
    WHEN tx.table_name IS NOT NULL
     AND st.metadata ? 'querySummary'
     AND st.metadata->>'querySummary'
         LIKE '%' || tx.table_name || '%'
    THEN true
    ELSE false
  END AS contention,
  st.fingerprint_id,
  st.transaction_fingerprint_id,
  st.plan_hash,
  st.metadata AS stmt_metadata,
  st.statistics AS stmt_statistics,
  st.sampled_plan,
  st.aggregation_interval,
  st.index_recommendations
FROM schedules.test_run_statement_statistics AS st
JOIN transactions AS tx
  ON tx.fingerprint_id = st.transaction_fingerprint_id
 AND tx.aggregated_ts = st.aggregated_ts
 AND tx.app_name = st.app_name
JOIN exploded AS ex
  ON ex.txn_id = st.transaction_fingerprint_id
 AND ex.stmt_id = st.fingerprint_id
ORDER BY tx.role, ex.ord;
$$ LANGUAGE SQL;
