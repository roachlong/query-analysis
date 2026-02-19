USE schedules;


CREATE TABLE IF NOT EXISTS workload_test.caller_contention_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  caller_id STRING DEFAULT gen_random_uuid()::STRING,  -- to isolate caller
  test_run STRING NOT NULL,
  ord INT,
  role STRING,
  status STRING,
  collection_ts TIMESTAMPTZ,
  aggregated_ts TIMESTAMPTZ,
  app_name STRING,
  database_name STRING,
  schema_name STRING,
  table_name STRING,
  index_name STRING,
  txn_metadata JSONB,
  txn_statistics JSONB,
  contention_type STRING,
  contention BOOL,
  fingerprint_id BYTES,
  transaction_fingerprint_id BYTES,
  plan_hash BYTES,
  stmt_metadata JSONB,
  stmt_statistics JSONB,
  sampled_plan JSONB,
  aggregation_interval INTERVAL,
  index_recommendations STRING[],
  CONSTRAINT fk_ccr_to_trc FOREIGN KEY (test_run)
      REFERENCES workload_test.test_run_configurations (test_run)
  ON DELETE CASCADE
)
WITH (ttl = 'on', ttl_expiration_expression = e'(aggregated_ts + INTERVAL \'90 days\')');


CREATE TABLE IF NOT EXISTS workload_test.caller_failed_statement (
  caller_id STRING PRIMARY KEY,
  collection_ts TIMESTAMPTZ,
  blocking_txn_fingerprint_id BYTES,
  waiting_txn_fingerprint_id BYTES,
  database_name TEXT,
  app_name TEXT,
  schema_name TEXT,
  table_name TEXT,
  index_name TEXT,
  contention_type TEXT,
  stmt_fingerprint_id BYTES
)
WITH (ttl = 'on', ttl_expiration_expression = e'(collection_ts + INTERVAL \'90 days\')');


CREATE OR REPLACE PROCEDURE workload_test.inspect_contention_from_exception(
  exception_str     STRING,
  OUT select_query  STRING,
  in_caller_id      STRING DEFAULT gen_random_uuid()::STRING,
  in_test_run       STRING DEFAULT NULL,
  in_app_name       STRING DEFAULT NULL,
  in_schema_name    STRING DEFAULT NULL,
  in_option         STRING DEFAULT 'same_app'
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  retry_error_type  STRING;
  contention_key    STRING;
  conflict_ts       TIMESTAMPTZ;
  txn_id_prefix     STRING;
BEGIN
  -- RAISE NOTICE 'DEBUG parameters: in_caller_id=%, in_app_name=%, in_schema_name=%, in_option=%',
  --   in_caller_id, in_app_name, in_schema_name, in_option;

  -- Extract values from exception_str into variables
  SELECT
    -- retry_error_type
    substring(exception_str 
      FROM 'TransactionRetryWithProtoRefreshError:[[:space:]]*([A-Za-z_()]+):'
    ),

    -- contention_key, only match the key inside the conflicting txn: meta={…}
    regexp_replace(
      substring(exception_str 
        FROM 'conflicting txn: meta=\{[^}]*key=([^ ]+)'
      ),
      E'\\\\(["\\\\])',   -- match \" or \\
      E'\\1',             -- keep just " or \
      'g'
    ) AS contention_key,

    -- conflict_ts, only match the ts inside the same block
    to_timestamp(
      substring(exception_str 
        FROM 'conflicting txn: meta=\{[^}]*ts=([0-9]+\.[0-9]+)'
      )::FLOAT8
    ),

    -- txn_id_prefix
    substring(exception_str 
      FROM '(?:"|\\")sql txn(?:"|\\") meta=\{id=([0-9A-Fa-f]+)'
    )
  INTO retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  -- RAISE NOTICE 'DEBUG extracted: retry_error_type=%, key=%, ts=%, txn_id_prefix=%',
  --   retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  DELETE FROM workload_test.caller_failed_statement WHERE caller_id = in_caller_id;

  INSERT INTO workload_test.caller_failed_statement
  SELECT
    in_caller_id,
    c.collection_ts,
    c.blocking_txn_fingerprint_id,
    c.waiting_txn_fingerprint_id,
    c.database_name,
    COALESCE(i.app_name, in_app_name) AS app_name,
    c.schema_name,
    c.table_name,
    c.index_name,
    c.contention_type,
    COALESCE(i.stmt_fingerprint_id, c.waiting_stmt_fingerprint_id) AS stmt_fingerprint_id
  FROM workload_test.transaction_contention_events AS c
  LEFT JOIN workload_test.cluster_execution_insights AS i
    ON i.test_run = c.test_run
   AND i.txn_fingerprint_id = c.waiting_txn_fingerprint_id
   AND i.stmt_fingerprint_id = c.waiting_stmt_fingerprint_id
  WHERE c.test_run = in_test_run
    AND c.waiting_txn_id::STRING LIKE txn_id_prefix || '%'
    AND c.contending_pretty_key = contention_key
    AND c.collection_ts BETWEEN conflict_ts AND conflict_ts + INTERVAL '60 seconds'
    AND (in_schema_name IS NULL OR c.schema_name = in_schema_name)
  ORDER BY c.collection_ts
  LIMIT 1;

  IF NOT EXISTS (
      SELECT 1 FROM workload_test.caller_failed_statement
      WHERE caller_id = in_caller_id
    ) THEN
    INSERT INTO workload_test.caller_failed_statement
    SELECT
      in_caller_id,
      i.start_time::timestamptz AS collection_ts,
      NULL AS blocking_txn_fingerprint_id,
      i.txn_fingerprint_id AS waiting_txn_fingerprint_id,
      i.database_name,
      i.app_name,
      in_schema_name AS schema_name,
      NULL AS table_name,
      NULL AS index_name,
      NULL AS contention_type,
      i.stmt_fingerprint_id
    FROM workload_test.txn_id_map AS m
    JOIN workload_test.cluster_execution_insights AS i
      ON i.test_run               = m.test_run
     AND i.txn_fingerprint_id     = m.txn_fingerprint_id
    WHERE m.test_run = in_test_run
      AND m.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.status = 'Failed'
      AND i.last_error_redactable LIKE '%' || retry_error_type || '%'
      -- AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
    ORDER BY i.start_time
    LIMIT 1;
  END IF;

  IF NOT EXISTS (
      SELECT 1 FROM workload_test.caller_failed_statement
      WHERE caller_id = in_caller_id
    ) THEN
    INSERT INTO workload_test.caller_failed_statement
    SELECT
      in_caller_id,
      i.start_time::timestamptz AS collection_ts,
      NULL AS blocking_txn_fingerprint_id,
      i.txn_fingerprint_id AS waiting_txn_fingerprint_id,
      i.database_name,
      i.app_name,
      in_schema_name AS schema_name,
      NULL AS table_name,
      NULL AS index_name,
      NULL AS contention_type,
      i.stmt_fingerprint_id
    FROM workload_test.txn_id_map AS m
    JOIN workload_test.cluster_execution_insights AS i
      ON i.test_run               = m.test_run
     AND i.txn_fingerprint_id     = m.txn_fingerprint_id
    WHERE m.test_run = in_test_run
      AND m.txn_id::TEXT LIKE txn_id_prefix || '%'
      -- AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
      AND i.query NOT LIKE 'SHOW%'
    ORDER BY i.start_time DESC
    LIMIT 1;
  END IF;

  DELETE FROM workload_test.caller_contention_results WHERE caller_id = in_caller_id;

  -- Now join with transaction stats and insert into final results table
  INSERT INTO workload_test.caller_contention_results (
    caller_id,
    test_run,
    ord,
    role,
    status,
    collection_ts,
    aggregated_ts,
    app_name,
    database_name,
    schema_name,
    table_name,
    index_name,
    txn_metadata,
    txn_statistics,
    contention_type,
    contention,
    fingerprint_id,
    transaction_fingerprint_id,
    plan_hash,
    stmt_metadata,
    stmt_statistics,
    sampled_plan,
    aggregation_interval,
    index_recommendations
  )

  SELECT
    f.caller_id,
    tx.test_run,
    tx_stmt.ord,
    tx.role_kind AS role,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN 'failed' 
    END AS status,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.collection_ts 
    END AS collection_ts,
    tx.aggregated_ts,
    tx.app_name,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.database_name 
    END AS database_name,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.schema_name 
    END AS schema_name,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.table_name 
    END AS table_name,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.index_name 
    END AS index_name,
    tx.metadata,
    tx.statistics,
    CASE 
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id 
        AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id 
      THEN f.contention_type 
    END AS contention_type,
    CASE 
      WHEN f.table_name IS NOT NULL 
        AND st.metadata ? 'querySummary' 
        AND st.metadata->>'querySummary' LIKE '%'||f.table_name||'%' 
      THEN true 
      ELSE false 
    END AS contention,
    st.fingerprint_id,
    st.transaction_fingerprint_id,
    st.plan_hash,
    st.metadata,
    st.statistics,
    st.sampled_plan,
    st.aggregation_interval,
    st.index_recommendations
    
  FROM workload_test.caller_failed_statement AS f

  -- pick the single tx row whose aggregated_ts is the latest ≤ hour(f.collection_ts)
  JOIN LATERAL (
    SELECT *
    FROM (
      SELECT
        tx2.*,
        CASE
          WHEN f.blocking_txn_fingerprint_id = f.waiting_txn_fingerprint_id THEN 'both'
          WHEN tx2.fingerprint_id = f.blocking_txn_fingerprint_id THEN 'blocking'
          ELSE 'waiting'
        END AS role_kind,
        row_number() OVER (
          PARTITION BY
            CASE
              WHEN f.blocking_txn_fingerprint_id = f.waiting_txn_fingerprint_id THEN 'both'
              WHEN tx2.fingerprint_id = f.blocking_txn_fingerprint_id THEN 'blocking'
              ELSE 'waiting'
            END
          ORDER BY tx2.aggregated_ts DESC
        ) AS rn
      FROM workload_test.cluster_transaction_statistics AS tx2
      WHERE tx2.fingerprint_id IN (
              f.blocking_txn_fingerprint_id,
              f.waiting_txn_fingerprint_id
            )
        AND (
          (tx2.fingerprint_id = f.blocking_txn_fingerprint_id AND (
            (in_option = 'same_app' AND tx2.app_name = f.app_name) OR
            (in_option = 'diff_app' AND tx2.app_name <> f.app_name) OR
            in_option = 'any_app'
          ))
          OR ((tx2.fingerprint_id <> f.blocking_txn_fingerprint_id
              OR f.blocking_txn_fingerprint_id IS NULL)
              AND tx2.app_name = f.app_name)
        )
        AND tx2.aggregated_ts <= f.collection_ts + interval '2 hours'
    ) s
    WHERE s.rn = 1
  ) AS tx ON true

  -- unnest the stmtFingerprintIDs with ordinality
  JOIN LATERAL (
    SELECT
      arr.ord,
      tx.fingerprint_id,
      decode(arr.stmt_hex, 'hex') AS stmt_fingerprint_id
    FROM jsonb_array_elements_text(
          tx.metadata->'stmtFingerprintIDs'
        ) WITH ORDINALITY AS arr(stmt_hex, ord)
  ) AS tx_stmt
    ON tx_stmt.fingerprint_id = tx.fingerprint_id

  -- pick the single stmt row whose aggregated_ts is the latest ≤ tx.aggregated_ts
  JOIN LATERAL (
    SELECT st2.*
    FROM workload_test.cluster_statement_statistics AS st2
    WHERE st2.test_run = tx.test_run
      AND st2.transaction_fingerprint_id = tx.fingerprint_id
      AND st2.fingerprint_id = tx_stmt.stmt_fingerprint_id
      AND st2.app_name = tx.app_name
      AND st2.aggregated_ts <= tx.aggregated_ts
    ORDER BY st2.aggregated_ts DESC
    LIMIT 1
  ) AS st ON true

  WHERE tx.test_run = in_test_run
    AND f.caller_id = in_caller_id

  ORDER BY tx.fingerprint_id, tx_stmt.ord;
  
  select_query :=
    'SELECT ' ||
    '  test_run, ' ||
    '  collection_ts, ' ||
    '  database_name, ' ||
    '  schema_name, ' ||
    '  table_name, ' ||
    '  index_name, ' ||
    '  contention_type, ' ||
    '  app_name, ' ||
    '  encode(transaction_fingerprint_id, ''hex'') AS txn_fingerprint_id, ' ||
    '  role AS tnx_type, ' ||
    '  contention, ' ||
    '  encode(fingerprint_id, ''hex'') AS stmt_fingerprint_id, ' ||
    '  stmt_metadata->''fullScan'' AS fullscan, ' ||
    '  index_recommendations, ' ||
    '  ord AS stmt_order, ' ||
    '  status, ' ||
    '  stmt_metadata->''query'' AS sql_statement ' ||
    'FROM workload_test.caller_contention_results ' ||
    'WHERE caller_id = ' || quote_literal(in_caller_id) || ' ' ||
    'ORDER BY test_run, role, ord;';
END;
$$;
