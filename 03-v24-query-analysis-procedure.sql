USE schedules;


CREATE TABLE IF NOT EXISTS workload_test.caller_contention_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  caller_id STRING DEFAULT gen_random_uuid()::STRING,  -- to isolate caller
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
  index_recommendations STRING[]
);


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
);


CREATE OR REPLACE PROCEDURE workload_test.inspect_contention_from_exception(
  exception_str     STRING,
  OUT select_query  STRING,
  in_caller_id      STRING DEFAULT gen_random_uuid()::STRING,
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
    substring(exception_str FROM 'TransactionRetryWithProtoRefreshError:[[:space:]]*([A-Za-z_()]+):'),
    substring(exception_str FROM '[[:space:]]*key=([^ ]+)'),
    to_timestamp(substring(exception_str FROM '[[:space:]]*ts=([0-9]+\.[0-9]+)')::FLOAT8),
    substring(exception_str FROM '"sql txn" meta=\{id=([0-9A-Fa-f]+)')
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
    ON i.txn_fingerprint_id = c.waiting_txn_fingerprint_id
   AND i.stmt_fingerprint_id = c.waiting_stmt_fingerprint_id
  WHERE c.waiting_txn_id::STRING LIKE txn_id_prefix || '%'
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
    FROM workload_test.cluster_execution_insights AS i
    WHERE i.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.status = 'Failed'
      AND i.last_error_redactable LIKE '%' || retry_error_type || '%'
      AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
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
    FROM workload_test.cluster_execution_insights AS i
    WHERE i.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
      AND i.query NOT LIKE 'SHOW%'
    ORDER BY i.start_time DESC
    LIMIT 1;
  END IF;

  DELETE FROM workload_test.caller_contention_results WHERE caller_id = in_caller_id;

  -- Now join with transaction stats and insert into final results table
  INSERT INTO workload_test.caller_contention_results (
    caller_id,
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
    caller_id,
    tx_stmt.ord,
    CASE WHEN tx.fingerprint_id = f.blocking_txn_fingerprint_id THEN 'blocking' ELSE 'waiting' END AS role,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN 'failed' ELSE NULL END AS status,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.collection_ts ELSE NULL END AS collection_ts,
    tx.aggregated_ts,
    tx.app_name,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.database_name ELSE NULL END AS database_name,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.schema_name ELSE NULL END AS schema_name,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.table_name ELSE NULL END AS table_name,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.index_name ELSE NULL END AS index_name,
    tx.metadata,
    tx.statistics,
    CASE WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id THEN f.contention_type ELSE NULL END AS contention_type,
    CASE WHEN f.table_name IS NOT NULL AND st.metadata ? 'querySummary' AND st.metadata->>'querySummary' LIKE '%' || f.table_name || '%' THEN true ELSE false END AS contention,
    st.fingerprint_id,
    st.transaction_fingerprint_id,
    st.plan_hash,
    st.metadata,
    st.statistics,
    st.sampled_plan,
    st.aggregation_interval,
    st.index_recommendations
  FROM workload_test.transaction_statistics AS tx
  JOIN workload_test.caller_failed_statement AS f
    ON tx.fingerprint_id IN (f.blocking_txn_fingerprint_id, f.waiting_txn_fingerprint_id)
   AND tx.aggregated_ts BETWEEN date_trunc('hour', f.collection_ts) AND date_trunc('hour', f.collection_ts) + INTERVAL '1 hour'
   AND (
     (tx.fingerprint_id = f.blocking_txn_fingerprint_id AND (
       (in_option = 'same_app' AND tx.app_name = f.app_name) OR
       (in_option = 'diff_app' AND tx.app_name <> f.app_name) OR
       in_option = 'any_app'))
     OR (tx.fingerprint_id <> f.blocking_txn_fingerprint_id OR f.blocking_txn_fingerprint_id IS NULL)
       AND tx.app_name = f.app_name
   )
  JOIN LATERAL (
    SELECT
      ord,
      tx.fingerprint_id,
      decode(stmt_hex, 'hex')  AS stmt_fingerprint_id
    FROM jsonb_array_elements_text(tx.metadata->'stmtFingerprintIDs')
    WITH ORDINALITY AS arr(stmt_hex, ord)
  ) AS tx_stmt ON tx_stmt.fingerprint_id = tx.fingerprint_id
  JOIN workload_test.statement_statistics AS st
    ON st.transaction_fingerprint_id = tx.fingerprint_id
   AND st.fingerprint_id = tx_stmt.stmt_fingerprint_id
   AND st.aggregated_ts = tx.aggregated_ts
   AND st.app_name = tx.app_name;
  
  select_query :=
    'SELECT ' ||
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
    '  stmt_metadata->''query'' AS sql_statement ' ||
    'FROM workload_test.caller_contention_results ' ||
    'WHERE caller_id = ' || quote_literal(in_caller_id) || ' ' ||
    'ORDER BY ord;';
END;
$$;
