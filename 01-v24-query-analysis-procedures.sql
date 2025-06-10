USE schedules;



CREATE OR REPLACE FUNCTION schedules.copy_test_run_observations(
  in_test_run  BIGINT        DEFAULT NULL,
  in_from_ts   TIMESTAMPTZ   DEFAULT NULL,
  in_to_ts     TIMESTAMPTZ   DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  run_id     BIGINT;
  from_ts    TIMESTAMPTZ;
  to_ts      TIMESTAMPTZ;
  cnt_cont   BIGINT;
  cnt_ins    BIGINT;
  cnt_stmt   BIGINT;
  cnt_txn    BIGINT;
BEGIN
  -- 1) determine run ID and time window
  SELECT
    COALESCE(in_test_run, COALESCE(MAX(test_run) + 1, 0)),
    COALESCE(in_from_ts,  NOW() - INTERVAL '24 hours'),
    COALESCE(in_to_ts,    NOW())
  INTO run_id, from_ts, to_ts
  FROM schedules.test_run_transaction_statistics;

  -- 2) insert contention events
  INSERT INTO schedules.test_run_contention_events (
    test_run,
    collection_ts,
    blocking_txn_id,
    blocking_txn_fingerprint_id,
    waiting_txn_id,
    waiting_txn_fingerprint_id,
    contention_duration,
    contending_key,
    contending_pretty_key,
    waiting_stmt_id,
    waiting_stmt_fingerprint_id,
    database_name,
    schema_name,
    table_name,
    index_name,
    contention_type
  )
  SELECT
    run_id,
    e.collection_ts,
    e.blocking_txn_id,
    e.blocking_txn_fingerprint_id,
    e.waiting_txn_id,
    e.waiting_txn_fingerprint_id,
    e.contention_duration,
    e.contending_key,
    e.contending_pretty_key,
    e.waiting_stmt_id,
    e.waiting_stmt_fingerprint_id,
    e.database_name,
    e.schema_name,
    e.table_name,
    e.index_name,
    e.contention_type
  FROM crdb_internal.transaction_contention_events AS e
  WHERE e.collection_ts BETWEEN from_ts AND to_ts;

  -- 3) insert execution insights
  INSERT INTO schedules.test_run_execution_insights (
    test_run,
    session_id,
    txn_id,
    txn_fingerprint_id,
    stmt_id,
    stmt_fingerprint_id,
    problem,
    causes,
    query,
    status,
    start_time,
    end_time,
    full_scan,
    user_name,
    app_name,
    database_name,
    plan_gist,
    rows_read,
    rows_written,
    priority,
    retries,
    last_retry_reason,
    exec_node_ids,
    kv_node_ids,
    contention,
    index_recommendations,
    implicit_txn,
    cpu_sql_nanos,
    error_code,
    last_error_redactable
  )
  SELECT
    run_id,
    i.session_id,
    i.txn_id,
    i.txn_fingerprint_id,
    i.stmt_id,
    i.stmt_fingerprint_id,
    i.problem,
    i.causes,
    i.query,
    i.status,
    i.start_time,
    i.end_time,
    i.full_scan,
    i.user_name,
    i.app_name,
    i.database_name,
    i.plan_gist,
    i.rows_read,
    i.rows_written,
    i.priority,
    i.retries,
    i.last_retry_reason,
    i.exec_node_ids,
    i.kv_node_ids,
    i.contention,
    i.index_recommendations,
    i.implicit_txn,
    i.cpu_sql_nanos,
    i.error_code,
    i.last_error_redactable
  FROM crdb_internal.cluster_execution_insights AS i
  WHERE i.start_time BETWEEN from_ts AND to_ts;

  -- 4) insert statement stats
  INSERT INTO schedules.test_run_statement_statistics (
    test_run,
    aggregated_ts,
    fingerprint_id,
    transaction_fingerprint_id,
    plan_hash,
    app_name,
    metadata,
    statistics,
    sampled_plan,
    aggregation_interval,
    index_recommendations
  )
  SELECT
    run_id,
    s.aggregated_ts,
    s.fingerprint_id,
    s.transaction_fingerprint_id,
    s.plan_hash,
    s.app_name,
    s.metadata,
    s.statistics,
    s.sampled_plan,
    s.aggregation_interval,
    s.index_recommendations
  FROM crdb_internal.statement_statistics AS s
  WHERE s.aggregated_ts BETWEEN from_ts AND to_ts;

  -- 5) insert transaction stats
  INSERT INTO schedules.test_run_transaction_statistics (
    test_run,
    aggregated_ts,
    fingerprint_id,
    app_name,
    metadata,
    statistics,
    aggregation_interval
  )
  SELECT
    run_id,
    x.aggregated_ts,
    x.fingerprint_id,
    x.app_name,
    x.metadata,
    x.statistics,
    x.aggregation_interval
  FROM crdb_internal.transaction_statistics AS x
  WHERE x.aggregated_ts BETWEEN from_ts AND to_ts;

  -- 6) now compute counts by querying each target table
  SELECT COUNT(*) INTO cnt_cont
    FROM schedules.test_run_contention_events
   WHERE test_run = run_id
     AND collection_ts BETWEEN from_ts AND to_ts;

  SELECT COUNT(*) INTO cnt_ins
    FROM schedules.test_run_execution_insights
   WHERE test_run = run_id
     AND start_time BETWEEN from_ts AND to_ts;

  SELECT COUNT(*) INTO cnt_stmt
    FROM schedules.test_run_statement_statistics
   WHERE test_run = run_id
     AND aggregated_ts BETWEEN from_ts AND to_ts;

  SELECT COUNT(*) INTO cnt_txn
    FROM schedules.test_run_transaction_statistics
   WHERE test_run = run_id
     AND aggregated_ts BETWEEN from_ts AND to_ts;

  -- 7) return a single JSONB object
  RETURN jsonb_build_object(
    'contention',   cnt_cont,
    'insights',     cnt_ins,
    'statements',   cnt_stmt,
    'transactions', cnt_txn
  );
END;
$$;





CREATE TABLE IF NOT EXISTS schedules.inspect_contention_results (
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





CREATE TABLE IF NOT EXISTS schedules.staging_failed_selection (
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





CREATE OR REPLACE PROCEDURE schedules.inspect_contention_from_exception(
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
  RAISE NOTICE 'DEBUG parameters: in_caller_id=%, in_app_name=%, in_schema_name=%, in_option=%',
    in_caller_id, in_app_name, in_schema_name, in_option;

  -- Extract values from exception_str into variables
  SELECT
    substring(exception_str FROM 'TransactionRetryWithProtoRefreshError:[[:space:]]*([A-Za-z_()]+):'),
    substring(exception_str FROM '[[:space:]]*key=([^ ]+)'),
    to_timestamp(substring(exception_str FROM '[[:space:]]*ts=([0-9]+\.[0-9]+)')::FLOAT8),
    substring(exception_str FROM '"sql txn" meta=\{id=([0-9A-Fa-f]+)')
  INTO retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  RAISE NOTICE 'DEBUG extracted: retry_error_type=%, key=%, ts=%, txn_id_prefix=%',
    retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  DELETE FROM schedules.staging_failed_selection WHERE caller_id = in_caller_id;

  INSERT INTO schedules.staging_failed_selection
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
  FROM crdb_internal.transaction_contention_events AS c
  LEFT JOIN crdb_internal.cluster_execution_insights AS i
    ON i.txn_fingerprint_id = c.waiting_txn_fingerprint_id
   AND i.stmt_fingerprint_id = c.waiting_stmt_fingerprint_id
  WHERE c.waiting_txn_id::STRING LIKE txn_id_prefix || '%'
    AND c.contending_pretty_key = contention_key
    AND c.collection_ts BETWEEN conflict_ts AND conflict_ts + INTERVAL '60 seconds'
    AND (in_schema_name IS NULL OR c.schema_name = in_schema_name)
  ORDER BY c.collection_ts
  LIMIT 1;

  IF NOT EXISTS (
      SELECT 1 FROM schedules.staging_failed_selection
      WHERE caller_id = in_caller_id
    ) THEN
    INSERT INTO schedules.staging_failed_selection
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
    FROM crdb_internal.cluster_execution_insights AS i
    WHERE i.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.status = 'Failed'
      AND i.last_error_redactable LIKE '%' || retry_error_type || '%'
      AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
    ORDER BY i.start_time
    LIMIT 1;
  END IF;

  IF NOT EXISTS (
      SELECT 1 FROM schedules.staging_failed_selection
      WHERE caller_id = in_caller_id
    ) THEN
    INSERT INTO schedules.staging_failed_selection
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
    FROM crdb_internal.cluster_execution_insights AS i
    WHERE i.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
      AND i.query NOT LIKE 'SHOW%'
    ORDER BY i.start_time DESC
    LIMIT 1;
  END IF;

  DELETE FROM schedules.inspect_contention_results WHERE caller_id = in_caller_id;

  -- Now join with transaction stats and insert into final results table
  INSERT INTO schedules.inspect_contention_results (
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
  FROM schedules.test_run_transaction_statistics AS tx
  JOIN schedules.staging_failed_selection AS f
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
  JOIN schedules.test_run_statement_statistics AS st
    ON st.transaction_fingerprint_id = tx.fingerprint_id
   AND st.fingerprint_id = tx_stmt.stmt_fingerprint_id
   AND st.aggregated_ts = tx.aggregated_ts
   AND st.app_name = tx.app_name;
  
  select_query :=
    'SELECT * ' ||
    'FROM schedules.inspect_contention_results ' ||
    'WHERE caller_id = ' || quote_literal(in_caller_id) || ' ' ||
    'ORDER BY ord;';
END;
$$;
