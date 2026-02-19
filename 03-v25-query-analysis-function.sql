USE schedules;


CREATE OR REPLACE FUNCTION workload_test.inspect_contention_from_exception(
  exception_str     STRING,
  in_test_run       STRING,
  in_app_name       STRING DEFAULT NULL,
  in_schema_name    STRING DEFAULT NULL,
  in_option         STRING DEFAULT 'same_app'
)
RETURNS TABLE (
  test_run                 STRING,
  ord                      INT,
  role                     STRING,
  status                   STRING,
  collection_ts            TIMESTAMPTZ,
  aggregated_ts            TIMESTAMPTZ,
  app_name                 STRING,
  database_name            STRING,
  schema_name              STRING,
  table_name               STRING,
  index_name               STRING,
  txn_metadata             JSONB,
  txn_statistics           JSONB,
  contention_type          STRING,
  contention               BOOL,
  fingerprint_id           BYTES,
  transaction_fingerprint_id BYTES,
  plan_hash                BYTES,
  stmt_metadata            JSONB,
  stmt_statistics          JSONB,
  sampled_plan             JSONB,
  aggregation_interval     INTERVAL,
  index_recommendations    STRING[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  retry_error_type  STRING;
  contention_key    STRING;
  conflict_ts       TIMESTAMPTZ;
  txn_id_prefix     STRING;
BEGIN

  -- Extract values from exception_str into variables (EXACT patterns from v24)
  SELECT
    substring(exception_str
      FROM 'TransactionRetryWithProtoRefreshError:[[:space:]]*([A-Za-z_()]+):'
    ),

    regexp_replace(
      substring(exception_str
        FROM 'conflicting txn: meta=\{[^}]*key=([^ ]+)'
      ),
      E'\\\\(["\\\\])',   -- match \" or \\
      E'\\1',             -- keep just " or \
      'g'
    ),

    to_timestamp(
      substring(exception_str
        FROM 'conflicting txn: meta=\{[^}]*ts=([0-9]+\.[0-9]+)'
      )::FLOAT8
    ),

    substring(exception_str
      FROM '(?:"|\\")sql txn(?:"|\\") meta=\{id=([0-9A-Fa-f]+)'
    )
  INTO retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  -- RAISE NOTICE 'DEBUG extracted: retry_error_type=%, key=%, ts=%, txn_id_prefix=%',
  --   retry_error_type, contention_key, conflict_ts, txn_id_prefix;

  RETURN QUERY
  WITH
  /* -----------------------------
     Stage 1: primary match from contention events
     (same join/predicates/order/limit)
     ----------------------------- */
  stage1 AS (
    SELECT
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
    LIMIT 1
  ),

  /* -----------------------------
     Stage 2: fallback via txn_id_map + insights with Failed + error match
     (same join/predicates/order/limit)
     ----------------------------- */
  stage2 AS (
    SELECT
      i.start_time::timestamptz AS collection_ts,
      NULL::BYTES               AS blocking_txn_fingerprint_id,
      i.txn_fingerprint_id      AS waiting_txn_fingerprint_id,
      i.database_name           AS database_name,
      i.app_name                AS app_name,
      in_schema_name            AS schema_name,
      NULL::STRING              AS table_name,
      NULL::STRING              AS index_name,
      NULL::STRING              AS contention_type,
      i.stmt_fingerprint_id     AS stmt_fingerprint_id
    FROM workload_test.txn_id_map AS m
    JOIN workload_test.cluster_execution_insights AS i
      ON i.test_run           = m.test_run
     AND i.txn_fingerprint_id = m.txn_fingerprint_id
    WHERE m.test_run = in_test_run
      AND m.txn_id::TEXT LIKE txn_id_prefix || '%'
      AND i.status = 'Failed'
      AND i.last_error_redactable LIKE '%' || retry_error_type || '%'
      -- AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
    ORDER BY i.start_time
    LIMIT 1
  ),

  /* -----------------------------
     Stage 3: fallback via txn_id_map + insights, exclude SHOW%, newest first
     (same join/predicates/order/limit)
     ----------------------------- */
  stage3 AS (
    SELECT
      i.start_time::timestamptz AS collection_ts,
      NULL::BYTES               AS blocking_txn_fingerprint_id,
      i.txn_fingerprint_id      AS waiting_txn_fingerprint_id,
      i.database_name           AS database_name,
      i.app_name                AS app_name,
      in_schema_name            AS schema_name,
      NULL::STRING              AS table_name,
      NULL::STRING              AS index_name,
      NULL::STRING              AS contention_type,
      i.stmt_fingerprint_id     AS stmt_fingerprint_id
    FROM workload_test.txn_id_map AS m
    JOIN workload_test.cluster_execution_insights AS i
      ON i.test_run           = m.test_run
     AND i.txn_fingerprint_id = m.txn_fingerprint_id
    WHERE m.test_run = in_test_run
      AND m.txn_id::TEXT LIKE txn_id_prefix || '%'
      -- AND i.start_time BETWEEN conflict_ts - INTERVAL '30 seconds' AND conflict_ts + INTERVAL '30 seconds'
      AND (in_app_name IS NULL OR i.app_name = in_app_name)
      AND i.query NOT LIKE 'SHOW%'
    ORDER BY i.start_time DESC
    LIMIT 1
  ),

  /* -----------------------------
     Choose the first available failed_statement exactly like v24
     ----------------------------- */
  failed_statement AS (
    SELECT
      collection_ts,
      blocking_txn_fingerprint_id,
      waiting_txn_fingerprint_id,
      database_name,
      app_name,
      schema_name,
      table_name,
      index_name,
      contention_type,
      stmt_fingerprint_id
    FROM stage1

    UNION ALL
    SELECT
      collection_ts,
      blocking_txn_fingerprint_id,
      waiting_txn_fingerprint_id,
      database_name,
      app_name,
      schema_name,
      table_name,
      index_name,
      contention_type,
      stmt_fingerprint_id
    FROM stage2
    WHERE NOT EXISTS (SELECT 1 FROM stage1)

    UNION ALL
    SELECT
      collection_ts,
      blocking_txn_fingerprint_id,
      waiting_txn_fingerprint_id,
      database_name,
      app_name,
      schema_name,
      table_name,
      index_name,
      contention_type,
      stmt_fingerprint_id
    FROM stage3
    WHERE NOT EXISTS (SELECT 1 FROM stage1)
      AND NOT EXISTS (SELECT 1 FROM stage2)
  )

  /* -----------------------------
     Main result (exactly the INSERT..SELECT logic, but returned)
     ----------------------------- */
  SELECT
    in_test_run                               AS test_run,
    tx_stmt.ord                               AS ord,
    tx.role_kind                              AS role,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN 'failed'
    END                                       AS status,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.collection_ts
    END                                       AS collection_ts,

    tx.aggregated_ts                          AS aggregated_ts,
    tx.app_name                               AS app_name,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.database_name
    END                                       AS database_name,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.schema_name
    END                                       AS schema_name,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.table_name
    END                                       AS table_name,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.index_name
    END                                       AS index_name,

    tx.metadata                               AS txn_metadata,
    tx.statistics                             AS txn_statistics,

    CASE
      WHEN tx.fingerprint_id = f.waiting_txn_fingerprint_id
       AND tx_stmt.stmt_fingerprint_id = f.stmt_fingerprint_id
      THEN f.contention_type
    END                                       AS contention_type,

    CASE
      WHEN f.table_name IS NOT NULL
       AND st.metadata ? 'querySummary'
       AND st.metadata->>'querySummary' LIKE '%'||f.table_name||'%'
      THEN true
      ELSE false
    END                                       AS contention,

    st.fingerprint_id                         AS fingerprint_id,
    st.transaction_fingerprint_id             AS transaction_fingerprint_id,
    st.plan_hash                              AS plan_hash,
    st.metadata                               AS stmt_metadata,
    st.statistics                             AS stmt_statistics,
    st.sampled_plan                           AS sampled_plan,
    st.aggregation_interval                   AS aggregation_interval,
    st.index_recommendations                  AS index_recommendations

  FROM failed_statement AS f

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

  ORDER BY tx.fingerprint_id, tx_stmt.ord;

END;
$$;
