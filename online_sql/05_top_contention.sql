-- ============================================================
-- 05 - Top contributors to contention
-- Source: crdb_internal.transaction_contention_events
--         (+ crdb_internal.cluster_statement_statistics for query text)
-- Purpose: find where lock contention is concentrated -- which
--          tables/indexes, which waiting statements, and which
--          blocking transactions.
--
-- NOTE: transaction_contention_events is an in-memory store of
--       recent events, so this reflects recent activity only.
-- ============================================================

-- A) Contention hot objects (table / index) -------------------
SELECT
  database_name,
  schema_name,
  table_name,
  coalesce(index_name, '(primary or n/a)')                       AS index_name,
  contention_type,
  count(*)                                                       AS events,
  round(extract(epoch FROM sum(contention_duration)) * 1000, 2)  AS total_contention_ms,
  round(extract(epoch FROM max(contention_duration)) * 1000, 2)  AS max_contention_ms
FROM crdb_internal.transaction_contention_events
WHERE collection_ts >= now() - INTERVAL '3 hours'
GROUP BY database_name, schema_name, table_name, index_name, contention_type
ORDER BY total_contention_ms DESC
LIMIT 20;

-- B) Statements that spend the most time WAITING on locks ------
SELECT
  encode(e.waiting_stmt_fingerprint_id, 'hex')                     AS waiting_stmt_fp,
  count(*)                                                         AS wait_events,
  round(extract(epoch FROM sum(e.contention_duration)) * 1000, 2)  AS total_wait_ms,
  min(ss.metadata->>'query')                                       AS sample_query
FROM crdb_internal.transaction_contention_events e
LEFT JOIN crdb_internal.cluster_statement_statistics ss
  ON ss.fingerprint_id = e.waiting_stmt_fingerprint_id
WHERE e.collection_ts >= now() - INTERVAL '3 hours'
GROUP BY e.waiting_stmt_fingerprint_id
ORDER BY total_wait_ms DESC
LIMIT 20;

-- C) Transactions that most often BLOCK others ----------------
--    (drill into these fingerprints with query 06 or with the
--     inspect_contention_from_exception function.)
SELECT
  encode(e.blocking_txn_fingerprint_id, 'hex')                     AS blocking_txn_fp,
  count(*)                                                         AS times_blocking,
  round(extract(epoch FROM sum(e.contention_duration)) * 1000, 2)  AS total_blocking_ms,
  count(DISTINCT e.waiting_txn_fingerprint_id)                     AS distinct_victims
FROM crdb_internal.transaction_contention_events e
WHERE e.collection_ts >= now() - INTERVAL '3 hours'
GROUP BY e.blocking_txn_fingerprint_id
ORDER BY total_blocking_ms DESC
LIMIT 20;
