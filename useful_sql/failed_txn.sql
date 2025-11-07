WITH stmt AS (
  SELECT
    app_name,
    transaction_fingerprint_id,
    (statistics->'statistics'->>'cnt')::INT                        AS execs,
    COALESCE((statistics->'statistics'->>'failureCount')::INT, 0)  AS failure_count
  FROM crdb_internal.statement_statistics
  WHERE aggregated_ts >= now() - interval '3 hours'
    AND app_name NOT LIKE '$ internal%'                                  -- drop internal jobs
    AND coalesce(metadata->>'db','') NOT IN ('system','crdb_internal')   -- drop system DB noise
--    AND transaction_fingerprint_id = decode('06D086C8A4456503', 'hex') -- testing one fingerprint
),
-- rollup the statement executions and failures under each transaction fingerprint
tx_stmt_rollup AS (
  SELECT
    app_name,
    transaction_fingerprint_id,
    SUM(execs)         AS stmt_execs,
    SUM(failure_count) AS failed_stmt_execs
  FROM stmt
  GROUP BY app_name, transaction_fingerprint_id
)
SELECT
  t.fingerprint_id AS txn_fingerprint_id,
  jsonb_array_length(t.metadata->'stmtFingerprintIDs')::INT AS num_stmt,
--  t.metadata->'stmtFingerprintIDs' AS stmts, -- to see the stmt prefix for failed txn
  t.app_name,
  (t.statistics->'statistics'->>'cnt')::INT AS tx_execs,
  x.stmt_execs,
  x.failed_stmt_execs,
  CASE WHEN x.failed_stmt_execs = 0 THEN 'NEVER'
       WHEN (x.failed_stmt_execs::DECIMAL / (t.statistics->'statistics'->>'cnt')::INT::DECIMAL) < 1 THEN 'SOMETIMES'
       ELSE 'ALWAYS'
  END AS failure_rate,
  (x.failed_stmt_execs::DECIMAL / (t.statistics->'statistics'->>'cnt')::INT::DECIMAL) AS failed_txn_ratio
FROM crdb_internal.transaction_statistics AS t
JOIN tx_stmt_rollup AS x
  ON t.app_name = x.app_name
 AND t.fingerprint_id = x.transaction_fingerprint_id
WHERE t.aggregated_ts >= now() - interval '3 hours'
  AND t.app_name NOT LIKE '$ internal%'
--  AND jsonb_array_length(t.metadata->'stmtFingerprintIDs')::INT > 1 -- to filter out single statement txn
--  AND x.failed_stmt_execs > (x.stmt_execs / jsonb_array_length(t.metadata->'stmtFingerprintIDs')::INT) -- where failed txn ratio > 1 due to retries
ORDER BY tx_execs DESC;
