-- ============================================================
-- 01 - Slow statements
-- Source: crdb_internal.cluster_statement_statistics
-- Purpose: rank statement fingerprints by latency to find the
--          slowest performers relative to your workload.
--
-- Two rankings are provided:
--   A) by MEAN latency        -> "what is slow per execution"
--   B) by TOTAL time consumed -> "what is costing the cluster most"
--      (execution_count * mean latency)
--
-- Adjust the lookback window and the app_name / db filters as needed.
-- ============================================================

-- A) Slowest by mean run latency ------------------------------
SELECT
  metadata->>'query'                                                        AS query,
  app_name,
  (statistics->'statistics'->>'cnt')::INT                                   AS execs,
  round((statistics->'statistics'->'runLat'->>'mean')::FLOAT  * 1000, 2)    AS mean_run_ms,
  round((statistics->'statistics'->'svcLat'->>'mean')::FLOAT  * 1000, 2)    AS mean_svc_ms,
  round((statistics->'execution_statistics'->'contentionTime'->>'mean')::FLOAT * 1000, 2) AS mean_contention_ms,
  round((statistics->'statistics'->'rowsRead'->>'mean')::FLOAT, 1)          AS mean_rows_read,
  (metadata->>'fullScan')::BOOL                                             AS full_scan
FROM crdb_internal.cluster_statement_statistics
WHERE aggregated_ts >= now() - INTERVAL '3 hours'
  AND app_name NOT LIKE '$ internal%'
  AND coalesce(metadata->>'db','') NOT IN ('system','crdb_internal')
ORDER BY mean_run_ms DESC
LIMIT 20;

-- B) Highest total time consumed (aggregate impact) -----------
SELECT
  metadata->>'query'                                                        AS query,
  app_name,
  (statistics->'statistics'->>'cnt')::INT                                   AS execs,
  round((statistics->'statistics'->'svcLat'->>'mean')::FLOAT * 1000, 2)     AS mean_svc_ms,
  round(
    (statistics->'statistics'->>'cnt')::FLOAT
    * (statistics->'statistics'->'svcLat'->>'mean')::FLOAT, 2)              AS total_svc_sec
FROM crdb_internal.cluster_statement_statistics
WHERE aggregated_ts >= now() - INTERVAL '3 hours'
  AND app_name NOT LIKE '$ internal%'
  AND coalesce(metadata->>'db','') NOT IN ('system','crdb_internal')
ORDER BY total_svc_sec DESC
LIMIT 20;
