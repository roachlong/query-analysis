-- ============================================================
-- 02 - Full table scans
-- Source: crdb_internal.cluster_statement_statistics
-- Purpose: surface statements that perform full scans, ranked by
--          how many rows they read in aggregate.  These are the
--          strongest candidates for a new index.
-- ============================================================

SELECT
  metadata->>'query'                                                     AS query,
  app_name,
  (statistics->'statistics'->>'cnt')::INT                                AS execs,
  round((statistics->'statistics'->'rowsRead'->>'mean')::FLOAT, 1)       AS mean_rows_read,
  round(
    (statistics->'statistics'->>'cnt')::FLOAT
    * (statistics->'statistics'->'rowsRead'->>'mean')::FLOAT, 0)         AS total_rows_read,
  round((statistics->'statistics'->'runLat'->>'mean')::FLOAT * 1000, 2)  AS mean_run_ms,
  index_recommendations
FROM crdb_internal.cluster_statement_statistics
WHERE aggregated_ts >= now() - INTERVAL '3 hours'
  AND (metadata->>'fullScan')::BOOL
  AND app_name NOT LIKE '$ internal%'
  AND coalesce(metadata->>'db','') NOT IN ('system','crdb_internal')
ORDER BY total_rows_read DESC
LIMIT 20;
