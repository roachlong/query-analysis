-- ============================================================
-- 03 - Flagged execution insights
-- Source: crdb_internal.cluster_execution_insights
-- Purpose: CockroachDB automatically flags problem executions
--          (SlowExecution, HighContention, HighRetryCount,
--          FailedExecution, etc.).  This groups those flags so you
--          can see the biggest offenders without any tuning.
--
-- NOTE: this is an in-memory ring buffer capped by
--       sql.insights.execution_insights_capacity (default 1000),
--       so it reflects recent activity only.
-- ============================================================

-- A) Flagged statements grouped by problem + query ------------
SELECT
  problem,
  causes,
  query,
  app_name,
  count(*)                                                    AS occurrences,
  round(extract(epoch FROM sum(contention)) * 1000, 2)        AS total_contention_ms,
  sum(rows_read)                                              AS rows_read,
  max(retries)                                                AS max_retries,
  bool_or(full_scan)                                          AS any_full_scan
FROM crdb_internal.cluster_execution_insights
WHERE start_time >= now() - INTERVAL '3 hours'
  AND app_name NOT LIKE '$ internal%'
GROUP BY problem, causes, query, app_name
ORDER BY occurrences DESC, total_contention_ms DESC
LIMIT 25;

-- B) Failed executions with the retry / error reason ----------
SELECT
  start_time,
  app_name,
  query,
  status,
  error_code,
  last_retry_reason,
  retries
FROM crdb_internal.cluster_execution_insights
WHERE start_time >= now() - INTERVAL '3 hours'
  AND status = 'Failed'
  AND app_name NOT LIKE '$ internal%'
ORDER BY start_time DESC
LIMIT 25;
