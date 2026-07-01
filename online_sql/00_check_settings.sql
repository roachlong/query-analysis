-- ============================================================
-- 00 - Environment check + data horizon
-- Purpose:
--   1. confirm the internal observability tables are populated
--   2. MEASURE how much history each source currently holds, so the
--      customer can see whether roll-off is happening in minutes,
--      hours, or days -- i.e. how urgent it is to persist this data.
--   3. show the cluster settings that govern that retention.
-- ============================================================

-- ------------------------------------------------------------
-- (1) DATA HORIZON: how far back does each source currently reach?
--
--   span = newest - oldest timestamp currently visible.
--   This is the key metric: a small span on a busy cluster means
--   the buffer is rolling off fast and older activity is already gone.
--
--   These four sources are ALL in-memory / capacity-capped (the cluster_*
--   stats are the current in-memory window, not the persisted view), so on
--   a high-velocity workload every span here can be only minutes wide --
--   that is the roll-off risk, and the case for persisting via the daemon.
--
--   Tip: if a buffer's row count is at/near its capacity setting (below),
--   it is actively evicting -> the span IS your true retention window.
-- ------------------------------------------------------------
SELECT 'cluster_statement_statistics'  AS source,
       count(*)                        AS rows,
       min(aggregated_ts)              AS oldest,
       max(aggregated_ts)              AS newest,
       max(aggregated_ts) - min(aggregated_ts) AS span
FROM crdb_internal.cluster_statement_statistics
UNION ALL
SELECT 'cluster_transaction_statistics',
       count(*), min(aggregated_ts), max(aggregated_ts),
       max(aggregated_ts) - min(aggregated_ts)
FROM crdb_internal.cluster_transaction_statistics
UNION ALL
SELECT 'cluster_execution_insights',
       count(*), min(start_time)::TIMESTAMPTZ, max(start_time)::TIMESTAMPTZ,
       max(start_time) - min(start_time)
FROM crdb_internal.cluster_execution_insights
UNION ALL
SELECT 'transaction_contention_events',
       count(*), min(collection_ts), max(collection_ts),
       max(collection_ts) - min(collection_ts)
FROM crdb_internal.transaction_contention_events;

-- ------------------------------------------------------------
-- (2) CAPACITY CAPS -- compare against the row counts above.
--     When rows ~= cap, the source is evicting and the span above
--     is the real (and shrinking) retention window.
-- ------------------------------------------------------------
SHOW CLUSTER SETTING sql.metrics.max_mem_stmt_fingerprints;      -- default 3000
SHOW CLUSTER SETTING sql.metrics.max_mem_txn_fingerprints;       -- default 3000
SHOW CLUSTER SETTING sql.insights.execution_insights_capacity;   -- default 1000 (rows)
SHOW CLUSTER SETTING sql.contention.event_store.capacity;        -- default 64 MiB (bytes)

-- ------------------------------------------------------------
-- (3) FYI: persisted-history settings.  These queries read the in-memory
--     cluster_* views (for 1:1 parity with the daemon's ingestion source).
--     The persisted crdb_internal.statement_statistics view is a DIFFERENT,
--     longer-lived source governed by the settings below -- shown here only
--     so you can see how much longer history *would* survive there.
-- ------------------------------------------------------------
SHOW CLUSTER SETTING sql.stats.flush.interval;                   -- default 10m
SHOW CLUSTER SETTING sql.stats.persisted_rows.max;               -- default 50,000 rows

-- ------------------------------------------------------------
-- Optional (non-production / benchmarking only): raise the caps so
-- more of the workload is captured while you evaluate.
-- SET CLUSTER SETTING sql.metrics.max_mem_stmt_fingerprints    = 100000;  -- default 3000
-- SET CLUSTER SETTING sql.metrics.max_mem_txn_fingerprints     = 100000;  -- default 3000
-- SET CLUSTER SETTING sql.insights.execution_insights_capacity = 20000;   -- default 1000
