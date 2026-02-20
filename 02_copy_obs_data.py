#!/usr/bin/env python3

import os
import time
import signal
import random
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Any

import psycopg
from psycopg.rows import dict_row

from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ==========================================================
# CONFIG
# ==========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
LOG_FILE = os.getenv("LOG_FILE", "/var/log/copy_obs_data.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

LOOP_INTERVAL_SECONDS = float(os.getenv("LOOP_INTERVAL_SECONDS", "15"))
JITTER_SECONDS = float(os.getenv("JITTER_SECONDS", "3"))

SLICE_SECONDS = int(os.getenv("SLICE_SECONDS", "30"))
SAFETY_DELAY_SECONDS = int(os.getenv("SAFETY_DELAY_SECONDS", "5"))

BACKFILL_ENABLED = os.getenv("BACKFILL_ENABLED", "true").lower() in ("1", "true", "yes")
BACKFILL_EVERY_N_LOOPS = int(os.getenv("BACKFILL_EVERY_N_LOOPS", "20"))
BACKFILL_WINDOW_HOURS = int(os.getenv("BACKFILL_WINDOW_HOURS", "2"))
BACKFILL_BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "250"))

LIVE_STREAMS = ("contention", "insights")
AGG_STREAMS  = ("stmt_stats", "txn_stats")
ALL_STREAMS  = LIVE_STREAMS + AGG_STREAMS

ZERO_FP_HEX = "0000000000000000"

log = logging.getLogger("copy-obs-data")
log.setLevel(LOG_LEVEL)

formatter = logging.Formatter(
    "%(asctime)s %(levelname)s [%(process)d] %(message)s"
)

# Console handler (optional but recommended)
console = logging.StreamHandler()
console.setFormatter(formatter)
log.addHandler(console)

# Rotating file handler
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=10
)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

shutdown_requested = False

# ==========================================================
# PROMETHEUS METRICS
# ==========================================================

INGEST_ROWS = Counter("obs_ingest_rows_total", "Rows inserted/updated", ["stream"])
INGEST_ERRORS = Counter("obs_ingest_errors_total", "Ingest errors", ["stream"])
INGEST_DURATION = Histogram("obs_ingest_duration_seconds", "Ingest duration", ["stream"])

ACTIVE_TEST_RUNS = Gauge("obs_active_test_runs", "Active test runs")
WATERMARK_LAG = Gauge("obs_watermark_lag_seconds", "Watermark lag", ["stream", "test_run"])

BACKFILL_UPDATED = Counter("obs_backfill_updated_total", "Placeholder updates", ["test_run"])

# ==========================================================
# SIGNAL HANDLING
# ==========================================================

def _handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    log.info("Shutdown requested...")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ==========================================================
# DB HELPERS
# ==========================================================

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(DATABASE_URL, autocommit=False)

def fetchall(conn, sql, params=None):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def fetchone(conn, sql, params=None):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount

# ==========================================================
# DATA STRUCTURES
# ==========================================================

@dataclass
class TestRun:
    test_run: str
    database_name: str
    start_time: datetime
    end_time: datetime
    agg_grace_interval: timedelta
    min_watermark: Optional[datetime]

# ==========================================================
# ACTIVE RUN SELECTION
# ==========================================================

SQL_GET_ACTIVE_RUNS = """
SELECT
  t.test_run,
  t.database_name,
  t.start_time,
  t.end_time,
  t.agg_grace_interval,
  s.min_watermark
FROM workload_test.test_run_configurations t
LEFT JOIN (
  SELECT test_run, MIN(watermark_ts) AS min_watermark
  FROM workload_test.ingest_state
  GROUP BY test_run
) s ON s.test_run = t.test_run
WHERE now() >= t.start_time
  AND (
       now() <= (t.end_time + t.agg_grace_interval)
    OR s.min_watermark IS NULL
    OR s.min_watermark < (t.end_time + t.agg_grace_interval)
  );
"""

# ==========================================================
# WATERMARK LOGIC
# ==========================================================

def get_watermark(conn, test_run, stream):
    row = fetchone(conn,
        "SELECT watermark_ts FROM workload_test.ingest_state WHERE test_run=%s AND stream=%s",
        (test_run, stream)
    )
    return row["watermark_ts"] if row else None

def set_watermark(conn, test_run, stream, ts):
    execute(conn,
        """
        UPSERT INTO workload_test.ingest_state (test_run, stream, watermark_ts, updated_at)
        VALUES (%s,%s,%s,now())
        """,
        (test_run, stream, ts)
    )

# ==========================================================
# SLICE LOGIC
# ==========================================================

def compute_live_slice(watermark, run, now_ts):
    from_ts = watermark or run.start_time
    to_ts = min(
        from_ts + timedelta(seconds=SLICE_SECONDS),
        now_ts - timedelta(seconds=SAFETY_DELAY_SECONDS),
        run.end_time
    )
    return (from_ts, to_ts) if to_ts > from_ts else (None, None)

def compute_agg_slice(watermark, run, now_ts):
    from_ts = watermark or run.start_time
    to_ts = min(
        from_ts + timedelta(seconds=SLICE_SECONDS),
        now_ts - timedelta(seconds=SAFETY_DELAY_SECONDS),
        run.end_time + run.agg_grace_interval
    )
    return (from_ts, to_ts) if to_ts > from_ts else (None, None)

# ==========================================================
# STREAM HANDLERS
# ==========================================================

def ingest_stream(conn, stream, run, from_ts, to_ts):

    if stream == "contention":
        sql = f"""
        INSERT INTO workload_test.transaction_contention_events (
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
        SELECT %s, e.*
        FROM crdb_internal.transaction_contention_events e
        WHERE e.database_name=%s
          AND e.collection_ts >= %s
          AND e.collection_ts < %s;
        """
        return execute(conn, sql, (run.test_run, run.database_name, from_ts, to_ts))

    elif stream == "insights":
        # 1) Insert into cluster_execution_insights
        sql_insights = f"""
        INSERT INTO workload_test.cluster_execution_insights (
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
            last_error_redactable,
            query_tags
        )
        SELECT %s, i.*
        FROM crdb_internal.cluster_execution_insights i
        WHERE i.database_name=%s
          AND i.start_time >= %s
          AND i.start_time < %s
          AND i.query <> 'SELECT _'
        ON CONFLICT ON CONSTRAINT uq_trei_run_txn_stmt DO NOTHING;
        """

        rows_insights = execute(
            conn,
            sql_insights,
            (run.test_run, run.database_name, from_ts, to_ts)
        )

        # 2) Insert into txn_id_map (restore v24 behavior)
        sql_txn_map = """
        INSERT INTO workload_test.txn_id_map (
            test_run,
            txn_id,
            txn_fingerprint_id
        )
        SELECT DISTINCT
            %s,
            i.txn_id,
            i.txn_fingerprint_id
        FROM crdb_internal.cluster_execution_insights i
        WHERE i.database_name = %s
        AND i.start_time >= %s
        AND i.start_time < %s
        AND i.query <> 'SELECT _'
        ON CONFLICT ON CONSTRAINT uq_txn_map_run_id DO NOTHING;
        """

        rows_map = execute(
            conn,
            sql_txn_map,
            (run.test_run, run.database_name, from_ts, to_ts)
        )

        return rows_insights + rows_map

    elif stream == "stmt_stats":
        sql = f"""
        INSERT INTO workload_test.cluster_statement_statistics (
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
        SELECT %s, s.*
        FROM crdb_internal.cluster_statement_statistics s
        WHERE s.aggregated_ts >= %s
          AND s.aggregated_ts < %s
        ON CONFLICT ON CONSTRAINT uq_stmt_stats DO UPDATE SET
          metadata = EXCLUDED.metadata,
          statistics = EXCLUDED.statistics,
          sampled_plan = EXCLUDED.sampled_plan,
          index_recommendations = EXCLUDED.index_recommendations;
        """
        return execute(conn, sql, (run.test_run, from_ts, to_ts))

    elif stream == "txn_stats":
        sql = f"""
        INSERT INTO workload_test.cluster_transaction_statistics (
            test_run,
            aggregated_ts,
            fingerprint_id,
            app_name,
            metadata,
            statistics,
            aggregation_interval
        )
        SELECT %s, x.*
        FROM crdb_internal.cluster_transaction_statistics x
        WHERE x.aggregated_ts >= %s
          AND x.aggregated_ts < %s
        ON CONFLICT ON CONSTRAINT uq_txn_stats DO UPDATE SET
          metadata = EXCLUDED.metadata,
          statistics = EXCLUDED.statistics;
        """
        return execute(conn, sql, (run.test_run, from_ts, to_ts))

    else:
        raise ValueError(f"Unknown stream {stream}")

# ==========================================================
# PLACEHOLDER BACKFILL
# ==========================================================

def _chunk(lst: List[Any], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def backfill_contention(conn, run) -> int:
    """
    Backfill placeholder fingerprint IDs in workload_test.transaction_contention_events
    using fresher data from crdb_internal.transaction_contention_events.

    IMPORTANT: CockroachDB disallows referencing crdb_internal virtual tables inside
    DML like UPDATE ... FROM crdb_internal..., so we:
      1) SELECT source rows from crdb_internal
      2) UPDATE target rows using a VALUES/CTE src table
    """
    window_hours = BACKFILL_WINDOW_HOURS
    batch_size = BACKFILL_BATCH_SIZE

    # 1) Read candidate rows from crdb_internal (SELECT is allowed)
    sql_select = f"""
    SELECT
      blocking_txn_id,
      waiting_txn_id,
      collection_ts,
      blocking_txn_fingerprint_id,
      waiting_txn_fingerprint_id
    FROM crdb_internal.transaction_contention_events
    WHERE collection_ts >= now() - interval '{window_hours} hours'
    """

    src_rows = fetchall(conn, sql_select)
    conn.commit()

    if not src_rows:
        return 0

    # Filter to rows where we actually have something to backfill (non-zero fingerprint)
    candidates: List[Tuple[Any, Any, Any, Any, Any]] = []
    for r in src_rows:
        bfp = r["blocking_txn_fingerprint_id"]
        wfp = r["waiting_txn_fingerprint_id"]

        # Skip rows where both fingerprints are still placeholders (nothing to backfill)
        if (bfp is None and wfp is None):
            continue

        # In CRDB, BYTES come back as Python bytes. Compare via hex for safety.
        bfp_hex = bfp.hex() if isinstance(bfp, (bytes, bytearray)) else None
        wfp_hex = wfp.hex() if isinstance(wfp, (bytes, bytearray)) else None

        if (bfp_hex == ZERO_FP_HEX or bfp_hex is None) and (wfp_hex == ZERO_FP_HEX or wfp_hex is None):
            continue

        candidates.append((
            r["blocking_txn_id"],
            r["waiting_txn_id"],
            r["collection_ts"],
            r["blocking_txn_fingerprint_id"],
            r["waiting_txn_fingerprint_id"],
        ))

    if not candidates:
        return 0

    updated_total = 0

    # 2) Batch UPDATE against workload_test using VALUES
    for batch in _chunk(candidates, batch_size):
        placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(batch))

        params: List[Any] = []
        for (blocking_txn_id, waiting_txn_id, collection_ts, bfp, wfp) in batch:
            params.extend([blocking_txn_id, waiting_txn_id, collection_ts, bfp, wfp])

        # test_run parameter at the end
        params.append(run.test_run)

        sql_update = f"""
        WITH src(
          blocking_txn_id,
          waiting_txn_id,
          collection_ts,
          blocking_txn_fingerprint_id,
          waiting_txn_fingerprint_id
        ) AS (
          VALUES {placeholders}
        )
        UPDATE workload_test.transaction_contention_events AS tgt
        SET
          blocking_txn_fingerprint_id = CASE
              WHEN tgt.blocking_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
               AND src.blocking_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES
              THEN src.blocking_txn_fingerprint_id
              ELSE tgt.blocking_txn_fingerprint_id
          END,
          waiting_txn_fingerprint_id = CASE
              WHEN tgt.waiting_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
               AND src.waiting_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES
              THEN src.waiting_txn_fingerprint_id
              ELSE tgt.waiting_txn_fingerprint_id
          END
        FROM src
        WHERE tgt.test_run = %s
          AND tgt.collection_ts = src.collection_ts
          AND tgt.blocking_txn_id = src.blocking_txn_id
          AND tgt.waiting_txn_id  = src.waiting_txn_id
          AND tgt.collection_ts >= now() - interval '{window_hours} hours'
          AND (
            (tgt.blocking_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
             AND src.blocking_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES)
            OR
            (tgt.waiting_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
             AND src.waiting_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES)
          );
        """

        updated = execute(conn, sql_update, tuple(params))
        if updated and updated > 0:
            updated_total += updated

    return updated_total

def backfill_contention(conn, run):
    window = f"{BACKFILL_WINDOW_HOURS} hours"

    sql = f"""
    UPDATE workload_test.transaction_contention_events tgt
    SET
      blocking_txn_fingerprint_id = src.blocking_txn_fingerprint_id,
      waiting_txn_fingerprint_id  = src.waiting_txn_fingerprint_id
    FROM crdb_internal.transaction_contention_events src
    WHERE tgt.test_run = %s
      AND tgt.collection_ts >= now() - interval '{BACKFILL_WINDOW_HOURS} hours'
      AND tgt.collection_ts = src.collection_ts
      AND tgt.blocking_txn_id = src.blocking_txn_id
      AND tgt.waiting_txn_id = src.waiting_txn_id
      AND (
          (tgt.blocking_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
           AND src.blocking_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES)
       OR (tgt.waiting_txn_fingerprint_id = '\\x{ZERO_FP_HEX}'::BYTES
           AND src.waiting_txn_fingerprint_id != '\\x{ZERO_FP_HEX}'::BYTES)
      );
    """
    return execute(conn, sql, (run.test_run,))

# ==========================================================
# MAIN LOOP
# ==========================================================

def daemon_loop():

    start_http_server(METRICS_PORT)
    log.info("Metrics exposed on :%s", METRICS_PORT)

    backoff = 5
    loop_count = 0

    while not shutdown_requested:

        loop_count += 1
        now_ts = datetime.now(timezone.utc)

        try:
            with get_connection() as conn:
                rows = fetchall(conn, SQL_GET_ACTIVE_RUNS)
                ACTIVE_TEST_RUNS.set(len(rows))

            for r in rows:
                run = TestRun(**r)

                for stream in ALL_STREAMS:

                    with get_connection() as conn:
                        watermark = get_watermark(conn, run.test_run, stream)

                    if stream in LIVE_STREAMS:
                        from_ts, to_ts = compute_live_slice(watermark, run, now_ts)
                    else:
                        from_ts, to_ts = compute_agg_slice(watermark, run, now_ts)

                    if not from_ts:
                        continue

                    start = time.time()
                    try:
                        with get_connection() as tx:
                            rows = ingest_stream(tx, stream, run, from_ts, to_ts)
                            set_watermark(tx, run.test_run, stream, to_ts)
                            tx.commit()

                        INGEST_ROWS.labels(stream=stream).inc(rows)
                        INGEST_DURATION.labels(stream=stream).observe(time.time() - start)
                        WATERMARK_LAG.labels(stream=stream, test_run=run.test_run).set(
                            (now_ts - to_ts).total_seconds()
                        )

                    except Exception:
                        INGEST_ERRORS.labels(stream=stream).inc()
                        log.exception("Stream failed: %s", stream)

                if BACKFILL_ENABLED and loop_count % BACKFILL_EVERY_N_LOOPS == 0:
                    try:
                        with get_connection() as tx:
                            updated = backfill_contention(tx, run)
                            tx.commit()
                        BACKFILL_UPDATED.labels(test_run=run.test_run).inc(updated)
                    except Exception:
                        log.exception("Backfill failed")

            backoff = 5
            time.sleep(LOOP_INTERVAL_SECONDS + random.uniform(0, JITTER_SECONDS))

        except Exception:
            log.exception("Top-level failure")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    log.info("Daemon exiting cleanly.")

if __name__ == "__main__":
    daemon_loop()
