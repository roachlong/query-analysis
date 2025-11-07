#!/usr/bin/env python3
import os, json
from typing import Iterable, Tuple
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

def get_conn():
    dsn = os.getenv('CRDB_DSN')
    if not dsn:
        host = os.getenv('CRDB_HOST','localhost')
        port = os.getenv('CRDB_PORT','26257')
        user = os.getenv('CRDB_USER','root')
        pwd  = os.getenv('CRDB_PASSWORD','')
        db   = os.getenv('CRDB_DB','defaultdb')
        ssl  = os.getenv('CRDB_SSLMODE','disable') if not pwd else os.getenv('CRDB_SSLMODE','require')
        dsn = f'host={host} port={port} user={user} dbname={db} sslmode={ssl} password={pwd}'

    conn = psycopg.connect(dsn, autocommit=True)

    # enable TEMP tables for this session (v24.1)
    with conn.cursor() as cur:
        cur.execute("SET experimental_enable_temp_tables = 'on'")
        cur.execute("SET application_name = 'copy_obs_data'")
    return conn

def copy_rows(cur, copy_sql: str, rows: Iterable[Tuple]) -> int:
    sent = 0
    with cur.copy(copy_sql) as cp:
        for r in rows:
            cp.write_row(r)
            sent += 1
    return sent

# SQL used by the script
SQL_GET_CONFIGS = '''
SELECT test_run, database_name, start_time, end_time, last_copy_time, last_agg_copy_time
FROM workload_test.test_run_configurations
WHERE now() >= start_time
  AND (last_copy_time IS NULL OR last_copy_time < end_time OR last_agg_copy_time IS NULL OR last_agg_copy_time < end_time);
'''

SQL_READ_TCE_AOST = '''
SELECT e.collection_ts, e.blocking_txn_id, e.blocking_txn_fingerprint_id, e.waiting_txn_id, e.waiting_txn_fingerprint_id,
       e.contention_duration, e.contending_key, e.contending_pretty_key, e.waiting_stmt_id, e.waiting_stmt_fingerprint_id,
       e.database_name, e.schema_name, e.table_name, e.index_name, e.contention_type
FROM crdb_internal.transaction_contention_events AS e
AS OF SYSTEM TIME follower_read_timestamp()
WHERE e.database_name = %s AND e.collection_ts >= %s AND e.collection_ts < %s;
'''

SQL_READ_CEI_AOST = '''
SELECT i.session_id, i.txn_id, i.txn_fingerprint_id, i.stmt_id, i.stmt_fingerprint_id, i.problem, i.causes, i.query, i.status,
       i.start_time, i.end_time, i.full_scan, i.user_name, i.app_name, i.database_name, i.plan_gist, i.rows_read, i.rows_written,
       i.priority, i.retries, i.last_retry_reason, i.exec_node_ids, i.contention, i.index_recommendations, i.implicit_txn,
       i.cpu_sql_nanos, i.error_code, i.last_error_redactable
FROM crdb_internal.cluster_execution_insights AS i
AS OF SYSTEM TIME follower_read_timestamp()
WHERE i.database_name = %s AND i.start_time >= %s AND i.start_time < %s AND i.query <> 'SELECT _';
'''

SQL_READ_STMT_STATS_AOST = '''
SELECT s.aggregated_ts, s.fingerprint_id, s.transaction_fingerprint_id, s.plan_hash,
       s.app_name, s.metadata, s.statistics, s.sampled_plan, s.aggregation_interval, s.index_recommendations
FROM crdb_internal.statement_statistics AS s
AS OF SYSTEM TIME follower_read_timestamp()
WHERE s.aggregated_ts >= %s AND s.aggregated_ts < %s;
'''

SQL_READ_TXN_STATS_AOST = '''
SELECT x.aggregated_ts, x.fingerprint_id, x.app_name, x.metadata, x.statistics, x.aggregation_interval
FROM crdb_internal.transaction_statistics AS x
AS OF SYSTEM TIME follower_read_timestamp()
WHERE x.aggregated_ts >= %s AND x.aggregated_ts < %s;
'''

SQL_READ_TCE2_AOST = '''
SELECT blocking_txn_id, waiting_txn_id, collection_ts, blocking_txn_fingerprint_id, waiting_txn_fingerprint_id
FROM crdb_internal.transaction_contention_events
AS OF SYSTEM TIME follower_read_timestamp()
WHERE collection_ts >= now() - interval '2 hours';
'''

SQL_GET_TO_TS = "SELECT LEAST(now() - interval '10 seconds', %s + interval '10 minutes')"
SQL_GET_MAX_AGG_INT = '''
SELECT max(aggregation_interval) * (1 + 10/60.0)
FROM crdb_internal.transaction_statistics
AS OF SYSTEM TIME follower_read_timestamp();
'''

SQL_DT_HOUR = "SELECT date_trunc('hour', %s)"
SQL_BEYOND_AGG = "SELECT (now() - %s) >= %s"

COUNT_TCE  = """SELECT COUNT(*) FROM workload_test.transaction_contention_events WHERE test_run=%s AND collection_ts >= %s AND collection_ts < %s"""
COUNT_CEI  = """SELECT COUNT(*) FROM workload_test.cluster_execution_insights WHERE test_run=%s AND start_time   >= %s AND start_time   < %s"""
COUNT_STMT = """SELECT COUNT(*) FROM workload_test.statement_statistics WHERE test_run=%s AND aggregated_ts>= %s AND aggregated_ts< %s"""
COUNT_TXN  = """SELECT COUNT(*) FROM workload_test.transaction_statistics WHERE test_run=%s AND aggregated_ts>= %s AND aggregated_ts< %s"""

DDL_TMP_TCE = '''
CREATE TEMP TABLE IF NOT EXISTS tmp_tce (
  test_run                    STRING,
  collection_ts               TIMESTAMPTZ,
  blocking_txn_id             UUID,
  blocking_txn_fingerprint_id BYTES,
  waiting_txn_id              UUID,
  waiting_txn_fingerprint_id  BYTES,
  contention_duration         INTERVAL,
  contending_key              BYTES,
  contending_pretty_key       STRING,
  waiting_stmt_id             STRING,
  waiting_stmt_fingerprint_id BYTES,
  database_name               STRING,
  schema_name                 STRING,
  table_name                  STRING,
  index_name                  STRING,
  contention_type             STRING
);
'''

DDL_TMP_CEI = '''
CREATE TEMP TABLE IF NOT EXISTS tmp_cei (
  test_run               STRING,
  session_id             STRING,
  txn_id                 UUID,
  txn_fingerprint_id     BYTES,
  stmt_id                STRING,
  stmt_fingerprint_id    BYTES,
  problem                STRING,
  causes                 STRING[],
  query                  STRING,
  status                 STRING,
  start_time             TIMESTAMPTZ,
  end_time               TIMESTAMPTZ,
  full_scan              BOOL,
  user_name              STRING,
  app_name               STRING,
  database_name          STRING,
  plan_gist              STRING,
  rows_read              INT8,
  rows_written           INT8,
  priority               STRING,
  retries                INT8,
  last_retry_reason      STRING,
  exec_node_ids          INT8[],
  contention             INTERVAL,
  index_recommendations  STRING[],
  implicit_txn           BOOL,
  cpu_sql_nanos          INT8,
  error_code             STRING,
  last_error_redactable  STRING
);
'''

DDL_TMP_TXN_MAP = '''
CREATE TEMP TABLE IF NOT EXISTS tmp_txn_map (
  test_run             STRING,
  txn_id               UUID,
  txn_fingerprint_id   BYTES
);
'''

DDL_TMP_TCE2 = '''
CREATE TEMP TABLE IF NOT EXISTS tmp_tce2_src (
  blocking_txn_id             UUID,
  waiting_txn_id              UUID,
  collection_ts               TIMESTAMPTZ,
  blocking_txn_fingerprint_id BYTES,
  waiting_txn_fingerprint_id  BYTES
);
'''

COPY_TMP_TCE    = "COPY tmp_tce FROM STDIN"
COPY_TMP_CEI    = "COPY tmp_cei FROM STDIN"
COPY_TMP_TXNMAP = "COPY tmp_txn_map FROM STDIN"
COPY_TMP_TCE2   = "COPY tmp_tce2_src FROM STDIN"

MERGE_TCE = '''
INSERT INTO workload_test.transaction_contention_events (
  test_run, collection_ts, blocking_txn_id, blocking_txn_fingerprint_id,
  waiting_txn_id, waiting_txn_fingerprint_id, contention_duration,
  contending_key, contending_pretty_key, waiting_stmt_id,
  waiting_stmt_fingerprint_id, database_name, schema_name, table_name,
  index_name, contention_type
)
SELECT
  test_run, collection_ts, blocking_txn_id, blocking_txn_fingerprint_id,
  waiting_txn_id, waiting_txn_fingerprint_id, contention_duration,
  contending_key, contending_pretty_key, waiting_stmt_id,
  waiting_stmt_fingerprint_id, database_name, schema_name, table_name,
  index_name, contention_type
FROM tmp_tce
ON CONFLICT DO NOTHING;
'''

MERGE_CEI = '''
INSERT INTO workload_test.cluster_execution_insights (
  test_run, session_id, txn_id, txn_fingerprint_id, stmt_id, stmt_fingerprint_id,
  problem, causes, query, status, start_time, end_time, full_scan, user_name,
  app_name, database_name, plan_gist, rows_read, rows_written, priority, retries,
  last_retry_reason, exec_node_ids, contention, index_recommendations, implicit_txn,
  cpu_sql_nanos, error_code, last_error_redactable
)
SELECT
  test_run, session_id, txn_id, txn_fingerprint_id, stmt_id, stmt_fingerprint_id,
  problem, causes, query, status, start_time, end_time, full_scan, user_name,
  app_name, database_name, plan_gist, rows_read, rows_written, priority, retries,
  last_retry_reason, exec_node_ids, contention, index_recommendations, implicit_txn,
  cpu_sql_nanos, error_code, last_error_redactable
FROM tmp_cei
ON CONFLICT ON CONSTRAINT uq_trei_run_txn_stmt DO NOTHING;
'''

MERGE_TXN_MAP = '''
INSERT INTO workload_test.txn_id_map (test_run, txn_id, txn_fingerprint_id)
SELECT test_run, txn_id, txn_fingerprint_id
FROM tmp_txn_map
ON CONFLICT ON CONSTRAINT uq_txn_map_run_id DO NOTHING;
'''

UPDATE_PLACEHOLDERS = '''
UPDATE workload_test.transaction_contention_events AS tgt
SET blocking_txn_fingerprint_id = src.blocking_txn_fingerprint_id,
    waiting_txn_fingerprint_id  = src.waiting_txn_fingerprint_id
FROM tmp_tce2_src AS src
WHERE (
  (tgt.blocking_txn_fingerprint_id = '\\x0000000000000000'::BYTES AND src.blocking_txn_fingerprint_id != '\\x0000000000000000'::BYTES) OR
  (tgt.waiting_txn_fingerprint_id  = '\\x0000000000000000'::BYTES AND src.waiting_txn_fingerprint_id  != '\\x0000000000000000'::BYTES)
)
AND tgt.blocking_txn_id = src.blocking_txn_id
AND tgt.waiting_txn_id  = src.waiting_txn_id
AND tgt.collection_ts   = src.collection_ts
AND tgt.test_run        = %s
AND tgt.collection_ts  >= now() - interval '2 hours';
'''

UPSERT_STMT_STATS_TMPL = '''
INSERT INTO workload_test.statement_statistics (
    test_run, aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash,
    app_name, metadata, statistics, sampled_plan, aggregation_interval, index_recommendations
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
ON CONFLICT (test_run, aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name)
DO UPDATE SET
    metadata = EXCLUDED.metadata,
    statistics = EXCLUDED.statistics,
    sampled_plan = EXCLUDED.sampled_plan,
    index_recommendations = EXCLUDED.index_recommendations;
'''

UPSERT_TXN_STATS_TMPL = '''
INSERT INTO workload_test.transaction_statistics (
    test_run, aggregated_ts, fingerprint_id, app_name, metadata, statistics, aggregation_interval
) VALUES (
    %s,%s,%s,%s,%s,%s,%s
)
ON CONFLICT (test_run, aggregated_ts, fingerprint_id, app_name)
DO UPDATE SET
    metadata = EXCLUDED.metadata,
    statistics = EXCLUDED.statistics;
'''

UPDATE_LAST_COPY = "UPDATE workload_test.test_run_configurations SET last_copy_time = %s WHERE test_run = %s"
UPDATE_LAST_AGG_COPY = "UPDATE workload_test.test_run_configurations SET last_agg_copy_time = %s WHERE test_run = %s"

def chunked(it, n):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def main():
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL_GET_CONFIGS)
            cfgs = cur.fetchall()
    if not cfgs:
        print('[]')
        return

    results = []
    for cfg in cfgs:
        test_run = cfg['test_run']
        test_db  = cfg['database_name']
        start_ts = cfg['start_time']
        end_ts   = cfg['end_time']
        last_ts  = cfg['last_copy_time']
        last_agg = cfg['last_agg_copy_time']

        # compute window
        from_ts = last_ts or start_ts
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_GET_TO_TS, (end_ts,))
                to_ts = cur.fetchone()[0]

        cnt_cont = cnt_ins = cnt_stmt = cnt_txn = 0

        if from_ts < to_ts:
            # AOST reads
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SQL_READ_TCE_AOST, (test_db, from_ts, to_ts))
                    tce_rows = cur.fetchall()
                with conn.cursor() as cur:
                    cur.execute(SQL_READ_CEI_AOST, (test_db, from_ts, to_ts))
                    cei_rows = cur.fetchall()

            # staging + merge in one write txn
            with get_conn() as conn:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(DDL_TMP_TCE)
                        cur.execute(DDL_TMP_CEI)
                        cur.execute(DDL_TMP_TXN_MAP)
                        copy_rows(cur, COPY_TMP_TCE,    ((test_run, *row) for row in tce_rows))
                        copy_rows(cur, COPY_TMP_CEI,    ((test_run, *row) for row in cei_rows))
                        copy_rows(cur, COPY_TMP_TXNMAP, ((test_run, row[1], row[2]) for row in cei_rows))
                        cur.execute(MERGE_TCE)
                        cur.execute(MERGE_CEI)
                        cur.execute(MERGE_TXN_MAP)
                        cur.execute(UPDATE_LAST_COPY, (to_ts, test_run))

            # counts
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(COUNT_TCE, (test_run, from_ts, to_ts))
                    cnt_cont = cur.fetchone()[0]
                    cur.execute(COUNT_CEI, (test_run, from_ts, to_ts))
                    cnt_ins  = cur.fetchone()[0]

        # Aggregated stats
        from_agg = last_agg or start_ts
        if from_agg < to_ts:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SQL_GET_MAX_AGG_INT)
                    agg_interval = cur.fetchone()[0]
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SQL_DT_HOUR, (from_agg,))
                    from_agg_aligned = cur.fetchone()[0]
                    cur.execute(SQL_DT_HOUR, (to_ts,))
                    to_ts_hour = cur.fetchone()[0]
            proceed = False
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SQL_BEYOND_AGG, (from_agg_aligned, agg_interval))
                    proceed = cur.fetchone()[0]
            if proceed:
                to_agg = to_ts_hour if from_agg_aligned < to_ts_hour else to_ts
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(SQL_READ_STMT_STATS_AOST, (from_agg_aligned, to_agg))
                        stmt_stats = cur.fetchall()
                    with conn.cursor() as cur:
                        cur.execute(SQL_READ_TXN_STATS_AOST, (from_agg_aligned, to_agg))
                        txn_stats = cur.fetchall()
                    with conn.cursor() as cur:
                        cur.execute(SQL_READ_TCE2_AOST)
                        tce2_rows = cur.fetchall()
                with get_conn() as conn:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            BATCH = 1000
                            if stmt_stats:
                                for chunk in chunked(stmt_stats, BATCH):
                                    params = []
                                    for r in chunk:
                                        # r fields by order:
                                        # 0: aggregated_ts
                                        # 1: fingerprint_id
                                        # 2: transaction_fingerprint_id
                                        # 3: plan_hash
                                        # 4: app_name
                                        # 5: metadata (dict)
                                        # 6: statistics (dict)
                                        # 7: sampled_plan (often dict/JSON-ish)
                                        # 8: aggregation_interval
                                        # 9: index_recommendations (array; may contain dicts)

                                        # normalize index_recommendations to list[str]
                                        ir = r[9]
                                        if ir is None:
                                            ir_norm = None
                                        elif isinstance(ir, (list, tuple)):
                                            ir_norm = [
                                                (json.dumps(x, separators=(',', ':')) if isinstance(x, (dict, list)) else (x if isinstance(x, str) else str(x)))
                                                for x in ir
                                            ]
                                        else:
                                            # unexpected type: just stringify it
                                            ir_norm = [str(ir)]

                                        params.append((
                                            test_run,
                                            r[0],                 # aggregated_ts
                                            r[1],                 # fingerprint_id
                                            r[2],                 # transaction_fingerprint_id
                                            r[3],                 # plan_hash
                                            r[4],                 # app_name
                                            Json(r[5]),           # metadata JSONB
                                            Json(r[6]),           # statistics JSONB
                                            Json(r[7]) if r[7] is not None else None,  # sampled_plan as JSONB
                                            r[8],                 # aggregation_interval (interval/timedelta)
                                            ir_norm,              # STRING[] (list[str])
                                        ))
                                    cur.executemany(UPSERT_STMT_STATS_TMPL, params)
                            if txn_stats:
                                for chunk in chunked(txn_stats, BATCH):
                                    params = [
                                        (
                                            test_run,
                                            r[0],        # aggregated_ts
                                            r[1],        # fingerprint_id
                                            r[2],        # app_name
                                            Json(r[3]),  # metadata JSONB
                                            Json(r[4]),  # statistics JSONB
                                            r[5],        # aggregation_interval
                                        )
                                        for r in chunk
                                    ]
                                    cur.executemany(UPSERT_TXN_STATS_TMPL, params)

                            cur.execute(UPDATE_LAST_AGG_COPY, (to_agg, test_run))

                            # placeholder backfill (uses temp table)
                            cur.execute(DDL_TMP_TCE2)
                            copy_rows(cur, COPY_TMP_TCE2, tce2_rows)
                            cur.execute(UPDATE_PLACEHOLDERS, (test_run,))
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(COUNT_STMT, (test_run, from_agg_aligned, to_agg))
                        cnt_stmt = cur.fetchone()[0]
                        cur.execute(COUNT_TXN, (test_run, from_agg_aligned, to_agg))
                        cnt_txn = cur.fetchone()[0]

        results.append({
            "test_run":     test_run,
            "contention":   int(cnt_cont),
            "insights":     int(cnt_ins),
            "statements":   int(cnt_stmt),
            "transactions": int(cnt_txn)
        })

    print(json.dumps(results, default=str))

if __name__ == '__main__':
    main()
