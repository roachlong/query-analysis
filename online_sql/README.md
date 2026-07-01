# Evaluating CockroachDB Performance Directly From Internal Tables

This guide is a self-contained set of SQL queries a customer can run **live against a
CockroachDB cluster** to surface performance problems, index recommendations, and the
top contributors to contention — **without deploying the observability daemon or
creating any persistent tables**.

It is the "offline-free" companion to the [query-analysis](../README.md) tool. The
full tool copies the same `crdb_internal` sources into persistent `workload_test.*`
tables so metrics survive node restarts and in-memory roll-off, and can be compared
across test runs. The queries here read the identical source tables directly, so they
are perfect for a quick, zero-footprint evaluation of the signal quality before
committing to the daemon.

---

## What you get, and the trade-off

| Approach | Footprint | History |
|---|---|---|
| **These live queries** | None. Read-only against `crdb_internal`. | Only what is currently in memory / the persisted retention window. Rolls off. |
| **The full daemon** ([../README.md](../README.md)) | A schema of `workload_test.*` tables + a Python daemon. | Retained per-test-run for 90 days (TTL), comparable across runs. |

The daemon exists precisely because the internal tables below are **in-memory / capped**
and roll off over time. Running these queries live is the best way to demonstrate the
*value* of that data before deciding to persist it.

---

## Source tables (live) → offline equivalents

Every query here reads one of these `crdb_internal` tables. The right-hand column shows
the persistent table the daemon would copy it into.

| Live `crdb_internal` table | What it holds | Daemon offline table |
|---|---|---|
| `cluster_statement_statistics` | Per–statement-fingerprint stats (latency, rows, retries, index recs). Current in-memory, fanned out across nodes. | `cluster_statement_statistics` |
| `cluster_transaction_statistics` | Per–transaction-fingerprint stats. Current in-memory. | `cluster_transaction_statistics` |
| `cluster_execution_insights` | Auto-flagged problem executions (slow, high-contention, failed, retried). In-memory, capacity-capped. | `cluster_execution_insights` |
| `transaction_contention_events` | Blocking/waiting transaction pairs with the contended key/index. In-memory, capacity-capped. | `transaction_contention_events` |
| `tables`, `table_indexes`, `index_usage_statistics` | Schema + index usage, used to resolve/verify index recommendations. | (read live) |
| `table_row_statistics` | Estimated row counts per table. | (read live) |

> **Why `cluster_*` and not `statement_statistics`?** These queries read the exact same
> `crdb_internal` sources the daemon ingests from, so each query is portable **by swapping
> only the schema prefix** — `crdb_internal.cluster_statement_statistics` (live) ↔
> `workload_test.cluster_statement_statistics` (persisted). Run the query online during the
> eval, then run it verbatim against the persisted tables once the daemon is in place, and
> you're comparing apples to apples.
>
> The trade-off: `cluster_*` is **current in-memory only** — it has no flushed history, so a
> lookback window (`now() - INTERVAL '3 hours'`) returns only what is still in the live
> buffer, which on a busy cluster may be far less than the full window. That limitation is
> exactly what the daemon removes by persisting the data. (CockroachDB also exposes
> `crdb_internal.statement_statistics`, a combined view over persisted + in-memory data —
> the same column/JSON shape — if you later want fuller history without the daemon.
> `cluster_execution_insights` is already the correct cluster-wide table for insights.)

---

## How to run

All queries are plain SQL. Run any file with the `cockroach` CLI:

```bash
conn_str="postgresql://root@<host>:26257/<db>?sslmode=verify-full&sslrootcert=<ca.crt>"

cockroach sql --url "$conn_str" -f online_sql/00_check_settings.sql
cockroach sql --url "$conn_str" -f online_sql/01_slow_statements.sql
# ...etc
```

Or paste any single query into the **DB Console → SQL Activity** / a SQL shell.

**Before you start**, run `00_check_settings.sql` to confirm the internal tables are
populated and to see the retention caps. Every query defaults to a **3-hour lookback**
and filters out internal (`$ internal%`) and `system`/`crdb_internal` traffic — adjust
the `INTERVAL` and `app_name`/`db` filters to match the workload you are evaluating.

---

## Query catalog

### 00 · Environment check + data horizon — [`00_check_settings.sql`](00_check_settings.sql)
Three things: **(1)** measures the **data horizon** — the oldest/newest timestamp and the
`span` currently held in each source, so you can see whether roll-off is happening in
minutes, hours, or days; **(2)** shows the **capacity caps** to compare against those row
counts (when rows ≈ cap, the buffer is evicting and the span *is* your real retention
window); **(3)** shows the **persisted-history** settings (`sql.stats.flush.interval`,
`sql.stats.persisted_rows.max`). This is the query to lead with when making the case for
persistence: **a small span on a busy cluster is the urgency argument for the daemon.**
Includes commented `SET` statements to raise the caps for a benchmark evaluation.

### 01 · Slow statements — [`01_slow_statements.sql`](01_slow_statements.sql)
Ranks statement fingerprints two ways: **(A) by mean run latency** ("what is slow per
execution") and **(B) by total service time** = `execs × mean latency` ("what costs the
cluster the most"). Shows execs, mean run/service/contention time, mean rows read, and
whether it does a full scan. *This is the query from the main README's "Slow Performers"
section, pointed at live data.*

### 02 · Full table scans — [`02_full_scans.sql`](02_full_scans.sql)
Every statement flagged `fullScan = true`, ranked by total rows read (`execs × mean
rows read`). These are the strongest candidates for a new index. Includes the engine's
own `index_recommendations` for each.

### 03 · Flagged execution insights — [`03_execution_insights.sql`](03_execution_insights.sql)
CockroachDB auto-flags problem executions (SlowExecution, HighContention, HighRetryCount,
FailedExecution). **(A)** groups flags by problem + query to show the biggest offenders;
**(B)** lists individual failed executions with `error_code` and `last_retry_reason`.
Requires no tuning — the engine decided these were problems.

### 04 · Index recommendations (scored) — [`04_index_recommendations.sql`](04_index_recommendations.sql)
The advanced index-opportunity report: unnests the engine's `index_recommendations`,
resolves the target table, and computes a **composite impact score** from a weighted
blend of total latency (0.5), contention (0.3), rows read (0.1), and retry (0.1). Groups
by table, tags each as `READ_HEAVY` / `WRITE_HEAVY` / `BALANCED`, and cross-references
**existing** indexes and their usage so you can tell a genuinely missing index from a
redundant one. Default lookback is 24h and min 50 executions — both tunable in the
`params` CTE at the top.

### 05 · Top contributors to contention — [`05_top_contention.sql`](05_top_contention.sql)
Three angles on lock contention from `transaction_contention_events`:
**(A)** hot objects — which table/index/contention-type accumulates the most wait time;
**(B)** the statements that spend the most time *waiting* on locks (with sample query
text joined from `cluster_statement_statistics`);
**(C)** the transactions that most often *block* others (with a distinct-victim count).

### 06 · Failed / retried transactions — [`06_failed_retried_txns.sql`](06_failed_retried_txns.sql)
Rolls statement-level failure counts up to the transaction fingerprint and classifies each
txn's failure behavior as `NEVER` / `SOMETIMES` / `ALWAYS`, with a failed-to-total ratio.
Great for spotting serialization-retry hotspots. Default lookback 3h.

### 07 · Write hotspots (rows written) — [`07_write_hotspots.sql`](07_write_hotspots.sql)
Approximates total rows written per table (`execs × mean rows written`) by parsing the
target table out of INSERT/UPSERT/MERGE/UPDATE/DELETE fingerprints. Identifies the
write-heaviest tables. Default lookback 2h.

---

## From a finding to a fix (worked example)

Once query 01 or 02 surfaces a slow full-scan statement, validate the plan and iterate —
exactly as in the main README's "Slow Performers" walkthrough:

```sql
EXPLAIN ANALYZE
SELECT flight_id FROM flights
AS OF SYSTEM TIME follower_read_timestamp()
ORDER BY random() LIMIT 16;
```

Look at `spans: FULL SCAN`, `KV rows decoded`, and `KV time`. If query 04 recommends an
index, apply it and re-run `EXPLAIN ANALYZE` to confirm the scan is gone and rows decoded
dropped. (For cases with no helpful index — like `ORDER BY random()` — the README shows a
`table_row_statistics`-based offset trick that reads ~10k rows instead of the full table.)

---

## Suggested test / demo methodology

1. **Baseline** — run `00_check_settings.sql`. Record the **data horizon** (`span`) for
   each source, especially `cluster_execution_insights` and `transaction_contention_events`
   — that number is how much history you have before it's gone. If the tables are sparse,
   optionally raise the retention caps (commented `SET`s) for the evaluation window. Re-run
   this at the end and compare: a shrinking/flat span under load is the persistence case.
2. **Generate load** — drive representative traffic. The repo's `dbworkload` workload
   (`transactions.py`) can simulate reads, writes, and explicit contention; or point the
   queries at real application traffic.
3. **Observe live** — while load runs, execute queries 01–07 and note the findings:
   slow statements, full scans, flagged insights, index recs, contention hot spots,
   retry hotspots, write hotspots.
4. **Act & re-measure** — apply an index recommendation from 04, then re-run 01/02/04 and
   `EXPLAIN ANALYZE` to show the improvement.
5. **Decide on persistence** — because these tables roll off, re-running the same query an
   hour later will show less/different data. That gap is the case for the daemon: if the
   customer wants run-over-run comparison and 90-day retention, move to the full
   [query-analysis](../README.md) setup.

---

## Limitations to set expectations

- **In-memory / capped.** `cluster_execution_insights` and `transaction_contention_events`
  are bounded ring buffers; `*_statistics` fingerprints are capped by the `max_mem_*`
  settings. High-cardinality workloads evict older entries. Restarting a node clears the
  in-memory portion.
- **Fingerprints, not literals.** Stats are aggregated by statement/transaction
  *fingerprint* (constants replaced by `_`), so you see patterns, not individual runs.
- **Approximations.** "Total time", "rows written", etc. are `count × mean`, not exact sums.
- **No cross-run comparison.** Live queries can't compare "before" vs "after" a change
  once the earlier data has rolled off — that is what the persistent tables provide.
