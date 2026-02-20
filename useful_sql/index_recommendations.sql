-- ============================================
-- Advanced Index Opportunity Report (v25)
-- - Groups index recommendations by table (and schema/db if present)
-- - Lists related statement fingerprints and sample queries
-- - Computes latency/contention/rows workload impact and a composite score
-- ============================================

WITH
-- ============================================
-- Tunable Parameters
-- ============================================
params AS (
  SELECT
    50  AS min_execution_threshold,
    0.5 AS latency_weight,
    0.3 AS contention_weight,
    0.1 AS rows_read_weight,
    0.1 AS retry_weight
),

-- ============================================
-- Expand Statement Statistics + Index Recommendations
-- - Unnest STRING[] index_recommendations
-- - Extract target object path once via regexp_extract into full_path
-- - Derive db/schema/table with safe guards for 1/2/3-part names
-- ============================================
expanded AS (
  SELECT
    ss.fingerprint_id,
    ss.app_name,
    ss.metadata->>'query' AS query,

    rec.index_recommendation,

    target.full_path,

    -- Split once (used for safe extraction)
    string_to_array(target.full_path, '.') AS path_parts,
    array_length(string_to_array(target.full_path, '.'), 1) AS path_len,

    -- Database = 3rd element from end, only if >= 3 parts
    CASE
      WHEN array_length(string_to_array(target.full_path, '.'), 1) >= 3
      THEN (string_to_array(target.full_path, '.'))[
        array_length(string_to_array(target.full_path, '.'), 1) - 2
      ]
      ELSE NULL
    END AS database_name,

    -- Schema = 2nd element from end, only if >= 2 parts
    CASE
      WHEN array_length(string_to_array(target.full_path, '.'), 1) >= 2
      THEN (string_to_array(target.full_path, '.'))[
        array_length(string_to_array(target.full_path, '.'), 1) - 1
      ]
      ELSE NULL
    END AS schema_name,

    -- Table = last element, always if regex matched
    (string_to_array(target.full_path, '.'))[
      array_length(string_to_array(target.full_path, '.'), 1)
    ] AS table_name,

    -- --------------------------------------------
    -- Stats (keep your tested v25 JSON paths)
    -- --------------------------------------------
    (ss.statistics->'execution_statistics'->>'cnt')::INT AS execution_count,

    (ss.statistics->'statistics'->'svcLat'->>'mean')::FLOAT AS avg_latency_sec,
    (ss.statistics->'execution_statistics'->'contentionTime'->>'mean')::FLOAT AS avg_contention_sec,
    (ss.statistics->'statistics'->'rowsRead'->>'mean')::FLOAT AS avg_rows_read,
    (ss.statistics->'statistics'->'rowsWritten'->>'mean')::FLOAT AS avg_rows_written,

    (ss.metadata->>'fullScan')::BOOL AS full_scan

  FROM crdb_internal.statement_statistics ss
  CROSS JOIN LATERAL
    unnest(ss.index_recommendations) AS rec(index_recommendation)
  CROSS JOIN LATERAL
    -- parse the target table path from the index recommendation string
    regexp_extract(
      rec.index_recommendation,
      '(?i)ON\s+([a-zA-Z0-9_\.]+)'
    ) AS target(full_path)
  CROSS JOIN params p
  WHERE
    array_length(ss.index_recommendations, 1) > 0
    AND (ss.statistics->'execution_statistics'->>'cnt')::INT >= p.min_execution_threshold
    -- guard: only keep rows where we successfully extracted a path
    AND target.full_path IS NOT NULL
),

-- ============================================
-- Score / Derive Workload Totals
-- - Cast execution_count to FLOAT for derived math
-- - Pass down db/schema/table
-- ============================================
scored AS (
  SELECT
    database_name,
    schema_name,
    table_name,
    full_path,

    index_recommendation,
    fingerprint_id,
    app_name,
    query,
    full_scan,

    execution_count,
    execution_count::FLOAT AS execution_count_f,

    avg_latency_sec,
    avg_contention_sec,
    avg_rows_read,
    avg_rows_written,

    (execution_count::FLOAT * avg_latency_sec) AS total_latency_sec,
    (execution_count::FLOAT * avg_contention_sec) AS total_contention_sec,
    (execution_count::FLOAT * avg_rows_read) AS total_rows_read,
    (execution_count::FLOAT * avg_rows_written) AS total_rows_written

  FROM expanded
),

-- ============================================
-- Aggregate Per (db, schema, table, recommendation)
-- - Groups overlapping recs by table
-- - Collects fingerprints + sample queries
-- ============================================
aggregated AS (
  SELECT
    database_name,
    schema_name,
    table_name,
    full_path,
    index_recommendation,

    SUM(execution_count_f) AS total_executions,
    SUM(total_latency_sec) AS total_latency_sec,
    SUM(total_contention_sec) AS total_contention_sec,
    SUM(total_rows_read) AS total_rows_read,
    SUM(total_rows_written) AS total_rows_written,

    -- Weighted averages
    SUM(total_latency_sec) / NULLIF(SUM(execution_count_f), 0) AS weighted_avg_latency_sec,
    SUM(total_contention_sec) / NULLIF(SUM(execution_count_f), 0) AS weighted_avg_contention_sec,

    BOOL_OR(full_scan) AS involves_full_scan,

    ARRAY_AGG(DISTINCT fingerprint_id) AS related_fingerprints,
    ARRAY_AGG(DISTINCT app_name) AS related_applications,
    ARRAY_AGG(DISTINCT substring(query, 1, 200)) AS sample_queries

  FROM scored
  GROUP BY
    database_name,
    schema_name,
    table_name,
    full_path,
    index_recommendation
),

-- ============================================
-- Existing Index + Usage Resolution
-- - Resolve table_id from database/schema/table
-- - Join to table_indexes for index names
-- - Join to index_usage_statistics for usage data
-- ============================================
index_presence AS (

  SELECT
    t.database_name,
    t.schema_name,
    t.name AS table_name,
    ti.index_name,
    ti.index_id,
    ius.total_reads,
    ius.last_read

  FROM crdb_internal.tables t

  JOIN crdb_internal.table_indexes ti
    ON t.table_id = ti.descriptor_id

  LEFT JOIN crdb_internal.index_usage_statistics ius
    ON ti.descriptor_id = ius.table_id
   AND ti.index_id = ius.index_id
),

-- ============================================
-- Aggregate Existing Indexes With Usage Per Table
-- ============================================
index_presence_agg AS (
  SELECT
    database_name,
    schema_name,
    table_name,

    json_agg(
      json_build_object(
        'index_name', index_name,
        'index_id', index_id,
        'total_reads', COALESCE(total_reads, 0),
        'last_read', last_read
      )
      ORDER BY COALESCE(total_reads, 0) DESC
    ) AS existing_index_usage

  FROM index_presence
  GROUP BY database_name, schema_name, table_name
)

-- ============================================
-- Final Output + Impact Score
-- ============================================
SELECT
  a.database_name,
  a.schema_name,
  a.table_name,
  a.full_path,
  a.index_recommendation,

  a.total_executions,
  a.total_latency_sec,
  a.total_contention_sec,
  a.total_rows_read,
  a.total_rows_written,

  a.weighted_avg_latency_sec,
  a.weighted_avg_contention_sec,

  CASE
    WHEN a.total_rows_written > a.total_rows_read THEN 'WRITE_HEAVY'
    WHEN a.total_rows_read > a.total_rows_written THEN 'READ_HEAVY'
    ELSE 'BALANCED'
  END AS workload_profile,

  a.involves_full_scan,

  ip.existing_index_usage,

  -- Composite impact score (tunable)
  (
    a.total_latency_sec::DECIMAL * p.latency_weight +
    a.total_contention_sec::DECIMAL * p.contention_weight +
    (a.total_rows_read / 1000000.0)::DECIMAL * p.rows_read_weight +
    (a.total_contention_sec * 0.5)::DECIMAL * p.retry_weight
  ) AS impact_score,

  a.related_fingerprints,
  a.related_applications,
  a.sample_queries

FROM aggregated a
LEFT JOIN index_presence_agg ip
  ON a.table_name = ip.table_name
 AND (a.schema_name IS NULL OR a.schema_name = ip.schema_name)
 AND (a.database_name IS NULL OR a.database_name = ip.database_name)

CROSS JOIN params p

ORDER BY impact_score DESC;
