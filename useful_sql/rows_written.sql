-- Approx total rows written per table (INSERT/UPSERT/MERGE/UPDATE/DELETE)
WITH base AS (
  SELECT
    coalesce(metadata->>'query', metadata->>'querySummary') AS qry,
    (statistics->'statistics'->>'cnt')::float                 AS execs,
    coalesce((statistics->'statistics'->'rowsWritten'->>'mean')::float, 0) AS rows_written_mean
  FROM crdb_internal.cluster_statement_statistics
  WHERE aggregated_ts >= now() - interval '2 hours'
    AND app_name NOT LIKE '$ internal%'
    AND coalesce(metadata->>'db','') NOT IN ('system','crdb_internal')
    AND coalesce((statistics->'statistics'->'rowsWritten'->>'mean')::float, 0) > 0
),
proj AS (
  SELECT
    coalesce(
      lower(substring(qry FROM '(?is)\binsert\s+into\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)){0,2})')),
      lower(substring(qry FROM '(?is)\bupsert\s+into\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)){0,2})')),
      lower(substring(qry FROM '(?is)\bmerge\s+into\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)){0,2})')),
      lower(substring(qry FROM '(?is)\bupdate\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)){0,2})')),
      lower(substring(qry FROM '(?is)\bdelete\s+from\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)){0,2})'))
    ) AS fq_name,
    execs * rows_written_mean AS approx_rows
  FROM base
)
SELECT
  fq_name                                                             AS table_identifier,
  reverse(split_part(reverse(replace(fq_name,'"','')), '.', 1))       AS table_name,  -- best-effort leaf name
  sum(approx_rows)::bigint                                            AS approx_rows_written
FROM proj
WHERE fq_name IS NOT NULL
  AND fq_name NOT LIKE 'system.%'
  AND fq_name NOT LIKE 'crdb_internal.%'
GROUP BY 1,2
ORDER BY approx_rows_written DESC;