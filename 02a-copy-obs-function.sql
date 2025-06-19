USE schedules;


CREATE OR REPLACE FUNCTION workload_test.copy_test_run_observations()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  -- arrays of configurations
  test_runs      STRING[];
  test_dbs       STRING[];
  start_times    TIMESTAMPTZ[];
  end_times      TIMESTAMPTZ[];
  last_times     TIMESTAMPTZ[];
  last_agg_times TIMESTAMPTZ[];
  cfg_count      INT;
  agg_interval   INTERVAL;
  i              INT;

  -- per-iteration vars
  test_name      STRING;
  test_db        STRING;
  from_ts        TIMESTAMPTZ;
  to_ts          TIMESTAMPTZ;
  cnt_cont       BIGINT;
  cnt_ins        BIGINT;
  cnt_stmt       BIGINT;
  cnt_txn        BIGINT;

  -- accumulator for results
  results        JSONB := '[]'::JSONB;
  
BEGIN
  -- 1) Pull out the “live” test runs into arrays:
  SELECT
    array_agg(test_run),
    array_agg(database_name),
    array_agg(start_time),
    array_agg(end_time),
    array_agg(last_copy_time),
    array_agg(last_agg_copy_time)
  INTO
    test_runs, test_dbs, start_times, end_times, last_times, last_agg_times
  FROM workload_test.test_run_configurations
  WHERE now() >= start_time
    AND (last_copy_time IS NULL
      OR last_copy_time < end_time
      OR last_agg_copy_time IS NULL
      OR last_agg_copy_time < end_time);

  cfg_count := array_length(test_runs, 1);
  IF cfg_count IS NULL OR cfg_count = 0 THEN
    RETURN results;
  END IF;
  
  -- 2) Loop by integer index (supported in v24.1):
  i := 1;
  WHILE i <= cfg_count LOOP
    test_name := test_runs[i];
    test_db   := test_dbs[i];
    from_ts   := coalesce(last_times[i], start_times[i]);
    to_ts     := LEAST(now(), end_times[i]);

    -- FIRST WE'LL CHECK FOR NON-AGGREGATED OBSERVABILITY METRICS
    IF from_ts < to_ts THEN
      RAISE NOTICE 'Scraping observability metrics: test_name=%, test_db=%, from_ts=%, to_ts=%',
        test_name, test_db, from_ts, to_ts;

      -- 3) insert contention events
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
      SELECT
        test_name,
        e.collection_ts,
        e.blocking_txn_id,
        e.blocking_txn_fingerprint_id,
        e.waiting_txn_id,
        e.waiting_txn_fingerprint_id,
        e.contention_duration,
        e.contending_key,
        e.contending_pretty_key,
        e.waiting_stmt_id,
        e.waiting_stmt_fingerprint_id,
        e.database_name,
        e.schema_name,
        e.table_name,
        e.index_name,
        e.contention_type
      FROM crdb_internal.transaction_contention_events AS e
      WHERE e.database_name = test_db
        AND e.collection_ts >= from_ts
        AND e.collection_ts < to_ts;

      -- 4) insert cluster execution insights
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
        contention,
        index_recommendations,
        implicit_txn,
        cpu_sql_nanos,
        error_code,
        last_error_redactable
      )
      SELECT
        test_name,
        i.session_id,
        i.txn_id,
        i.txn_fingerprint_id,
        i.stmt_id,
        i.stmt_fingerprint_id,
        i.problem,
        i.causes,
        i.query,
        i.status,
        i.start_time,
        i.end_time,
        i.full_scan,
        i.user_name,
        i.app_name,
        i.database_name,
        i.plan_gist,
        i.rows_read,
        i.rows_written,
        i.priority,
        i.retries,
        i.last_retry_reason,
        i.exec_node_ids,
        i.contention,
        i.index_recommendations,
        i.implicit_txn,
        i.cpu_sql_nanos,
        i.error_code,
        i.last_error_redactable
      FROM crdb_internal.cluster_execution_insights AS i
      WHERE i.database_name = test_db
        AND i.start_time >= from_ts
        AND i.start_time < to_ts;

      -- 5) advance last_copy_time so we don’t double-copy next run
      UPDATE workload_test.test_run_configurations
      SET last_copy_time = to_ts
      WHERE test_run = test_name;

      -- 6) now compute counts by querying each target table
      SELECT COUNT(*) INTO cnt_cont
        FROM workload_test.transaction_contention_events
      WHERE test_run = test_name
        AND collection_ts >= from_ts
        AND collection_ts < to_ts;

      SELECT COUNT(*) INTO cnt_ins
        FROM workload_test.cluster_execution_insights
      WHERE test_run = test_name
        AND start_time >= from_ts
        AND start_time < to_ts;
    
    END IF;

    -- THEN LOOK FOR AGGREGATED OBSERVABILITY METRICS WITH LESS FREQUENCY
    from_ts := coalesce(last_agg_times[i], start_times[i]);

    -- check if we've already captured all the relevant stats data
    IF from_ts < to_ts THEN
      SELECT max(aggregation_interval) * (1 + 10/60) INTO agg_interval
      FROM crdb_internal.transaction_statistics;
      
      -- then check if we're beyond the aggregation threshold for the previous period
      from_ts := date_trunc('hour', from_ts);
      IF now() - from_ts >= agg_interval THEN

        -- and this check confirms if we're on the last interval or now
        IF from_ts < date_trunc('hour', to_ts) THEN
          to_ts := date_trunc('hour', to_ts);
        END IF;

        RAISE NOTICE 'Scraping aggregated statistics: test_name=%, test_db=%, from_ts=%, to_ts=%',
          test_name, test_db, from_ts, to_ts;

        -- 7) insert statement stats
        INSERT INTO workload_test.statement_statistics (
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
        SELECT
          test_name,
          s.aggregated_ts,
          s.fingerprint_id,
          s.transaction_fingerprint_id,
          s.plan_hash,
          s.app_name,
          s.metadata,
          s.statistics,
          s.sampled_plan,
          s.aggregation_interval,
          s.index_recommendations
        FROM crdb_internal.statement_statistics AS s
        WHERE s.aggregated_ts >= from_ts
          AND s.aggregated_ts < to_ts
        ON CONFLICT (test_run, aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name)
        DO UPDATE SET
          metadata = EXCLUDED.metadata,
          statistics = EXCLUDED.statistics,
          sampled_plan = EXCLUDED.sampled_plan,
          index_recommendations = EXCLUDED.index_recommendations;

        -- 8) insert transaction stats
        INSERT INTO workload_test.transaction_statistics (
          test_run,
          aggregated_ts,
          fingerprint_id,
          app_name,
          metadata,
          statistics,
          aggregation_interval
        )
        SELECT
          test_name,
          x.aggregated_ts,
          x.fingerprint_id,
          x.app_name,
          x.metadata,
          x.statistics,
          x.aggregation_interval
        FROM crdb_internal.transaction_statistics AS x
        WHERE x.aggregated_ts >= from_ts
          AND x.aggregated_ts < to_ts
        ON CONFLICT (test_run, aggregated_ts, fingerprint_id, app_name)
        DO UPDATE SET
          metadata = EXCLUDED.metadata,
          statistics = EXCLUDED.statistics;

        -- 9) advance last_agg_copy_time so we don’t double-copy next run
        UPDATE workload_test.test_run_configurations
        SET last_agg_copy_time = to_ts
        WHERE test_run = test_name;

        -- 10) now compute counts by querying each target table
        SELECT COUNT(*) INTO cnt_stmt
          FROM workload_test.statement_statistics
        WHERE test_run = test_name
          AND aggregated_ts >= from_ts
          AND aggregated_ts < to_ts;

        SELECT COUNT(*) INTO cnt_txn
          FROM workload_test.transaction_statistics
        WHERE test_run = test_name
          AND aggregated_ts >= from_ts
          AND aggregated_ts < to_ts;
      
      END IF;
    END IF;
    
    -- 11) collect this run’s counts into our JSONB array
    results := results || jsonb_build_array(
      jsonb_build_object(
        'test_run',      test_name,
        'run_time',      now(),
        'contention',    cnt_cont,
        'insights',      cnt_ins,
        'statements',    cnt_stmt,
        'transactions',  cnt_txn
      )
    );

    i := i + 1;
  END LOOP;

  RETURN results;
END;
$$;