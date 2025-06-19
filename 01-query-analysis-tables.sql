CREATE DATABASE IF NOT EXISTS schedules;
USE schedules;
CREATE SCHEMA IF NOT EXISTS workload_test;


DROP TABLE IF EXISTS workload_test.transaction_contention_events CASCADE;
DROP TABLE IF EXISTS workload_test.cluster_execution_insights CASCADE;
DROP TABLE IF EXISTS workload_test.transaction_statistics CASCADE;
DROP TABLE IF EXISTS workload_test.statement_statistics CASCADE;
DROP TABLE IF EXISTS workload_test.test_run_configurations CASCADE;


-- bridge table to link test runs to observability data
CREATE TABLE workload_test.test_run_configurations (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run STRING NOT NULL,
	database_name STRING NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
	last_copy_time TIMESTAMPTZ,
	last_agg_copy_time TIMESTAMPTZ,
    CONSTRAINT uq_test_run_config UNIQUE (test_run),
    INDEX idx_test_run_times (start_time, end_time) STORING (test_run, database_name, last_copy_time, last_agg_copy_time)
)
WITH (ttl = 'on', ttl_expiration_expression = e'(end_time + INTERVAL \'90 days\')');


-- crdb_internal.transaction_contention_events
CREATE TABLE workload_test.transaction_contention_events (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run STRING NOT NULL,
    collection_ts TIMESTAMPTZ NOT NULL,
	blocking_txn_id UUID NOT NULL,
	blocking_txn_fingerprint_id BYTES NOT NULL,
	waiting_txn_id UUID NOT NULL,
	waiting_txn_fingerprint_id BYTES NOT NULL,
	contention_duration INTERVAL NOT NULL,
	contending_key BYTES NOT NULL,
	contending_pretty_key STRING NOT NULL,
	waiting_stmt_id STRING NOT NULL,
	waiting_stmt_fingerprint_id BYTES NOT NULL,
	database_name STRING NOT NULL,
	schema_name STRING NOT NULL,
	table_name STRING NOT NULL,
	index_name STRING NULL,
	contention_type STRING NOT NULL,
    CONSTRAINT fk_trce_to_trc FOREIGN KEY (test_run)
        REFERENCES workload_test.test_run_configurations (test_run)
		ON DELETE CASCADE,
    INDEX idx_trce_by_test_run (test_run)
)
WITH (ttl = 'on', ttl_expiration_expression = e'(collection_ts + INTERVAL \'90 days\')');


-- crdb_internal.cluster_execution_insights
CREATE TABLE workload_test.cluster_execution_insights (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run STRING NOT NULL,
	session_id STRING NOT NULL,
	txn_id UUID NOT NULL,
	txn_fingerprint_id BYTES NOT NULL,
	stmt_id STRING NOT NULL,
	stmt_fingerprint_id BYTES NOT NULL,
	problem STRING NOT NULL,
	causes STRING[] NOT NULL,
	query STRING NOT NULL,
	status STRING NOT NULL,
	start_time TIMESTAMP NOT NULL,
	end_time TIMESTAMP NOT NULL,
	full_scan BOOL NOT NULL,
	user_name STRING NOT NULL,
	app_name STRING NOT NULL,
	database_name STRING NOT NULL,
	plan_gist STRING NOT NULL,
	rows_read INT8 NOT NULL,
	rows_written INT8 NOT NULL,
	priority STRING NOT NULL,
	retries INT8 NOT NULL,
	last_retry_reason STRING NULL,
	exec_node_ids INT8[] NOT NULL,
	contention INTERVAL NULL,
	index_recommendations STRING[] NOT NULL,
	implicit_txn BOOL NOT NULL,
	cpu_sql_nanos INT8 NULL,
	error_code STRING NULL,
	last_error_redactable STRING NULL,
    CONSTRAINT fk_trei_to_trc FOREIGN KEY (test_run)
        REFERENCES workload_test.test_run_configurations (test_run)
		ON DELETE CASCADE,
    INDEX idx_trei_by_test_run (test_run)
)
WITH (ttl = 'on', ttl_expiration_expression = e'(start_time::TIMESTAMPTZ + INTERVAL \'90 days\')');


-- crdb_internal.transaction_statistics
CREATE TABLE workload_test.transaction_statistics (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run STRING NOT NULL,
	aggregated_ts TIMESTAMPTZ NOT NULL,
	fingerprint_id BYTES NOT NULL,
	app_name STRING NOT NULL,
	metadata JSONB NOT NULL,
	statistics JSONB NOT NULL,
	aggregation_interval INTERVAL NOT NULL,
    CONSTRAINT fk_trts_to_trc FOREIGN KEY (test_run)
        REFERENCES workload_test.test_run_configurations (test_run)
		ON DELETE CASCADE,
    CONSTRAINT uq_tran_stats UNIQUE (test_run, aggregated_ts, fingerprint_id, app_name),
    INDEX idx_trts_by_test_run (test_run)
)
WITH (ttl = 'on', ttl_expiration_expression = e'(aggregated_ts + INTERVAL \'90 days\')');


-- crdb_internal.statement_statistics
CREATE TABLE workload_test.statement_statistics (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run STRING NOT NULL,
	aggregated_ts TIMESTAMPTZ NOT NULL,
	fingerprint_id BYTES NOT NULL,
	transaction_fingerprint_id BYTES NOT NULL,
	plan_hash BYTES NOT NULL,
	app_name STRING NOT NULL,
	metadata JSONB NOT NULL,
	statistics JSONB NOT NULL,
	sampled_plan JSONB NOT NULL,
	aggregation_interval INTERVAL NOT NULL,
	index_recommendations STRING[] NOT NULL,
    CONSTRAINT fk_trss_to_trc FOREIGN KEY (test_run)
        REFERENCES workload_test.test_run_configurations (test_run)
		ON DELETE CASCADE,
    CONSTRAINT uq_stmt_stats UNIQUE (test_run, aggregated_ts, fingerprint_id, transaction_fingerprint_id, plan_hash, app_name),
    INDEX idx_trss_by_test_run (test_run)
)
WITH (ttl = 'on', ttl_expiration_expression = e'(aggregated_ts + INTERVAL \'90 days\')');
