USE schedules;


DROP TABLE IF EXISTS schedules.test_run_contention_events;
DROP TABLE IF EXISTS schedules.test_run_execution_insights;
DROP TABLE IF EXISTS schedules.test_run_transaction_statistics;
DROP TABLE IF EXISTS schedules.test_run_statement_statistics;


-- crdb_internal.transaction_contention_events
CREATE TABLE schedules.test_run_contention_events (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run INT8 NOT NULL,
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
	contention_type STRING NOT NULL
)
WITH (ttl = 'on', ttl_expiration_expression = e'(collection_ts + INTERVAL \'90 days\')');



-- crdb_internal.cluster_execution_insights
CREATE TABLE schedules.test_run_execution_insights (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run INT8 NOT NULL,
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
	kv_node_ids INT8[] NOT NULL,
	contention INTERVAL NULL,
	index_recommendations STRING[] NOT NULL,
	implicit_txn BOOL NOT NULL,
	cpu_sql_nanos INT8 NULL,
	error_code STRING NULL,
	last_error_redactable STRING NULL
)
WITH (ttl = 'on', ttl_expiration_expression = e'(start_time::TIMESTAMPTZ + INTERVAL \'90 days\')');



-- crdb_internal.transaction_statistics
CREATE TABLE schedules.test_run_transaction_statistics (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run INT8 NOT NULL,
	aggregated_ts TIMESTAMPTZ NOT NULL,
	fingerprint_id BYTES NOT NULL,
	app_name STRING NOT NULL,
	metadata JSONB NOT NULL,
	statistics JSONB NOT NULL,
	aggregation_interval INTERVAL NOT NULL
)
WITH (ttl = 'on', ttl_expiration_expression = e'(aggregated_ts + INTERVAL \'90 days\')');


-- crdb_internal.statement_statistics
CREATE TABLE schedules.test_run_statement_statistics (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_run INT8 NOT NULL,
	aggregated_ts TIMESTAMPTZ NOT NULL,
	fingerprint_id BYTES NOT NULL,
	transaction_fingerprint_id BYTES NOT NULL,
	plan_hash BYTES NOT NULL,
	app_name STRING NOT NULL,
	metadata JSONB NOT NULL,
	statistics JSONB NOT NULL,
	sampled_plan JSONB NOT NULL,
	aggregation_interval INTERVAL NOT NULL,
	index_recommendations STRING[] NOT NULL
)
WITH (ttl = 'on', ttl_expiration_expression = e'(aggregated_ts + INTERVAL \'90 days\')');
