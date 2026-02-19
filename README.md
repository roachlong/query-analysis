# query-analysis
This repo was setup to provide an example of using our dbworkload tool to identify and optimize workloads.  We'll piggyback off of our [distributed-rollups](https://github.com/roachlong/distributed-rollups) repository to setup and populate a sample schema with information on flight schedules.  Then we'll show how we can extract observability metrics for the workload and evaluate contention and/or poorly performing queries to optimize your database.


## Clone the Repository
First install [git](https://git-scm.com) if you don't already have it.  Instructions for Mac, Windows or Linux can be found [here](https://www.atlassian.com/git/tutorials/install-git).  Then open a Mac Terminal or Windows PowerShell in your workspace folder (or wherever you keep your local repositories) and execute the following command.
```
git clone https://github.com/roachlong/query-analysis.git
cd query-analysis
git status
```


## Cockroach
If we're executing the PoC as a stand alone lab we can install and run a single node instance of cockroach on our laptops.  For Mac you can install CRDB with ```brew install cockroachdb/tap/cockroach```.  For Windows you can download and extract the latest binary from [here](https://www.cockroachlabs.com/docs/releases), then add the location of the cockroach.exe file (i.e. C:\Users\myname\AppData\Roaming\cockroach) to your Windows Path environment variable.

Then open a new Mac Terminal or PowerShell window and execute the following command to launch your single node database, here we are testing with v24.1.
```
alias crdb24=/opt/homebrew/Cellar/cockroach\@24.1/24.1.19/bin/cockroach
crdb24 start-single-node --insecure --advertise-addr=localhost --store=./data --background
```
Then open a browser to http://localhost:8080 to view the dashboard for your local cockroach instance

And populate our database leveraging scripts from the distributed-rollups repository.
```
conn_str="postgresql://localhost:26257/defaultdb?sslmode=disable"
cd ../distributed-rollups/
cockroach sql --url "$conn_str" -f 00-initial-schema.sql
export conn_str="${conn_str/defaultdb/schedules}"
cockroach sql --url "$conn_str" -f 01-populate-sample-data.sql
cd -
```

There are also a few cluster settings that manage how much information is stored in our internal obseravbility tables.  The default values for these have been decreased since v24.1 in an effort to reduce cardinality of information collected and decrease memory pressure leading to improved performance.  But these default values may be artificially low for many workloads and small cluster sizes.  For benchmark testing, in non-production environments where we can control the velocity and volumne of data for our workloads, you may be able to adjust these settings and observe the behavior in our system.
```
cockroach sql --url "$conn_str" -e """
set cluster setting sql.metrics.max_mem_stmt_fingerprints = 100000;  -- instead of 7500
set cluster setting sql.metrics.max_mem_txn_fingerprints = 100000;  -- instead of 7500
set cluster setting sql.insights.execution_insights_capacity = 20000;  -- instead of 1000
"""
```


## Setup Query Analysis Tables
When we run workload tests we may want to compare results between runs or analyze the data from a particular run later.  Our CRDB internal tables offer great insights into contention events, transaction runtimes, statement statistics, etc.  However, these are in-memory structures and data will roll-off eventually, or if you restart your nodes the data is lost.  But we can create storage tables and offload information for specific runs and maintain that data over time.  And with real tables we can create an indexing strategy that will make it easier for us to query the data.  The recommendation is to load your test runs based on a "partition" identifier and use TTL to control when you want to cleanup old test runs.  We don't really need to maintain partitions since CRDB is already distributing our data evenly across ranges.  But we'll add a test_run identifier so we can easily segment our data when querying.  And by defualt we'll expire the data for a test run after 90 days.

For demonstration purposes I'm going to capture observability information from the following crdb_internal tables.
- transaction_contention_events
- cluster_execution_insights
- cluster_transaction_statistics
- cluster_statement_statistics

First we'll store the connection string as a variable in our terminal shell window.  On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://localhost:26257/schedules?sslmode=disable"
```

We can create the storage tables with TTL enabled by executing the following script.
```
cockroach sql --url "$conn_str" -f 01-query-analysis-tables.sql
```

Then we'll run a python daemon to capture observanility metrics 
```
export DATABASE_URL="postgresql://root@localhost:26257/schedules?sslmode=disable"
export LOG_FILE="./log/copy_obs_data.log"
nohup python 02_copy_obs_data.py > /dev/null 2>&1 & disown
```

To stop the process later you can run
```
pkill -f 02_copy_obs_data.py
```

Or to setup the daemon to run as a system service
```
cat <<'EOF' > /etc/systemd/system/copy-obs-data.service
[Unit]
Description=Copy Observation Data Daemon
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/query-analysis
Environment="DATABASE_URL=postgresql://user:pass@host:26257/db?sslmode=require"
Environment="LOG_FILE=/var/log/copy_obs_data.log"
ExecStart=/usr/bin/python 02_copy_obs_data.py
Restart=always
RestartSec=5
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable copy-obs-data
sudo systemctl start copy-obs-data
```

Finally we'll setup the function that will allow us to inspect contention related to a SQL retry error.
```
-- Either v24
cockroach sql --url "$conn_str" -f 03-v24-query-analysis-procedure.sql
-- OR v25 script
cockroach sql --url "$conn_str" -f 03-v25-query-analysis-function.sql
```


## dbworkload
This is a tool we use to simulate data flowing into cockroach, developed by one of our colleagues with python.  We can install the tool with ```pip3 install "dbworkload[postgres]"```, and then add it to your path.  On Mac or Linux with Bash you can use:
```
echo -e '\nexport PATH=`python3 -m site --user-base`/bin:$PATH' >> ~/.bashrc 
source ~/.bashrc
```
For Windows it's just ```pip install dbworkload``` and you can add the location of the dbworkload.exe file (i.e. C:\Users\myname\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.9_abcdefghijk99\LocalCache\local-packages\Python39\Scripts) to your Windows Path environment variable.  The pip command above should provide the exact path to your local python executables.


## Sample Workload
We can create workloads to test a variety of scenarios, including implicit and explicit transactions, bulk writes, simulate contention, connection swarms, etc.  And we can control the velocity and volume of the workload with custom properties.  I've created a few examples for our flight schedule schema described below.
* num_connections: we'll simulate the workload across a number of processes
* duration: the number of minutes for which we want to run the simulation
* schedule_freq: the percentage of cycles we want to make updates to the flight schedule
* status_freq: the percentage of cycles we want to make updates to flight status
* inventory_freq: the percentage of cycles we want to make updates to the available seating
* price_freq: the percentage of cycles we want to make updates to the ticket prices
* contention_freq: the percentage of cycles we want to simulate a contention scenario
* batch_size: the number of records we want to update in a single cycle
* delay: the number of milliseconds we should pause between transactions, so we don't overload admission controls

We'll store this information as variables in the terminal shell window. On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://root@localhost:26257/schedules?sslmode=disable"
num_connections=16
duration=60
schedule_freq=10
status_freq=90
inventory_freq=75
price_freq=25
contention_freq=10
batch_size=16
delay=10
```

Before we run the test, let's insert a record into our test run configurations table to record observations for the next hour
```
cockroach sql --url "$conn_str" -e """
INSERT INTO workload_test.test_run_configurations (
  test_run,
  database_name,
  start_time,
  end_time
) VALUES (
  'load_test_2026_02_19',            -- your test_run name
  'schedules',                       -- the database you’re targeting
  NOW(),                             -- when the test started
  NOW() + INTERVAL '1 hour'          -- when it ends
);
"""
```

Then we can use our dbworkload script to simulate the workload.  **Note**: with Windows PowerShell replace each backslash double quote(\\") with a pair of double quotes around the json properties, i.e. ``` ""batch_size"": ""$batch_size"" ```
```
dbworkload run -w transactions.py -c $num_connections -d $(( ${duration} * 60 )) --uri "$conn_str" --args "{
        \"schedule_freq\": $schedule_freq,
        \"status_freq\": $status_freq,
        \"inventory_freq\": $inventory_freq,
        \"price_freq\": $price_freq,
        \"contention_freq\": $contention_freq,
        \"batch_size\": $batch_size,
        \"delay\": $delay
    }"
```

When the workload completes it will print out a summary of percentile latencies for each transaction.
```
-------------  ----------------------------
run_name       Transactions.20260219_212810
start_time     2026-02-19 21:28:10
end_time       2026-02-19 22:28:10
test_duration  3600
-------------  ----------------------------

┌───────────┬────────────┬───────────┬───────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
│   elapsed │ id         │   threads │   tot_ops │   tot_ops/s │   mean(ms) │   p50(ms) │   p90(ms) │   p95(ms) │   p99(ms) │   max(ms) │
├───────────┼────────────┼───────────┼───────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
│     3,600 │ __cycle__  │        16 │    44,886 │          12 │   1,282.39 │  1,268.77 │  1,931.17 │  2,044.22 │  2,533.65 │  4,632.82 │
│     3,600 │ contention │        16 │    44,886 │          12 │       5.87 │      0.00 │     23.36 │     58.85 │     76.23 │    139.94 │
│     3,600 │ inventory  │        16 │    44,886 │          12 │     478.50 │    604.50 │    698.71 │    736.36 │    870.40 │  1,526.28 │
│     3,600 │ price      │        16 │    44,886 │          12 │     159.65 │      0.01 │    640.60 │    677.26 │    761.08 │  1,529.12 │
│     3,600 │ schedule   │        16 │    44,886 │          12 │      63.52 │      0.01 │    303.76 │    629.21 │    719.32 │  1,630.14 │
│     3,600 │ status     │        16 │    44,886 │          12 │     574.84 │    620.94 │    709.18 │    748.64 │    884.37 │  1,590.98 │
└───────────┴────────────┴───────────┴───────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘
```


## Observability
Our data ingestion process to copy crdb internal system information is publishing metrics and alerts for prometheus.  These are process data points related to the number of observations, lag time and errors ecnountered during the process.  You can use the docker compose file found in the observability folder to investigate and visualize these metrics with grafana.
```
cd observability && docker-compose up -d && cd ..
```
Then you can navigate to:
- Prometheus at http://localhost:9090
- Grafana at http://localhost:3000

In Prometheus you can explore the metrics that we publish
![copy_obs_data metrics](image.png)

In Grafana you'll need to Go to Connections → Data Sources to add Prometheus with the URL http://prometheus:9090.


## Data Collection
We can also get more grainular information on transaction and statememt statistics from the CRDB Admin Console, which are backed by our CRDB internal observability tables.  And since we're exporting the data into our own physical tables can compare metrics between runs and measure performance improvements over time.

As an example, we can grab any exception in the workload's log output to investigate statements related to a failed transaction.
```
error_str=$(cat <<EOF
Error occurred: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError: write for key /Table/107/1/"\x05\xa6\xfdv\xdejE:\xa7\xe8T\xa7s\v\x192"/0 at timestamp 1771539694.711113000,0 too old; must write at or above 1771539694.715943000,1: "sql txn" meta={id=103453f8 key=/Table/107/1/"\x05\xa6\xfdv\xdejE:\xa7\xe8T\xa7s\v\x192"/0 iso=Serializable pri=0.05014185 epo=0 ts=1771539694.715943000,1 min=1771539694.711113000,0 seq=1} lock=true stat=PENDING rts=1771539694.711113000,0 gul=1771539695.211113000,0 obs={n1@1771539694.711113000,0}
HINT:  See: https://www.cockroachlabs.com/docs/v25.3/transaction-retry-error-reference.html
2026-02-19 22:21:38.000001 [WARNING] (MainProcess MainThread) run:507: stats_received=16, expected=16. CPU Util=98.3%, Memory=75.4%
EOF
)
```

**AND EITHER V24**
```
cockroach sql --url "$conn_str" -e """
CALL workload_test.inspect_contention_from_exception(
  '$$(echo "$error_str")$$',
  NULL,                    -- out select_query
  'test-caller-1',         -- in caller_id
  'load_test_2025_06_18',  -- in test_run
  'Transactions',          -- in app_name
  'public',                -- in schema_name
  'same_app'               -- in contention option
);
"""

cockroach sql --url "$conn_str" -e """
SELECT   test_run,   collection_ts,   database_name,   schema_name,   table_name,   index_name,   contention_type,   app_name,   encode(transaction_fingerprint_id, 'hex') AS txn_fingerprint_id,   role AS tnx_type,   contention,   encode(fingerprint_id, 'hex') AS stmt_fingerprint_id,   stmt_metadata->'fullScan' AS fullscan,   index_recommendations,   ord AS stmt_order,   status,   stmt_metadata->'query' AS sql_statement FROM workload_test.caller_contention_results WHERE caller_id = 'test-caller-1' ORDER BY test_run, encode(transaction_fingerprint_id, 'hex'), ord;
"""

        test_run       |         collection_ts         | database_name | schema_name | table_name | index_name | contention_type |   app_name   | txn_fingerprint_id | tnx_type | contention | stmt_fingerprint_id | fullscan |                                         index_recommendations                                          | stmt_order | status |                  sql_statement
-----------------------+-------------------------------+---------------+-------------+------------+------------+-----------------+--------------+--------------------+----------+------------+---------------------+----------+--------------------------------------------------------------------------------------------------------+------------+--------+---------------------------------------------------
  load_test_2025_06_18 | NULL                          | NULL          | NULL        | NULL       | NULL       | NULL            | Transactions | 1c0d24389254fc7a   | waiting  |     f      | 2ab0c15b7b14e792    | true     | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"} |          1 | NULL   | "SELECT * FROM airports WHERE city = _"
  load_test_2025_06_18 | 2025-06-19 02:24:30.687285+00 | schedules     | public      | NULL       | NULL       | NULL            | Transactions | 1c0d24389254fc7a   | waiting  |     f      | 67a10dfb99638ead    | true     | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"} |          2 | failed | "UPDATE airports SET country = _ WHERE city = _"
```

**OR V25 SCRIPT**
```
cockroach sql --url "$conn_str" -e """
SELECT * 
FROM workload_test.inspect_contention_from_exception(
  '$$(echo "$error_str")$$',
  'load_test_2026_02_19',  -- in test_run
  'Transactions',          -- in app_name
  'public',                -- in schema_name
  'same_app'               -- in contention option
);
"""


        test_run       | ord |  role   | status |         collection_ts         |     aggregated_ts      |   app_name   | database_name | schema_name | table_name | index_name |                           txn_metadata                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    txn_statistics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | contention_type | contention |   fingerprint_id   | transaction_fingerprint_id |     plan_hash      |                                                                                                                 stmt_metadata                                                                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           stmt_statistics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |         sampled_plan         | aggregation_interval |                                         index_recommendations
-----------------------+-----+---------+--------+-------------------------------+------------------------+--------------+---------------+-------------+------------+------------+------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+------------+--------------------+----------------------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------+----------------------+---------------------------------------------------------------------------------------------------------
  load_test_2026_02_19 |   1 | waiting | NULL   | NULL                          | 2026-02-19 22:00:00+00 | Transactions | NULL          | NULL        | NULL       | NULL       | {"stmtFingerprintIDs": ["2ab0c15b7b14e792", "67a10dfb99638ead"]} | {"execution_statistics": {"cnt": 6, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 259390.16666666666, "sqDiff": 49715327768.833336}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 183756, "sqDiff": 2291245488.000001}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 42, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 4E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 42, "sqDiff": 0}, "valueBytes": {"mean": 1704, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "statistics": {"bytesRead": {"mean": 3234.347826086954, "sqDiff": 1224.347826087057}, "cnt": 460, "commitLat": {"mean": 0.004465308069565223, "sqDiff": 0.004778839902503214}, "idleLat": {"mean": 0.04196431198260871, "sqDiff": 0.11875843074468731}, "maxRetries": 0, "numRows": {"mean": 1.9543478260869571, "sqDiff": 20.041304347826088}, "retryLat": {"mean": 0, "sqDiff": 0}, "rowsRead": {"mean": 4E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "svcLat": {"mean": 0.04553878941304344, "sqDiff": 0.07738831830996401}}} | NULL            |     f      | \x2ab0c15b7b14e792 | \x1c0d24389254fc7a         | \x025144e18ceb2338 | {"db": "schedules", "distsql": false, "fullScan": true, "implicitTxn": false, "query": "SELECT * FROM airports WHERE city = _", "querySummary": "SELECT * FROM airports", "stmtType": "TypeDML", "vec": true}                                  | {"execution_statistics": {"cnt": 7, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 18369.714285714286, "sqDiff": 150085547.4285714}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 2.048E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 90891, "sqDiff": 6.1372647E+8}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 21, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 1, "sqDiff": 0}, "seekCountInternal": {"mean": 1, "sqDiff": 0}, "stepCount": {"mean": 2E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 21, "sqDiff": 0}, "valueBytes": {"mean": 852, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": ["creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"], "statistics": {"bytesRead": {"mean": 1617.173913043477, "sqDiff": 306.08695652176425}, "cnt": 460, "failureCount": 0, "firstAttemptCnt": 460, "genericCount": 460, "idleLat": {"mean": 0.0015700895586956505, "sqDiff": 0.0012552656503659783}, "indexes": ["107@1"], "kvNodeIds": [1], "lastErrorCode": "", "lastExecAt": "2026-02-19T22:00:09.751818Z", "latencyInfo": {"max": 0.010465291, "min": 0.000169542}, "maxRetries": 0, "nodes": [1], "numRows": {"mean": 1, "sqDiff": 0}, "ovhLat": {"mean": 0.000004687076086956516, "sqDiff": 1.42785249986337E-7}, "parseLat": {"mean": 0.000006561773913043474, "sqDiff": 9.527536853248694E-8}, "planGists": ["AgHWAQIAHwAAAAMGCg=="], "planLat": {"mean": 0.00012793259565217405, "sqDiff": 0.00003875441849893079}, "regions": [], "rowsRead": {"mean": 2E+1, "sqDiff": 0}, "rowsWritten": {"mean": 0, "sqDiff": 0}, "runLat": {"mean": 0.0007337688347826083, "sqDiff": 0.000674909390925327}, "sqlType": "TypeDML", "svcLat": {"mean": 0.0008729502804347834, "sqDiff": 0.0007422521109827432}, "usedFollowerRead": false}}                                                      | {"Children": [], "Name": ""} | 01:00:00             | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"}
  load_test_2026_02_19 |   2 | waiting | failed | 2026-02-19 21:28:45.989144+00 | 2026-02-19 22:00:00+00 | Transactions | schedules     | public      | NULL       | NULL       | {"stmtFingerprintIDs": ["2ab0c15b7b14e792", "67a10dfb99638ead"]} | {"execution_statistics": {"cnt": 6, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 259390.16666666666, "sqDiff": 49715327768.833336}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 183756, "sqDiff": 2291245488.000001}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 42, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 4E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 42, "sqDiff": 0}, "valueBytes": {"mean": 1704, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "statistics": {"bytesRead": {"mean": 3234.347826086954, "sqDiff": 1224.347826087057}, "cnt": 460, "commitLat": {"mean": 0.004465308069565223, "sqDiff": 0.004778839902503214}, "idleLat": {"mean": 0.04196431198260871, "sqDiff": 0.11875843074468731}, "maxRetries": 0, "numRows": {"mean": 1.9543478260869571, "sqDiff": 20.041304347826088}, "retryLat": {"mean": 0, "sqDiff": 0}, "rowsRead": {"mean": 4E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "svcLat": {"mean": 0.04553878941304344, "sqDiff": 0.07738831830996401}}} | NULL            |     f      | \x67a10dfb99638ead | \x1c0d24389254fc7a         | \xfc4bcd4933e6c787 | {"db": "schedules", "distsql": false, "fullScan": true, "implicitTxn": false, "query": "UPDATE airports SET country = _ WHERE city = _", "querySummary": "UPDATE airports SET country = _ WHERE city = _", "stmtType": "TypeDML", "vec": true} | {"execution_statistics": {"cnt": 7, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 241220.57142857145, "sqDiff": 51071787497.71428}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 90891, "sqDiff": 6.1372647E+8}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 21, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 1, "sqDiff": 0}, "seekCountInternal": {"mean": 1, "sqDiff": 0}, "stepCount": {"mean": 2E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 21, "sqDiff": 0}, "valueBytes": {"mean": 852, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": ["creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"], "statistics": {"bytesRead": {"mean": 1617.173913043477, "sqDiff": 306.08695652176425}, "cnt": 460, "failureCount": 21, "firstAttemptCnt": 460, "genericCount": 460, "idleLat": {"mean": 0.036893010408695644, "sqDiff": 0.06785057027505134}, "indexes": ["107@1"], "kvNodeIds": [1], "lastErrorCode": "40001", "lastExecAt": "2026-02-19T22:00:09.751821Z", "latencyInfo": {"max": 0.036111709, "min": 0.000372416}, "maxRetries": 0, "nodes": [1], "numRows": {"mean": 0.9543478260869565, "sqDiff": 20.041304347826095}, "ovhLat": {"mean": 0.000001267558695652189, "sqDiff": 1.7273325541521575E-10}, "parseLat": {"mean": 0.000012714502173913054, "sqDiff": 0.0000012472759055609989}, "planGists": ["AgHWAQIAHwAAAAMHDAUMIdYBAAA="], "planLat": {"mean": 0.00013482153695652173, "sqDiff": 0.00002494050667105438}, "regions": [], "rowsRead": {"mean": 2E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "runLat": {"mean": 0.0015842477521739126, "sqDiff": 0.0035261367312931402}, "sqlType": "TypeDML", "svcLat": {"mean": 0.0017330513500000003, "sqDiff": 0.0036076589992625487}, "usedFollowerRead": false}} | {"Children": [], "Name": ""} | 01:00:00             | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"}
```

## Slow Performers
Now we can query the observability metrics for our last run to look for failures, retries, execution count, execution time, idle time, rows processed, amount of data read, amount of data written, almost anything you need.  Below is a simple example where we query for slow performers, relative to our workload.
```
cockroach sql --url "$conn_str" -e """
SELECT MAX(ROUND(CAST(statistics->'statistics'->'runLat'->'mean' AS FLOAT) * 1000, 2)) AS avg_ms,
       metadata->'query' AS QUERY
FROM workload_test.statement_statistics
WHERE test_run = 'load_test_2025_06_18'
  AND app_name = 'Transactions'
GROUP BY 2
ORDER BY 1 DESC
LIMIT 5;
"""

  avg_ms |                                                                                                                                                                            query
---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  636.42 | "SELECT flight_id FROM flights AS OF SYSTEM TIME follower_read_timestamp() ORDER BY random() LIMIT _"
   22.43 | "UPDATE flights SET scheduled_departure = scheduled_departure + (((CASE WHEN random() < _ THEN _ ELSE _ END) * (floor(random() * _) + _)::INT8) * _::INTERVAL), scheduled_arrival = scheduled_arrival + (((CASE WHEN random() < _ THEN _ ELSE _ END) * (floor(random() * _) + _)::INT8) * _::INTERVAL), updated_at = now() WHERE flight_id IN (_, __more__)"
   13.66 | "UPDATE flight_prices SET price_usd = price_usd * (_ + ((random() - _) / _))::DECIMAL, updated_at = now() WHERE flight_id IN (_, __more__)"
   13.48 | "UPDATE seat_inventory SET seats_available = (seats_available::FLOAT8 * (_ + ((random() - _) / _)))::INT8, updated_at = now() WHERE flight_id IN (_, __more__)"
   12.89 | "UPDATE flight_status SET status = (ARRAY[_, __more__])[_ + floor(random() * _)::INT8], updated_at = now() WHERE flight_id IN (_, __more__)"
```

We want to address our slowest running query, although this is a bad example because we're doing a full scan to get a random batch of flights from the table.  But let's run an explain plan to see how the optimizer will execute this query.
```
cockroach sql --url "$conn_str" -e """
EXPLAIN ANALYZE
SELECT flight_id
FROM flights
AS OF SYSTEM TIME follower_read_timestamp()
ORDER BY random()
LIMIT 16;
"""

                                                                 info
---------------------------------------------------------------------------------------------------------------------------------------
  planning time: 401µs
  execution time: 375ms
  distribution: full
  vectorized: true
  plan type: custom
  rows decoded from KV: 1,000,000 (143 MiB, 15 gRPC calls)
  cumulative time spent in KV: 370ms
  maximum memory usage: 10 MiB
  network usage: 0 B (0 messages)
  sql cpu time: 101ms
  isolation level: serializable
  priority: normal
  quality of service: regular

  • top-k
  │ nodes: n1
  │ actual row count: 16
  │ estimated max memory allocated: 10 KiB
  │ estimated max sql temp disk usage: 0 B
  │ sql cpu time: 5ms
  │ estimated row count: 16
  │ order: +column11
  │ k: 16
  │
  └── • render
      │
      └── • scan
            nodes: n1
            actual row count: 1,000,000
            KV time: 370ms
            KV contention time: 0µs
            KV rows decoded: 1,000,000
            KV bytes read: 143 MiB
            KV gRPC calls: 15
            estimated max memory allocated: 10 MiB
            sql cpu time: 95ms
            estimated row count: 1,000,000 (100% of the table; stats collected 19 hours ago; using stats forecast for 45 minutes ago)
            table: flights@flights_pkey
            spans: FULL SCAN
(39 rows)

Time: 378ms
```

There's not an index that will help with this use case.  So our objective would be to find another strategy to get similar results without a full table scan.  Below uses a random offset based on a slice of the overall table data, rather than pulling a million records from disk.  Same outcome, but only needs to decode ~10k records in about 4% of the execution time.
```
-- confirm the record count for flights
cockroach sql --url "$conn_str" -e """
SELECT estimated_row_count AS flights
FROM crdb_internal.table_row_statistics
WHERE table_name = 'flights';
"""

  flights
-----------
  1000000


-- then use table statistcs to slice the rows
cockroach sql --url "$conn_str" -e """
EXPLAIN ANALYZE
SELECT flight_id
FROM flights
AS OF SYSTEM TIME follower_read_timestamp()
OFFSET floor(random() * (
  SELECT (estimated_row_count / 10)::FLOAT AS flights
  FROM crdb_internal.table_row_statistics
  WHERE table_name = 'flights'
))::INT
LIMIT 16;
"""

                                                                          info
--------------------------------------------------------------------------------------------------------------------------------------------------------
  planning time: 192µs
  execution time: 14ms
  distribution: full
  vectorized: true
  plan type: generic, reused
  rows decoded from KV: 10,235 (1.5 MiB, 1 gRPC calls)
  cumulative time spent in KV: 3ms
  maximum memory usage: 1.5 MiB
  network usage: 0 B (0 messages)
  sql cpu time: 931µs
  isolation level: serializable
  priority: normal
  quality of service: regular

  • root
  │
  ├── • limit
  │   │ count: 16
  │   │
  │   └── • limit
  │       │ offset: floor(random() * @S1)::INT8
  │       │
  │       └── • scan
  │             nodes: n1
  │             actual row count: 16
  │             KV time: 3ms
  │             KV contention time: 0µs
  │             KV rows decoded: 10,235
  │             KV bytes read: 1.5 MiB
  │             KV gRPC calls: 1
  │             estimated max memory allocated: 1.5 MiB
  │             sql cpu time: 681µs
  │             estimated row count: 1,000,000 (100% of the table; stats collected 19 hours ago; using stats forecast for 47 minutes ago)
  │             table: flights@flights_pkey  ----------------------  WARNING: the row count estimate is inaccurate, consider running 'ANALYZE flights'
  │             spans: FULL SCAN
  │
  └── • subquery
      │ id: @S1
      │ original sql: (SELECT (estimated_row_count / 10)::FLOAT8 AS flights FROM crdb_internal.table_row_statistics WHERE table_name = 'flights')
      │ exec mode: one row
      │
      └── • max1row
          │ nodes: n1
          │ actual row count: 1
          │ sql cpu time: 33µs
          │ estimated row count: 1
          │
          └── • render
              │
              └── • filter
                  │ nodes: n1
                  │ actual row count: 1
                  │ sql cpu time: 24µs
                  │ filter: table_name = 'flights'
                  │
                  └── • virtual table
                        nodes: n1
                        actual row count: 335
                        sql cpu time: 194µs
                        table: table_row_statistics@primary

  WARNING: the row count estimate on table "flights" is inaccurate, consider running 'ANALYZE flights'
(62 rows)

Time: 15ms
```




