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

Then open a new Mac Terminal or PowerShell window and execute the following command to launch your single node database.
```
cockroach start-single-node --insecure --store=./data
```
Then open a browser to http://localhost:8080 to view the dashboard for your local cockroach instance


## Setup Query Analysis Tables
When we run workload tests we may want to compare results between runs or analyze the data from a particular run later.  Our CRDB internal tables offer great insights into contention events, transaction runtimes, statement statistics, etc.  However, these are in-memory structures and data will roll-off eventually, or if you restart your nodes the data is lost.  But we can create storage tables and offload information for specific runs and maintain that data over time.  And with real tables we can create an indexing strategy that will make it easier for us to query the data.  The recommendation is to load your test runs based on a "partition" identifier and use TTL to control when you want to cleanup old test runs.  We don't really need to maintain partitions since CRDB is already distributing our data evenly across ranges.  But we'll add a test_run identifier so we can easily segment our data when querying.  And by defualt we'll expire the data for a test run after 90 days.

First we'll store the connection string as a variable in our terminal shell window.  On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://localhost:26257/schedules?sslmode=disable"
```

For demonstration purposes I'm going to capture observability information from the following CRDB internal tables.
- transaction_contention_events
- cluster_execution_insights
- transaction_statistics
- statement_statistics

We can create the storage tables with TTL enabled and helper functions to load the tables by executing the following scripts.
```
cockroach sql --url "$conn_str" -f 00-query-analysis-tables.sql
cockroach sql --url "$conn_str" -f 01-query-analysis-functions.sql
```


## dbworkload
This is a tool we use to simulate data flowing into cockroach, developed by one of our colleagues with python.  We can install the tool with ```pip3 install "dbworkload[postgres]"```, and then add it to your path.  On Mac or Linux with Bash you can use:
```
echo -e '\nexport PATH=`python3 -m site --user-base`/bin:$PATH' >> ~/.bashrc 
source ~/.bashrc
```
For Windows you can add the location of the dbworkload.exe file (i.e. C:\Users\myname\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.9_abcdefghijk99\LocalCache\local-packages\Python39\Scripts) to your Windows Path environment variable.  The pip command above should provide the exact path to your local python executables.


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
num_connections=4
duration=60
schedule_freq=10
status_freq=90
inventory_freq=75
price_freq=25
contention_freq=10
batch_size=16
delay=100
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
run_name       Transactions.20250529_201257
start_time     2025-05-29 20:12:57
end_time       2025-05-29 21:12:58
test_duration  3601
-------------  ----------------------------

┌───────────┬────────────┬───────────┬───────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
│   elapsed │ id         │   threads │   tot_ops │   tot_ops/s │   mean(ms) │   p50(ms) │   p90(ms) │   p95(ms) │   p99(ms) │   max(ms) │
├───────────┼────────────┼───────────┼───────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
│     3,601 │ __cycle__  │         4 │    17,994 │           4 │     800.13 │    787.96 │  1,184.45 │  1,222.64 │  1,559.30 │  1,979.30 │
│     3,601 │ contention │         4 │    17,994 │           4 │      15.04 │      0.00 │     78.86 │    145.22 │    162.24 │    217.76 │
│     3,601 │ inventory  │         4 │    17,994 │           4 │     293.85 │    387.86 │    402.31 │    409.64 │    430.23 │    818.22 │
│     3,601 │ price      │         4 │    17,994 │           4 │      99.33 │      0.01 │    392.78 │    398.54 │    414.92 │    740.11 │
│     3,601 │ schedule   │         4 │    17,994 │           4 │      38.86 │      0.00 │    231.88 │    384.72 │    398.45 │    801.86 │
│     3,601 │ status     │         4 │    17,994 │           4 │     353.04 │    390.71 │    404.38 │    412.33 │    432.96 │    848.84 │
└───────────┴────────────┴───────────┴───────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘
```


## Data Collection
We can also get more grainular information on transaction and statememt statistics from the CRDB Admin Console, which are backed by our CRDB internal observability tables.  But if we export that data into our own tables then we'll be able to compare metrics between runs and measure performance improvements over time.  The following function call will capture the metrics for our latest run, which will include four tables for demonstration purposes to pull the contention records, query insights, statement and transaction statistics collected during the run.
```
cockroach sql --url "$conn_str" -e """
SELECT *
FROM copy_test_run_observations(
  20250330,      -- test run id, default to next incremental id
  '2025-05-29 20:00:00.000000+00'::TIMESTAMPTZ,      -- starting from, default to past 24 hours
  NULL           -- going until, default to now()
);
"""

  contention | insights | statements | transactions
-------------+----------+------------+---------------
           4 |      315 |        315 |          115
```

And then, for example, we can grab any exception in the workload's log output to investigate statements related to the failed transaction.
```
error_str=$(cat <<EOF
Error occurred: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError: write for key /Table/156/1/"C-O\xab\r\xdaM\v\x94\x01\xb03\nzߟ"/0 at timestamp 1748553143.702115000,0 too old; must write at or above 1748553143.702115000,2: "sql txn" meta={id=d039504e key=/Table/156/1/"C-O\xab\r\xdaM\v\x94\x01\xb03\nzߟ"/0 iso=Serializable pri=0.00553518 epo=0 ts=1748553143.702115000,2 min=1748553143.702115000,0 seq=1} lock=true stat=PENDING rts=1748553143.702115000,0 wto=false gul=1748553144.202115000,0 obs={n1@1748553143.702115000,0}
EOF
)

cockroach sql --url "$conn_str" -e """
SELECT * 
FROM inspect_contention_from_exception(
  '$$(echo "$error_str")$$',
  'Transactions',    -- app_name
  'schedules',       -- schema_name
  'same_app'         -- contention option
);
"""

  ord |  role   | status |         collection_ts         |     aggregated_ts      |   app_name   | database_name | schema_name | table_name | index_name |                           txn_metadata                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             txn_statistics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | contention_type | contention |   fingerprint_id   | transaction_fingerprint_id |     plan_hash      |                                                                                                                 stmt_metadata                                                                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                stmt_statistics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |         sampled_plan         | aggregation_interval |                                         index_recommendations
------+---------+--------+-------------------------------+------------------------+--------------+---------------+-------------+------------+------------+------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+------------+--------------------+----------------------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------+----------------------+---------------------------------------------------------------------------------------------------------
    1 | waiting | NULL   | NULL                          | 2025-05-29 21:00:00+00 | Transactions | NULL          | NULL        | NULL       | NULL       | {"stmtFingerprintIDs": ["2ab0c15b7b14e792", "67a10dfb99638ead"]} | {"execution_statistics": {"cnt": 5, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 646398.6, "sqDiff": 11524737185.2}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 260618, "sqDiff": 8011623920.000001}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 94, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 7E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 94, "sqDiff": 0}, "valueBytes": {"mean": 1996, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "statistics": {"bytesRead": {"mean": 3274.135363790186, "sqDiff": 629.1708967851387}, "cnt": 591, "commitLat": {"mean": 0.00558303270219966, "sqDiff": 0.0014154193429448854}, "idleLat": {"mean": 0.034576343846023684, "sqDiff": 0.11486110703191013}, "maxRetries": 0, "numRows": {"mean": 1.9796954314720812, "sqDiff": 11.756345177664977}, "retryLat": {"mean": 0, "sqDiff": 0}, "rowsRead": {"mean": 4E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "svcLat": {"mean": 0.04113040387986464, "sqDiff": 0.0901331795568695}}} | NULL            |     f      | \x2ab0c15b7b14e792 | \x1c0d24389254fc7a         | \x025144e18ceb2338 | {"db": "schedules", "distsql": false, "fullScan": true, "implicitTxn": false, "query": "SELECT * FROM airports WHERE city = _", "querySummary": "SELECT * FROM airports", "stmtType": "TypeDML", "vec": true}                                  | {"execution_statistics": {"cnt": 8, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 14441.75, "sqDiff": 53578727.5}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 2.048E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 126856.75, "sqDiff": 10685503205.500002}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 47, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 1, "sqDiff": 0}, "seekCountInternal": {"mean": 1, "sqDiff": 0}, "stepCount": {"mean": 35, "sqDiff": 0}, "stepCountInternal": {"mean": 47, "sqDiff": 0}, "valueBytes": {"mean": 997.75, "sqDiff": 3.4999999999999662}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": ["creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"], "statistics": {"bytesRead": {"mean": 1637.067681895093, "sqDiff": 157.29272419628467}, "cnt": 591, "failureCount": 0, "firstAttemptCnt": 591, "genericCount": 591, "idleLat": {"mean": 0.00020908501861252127, "sqDiff": 0.000006089591580494795}, "indexes": ["156@1"], "kvNodeIds": [1], "lastErrorCode": "", "lastExecAt": "2025-05-29T21:12:58.000765Z", "latencyInfo": {"max": 0.013574292, "min": 0.000432208}, "maxRetries": 0, "nodes": [1], "numRows": {"mean": 1, "sqDiff": 0}, "ovhLat": {"mean": 0.0000024722064297800332, "sqDiff": 2.92632892815572E-10}, "parseLat": {"mean": 0.000002392832487309645, "sqDiff": 4.1806406002416235E-8}, "planGists": ["AgG4AgIAHwAAAAMGCg=="], "planLat": {"mean": 0.00005162517597292724, "sqDiff": 6.899717806916985E-7}, "regions": [], "rowsRead": {"mean": 2E+1, "sqDiff": 0}, "rowsWritten": {"mean": 0, "sqDiff": 0}, "runLat": {"mean": 0.0006652432673434853, "sqDiff": 0.00019420337721665374}, "sqlType": "TypeDML", "svcLat": {"mean": 0.0007217334822335027, "sqDiff": 0.00019573682877965754}, "usedFollowerRead": false}}                                              | {"Children": [], "Name": ""} | 01:00:00             | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"}
    2 | waiting | failed | 2025-05-29 21:12:23.728207+00 | 2025-05-29 21:00:00+00 | Transactions | schedules     | schedules   | NULL       | NULL       | {"stmtFingerprintIDs": ["2ab0c15b7b14e792", "67a10dfb99638ead"]} | {"execution_statistics": {"cnt": 5, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 646398.6, "sqDiff": 11524737185.2}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 260618, "sqDiff": 8011623920.000001}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 94, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 2, "sqDiff": 0}, "seekCountInternal": {"mean": 2, "sqDiff": 0}, "stepCount": {"mean": 7E+1, "sqDiff": 0}, "stepCountInternal": {"mean": 94, "sqDiff": 0}, "valueBytes": {"mean": 1996, "sqDiff": 0}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "statistics": {"bytesRead": {"mean": 3274.135363790186, "sqDiff": 629.1708967851387}, "cnt": 591, "commitLat": {"mean": 0.00558303270219966, "sqDiff": 0.0014154193429448854}, "idleLat": {"mean": 0.034576343846023684, "sqDiff": 0.11486110703191013}, "maxRetries": 0, "numRows": {"mean": 1.9796954314720812, "sqDiff": 11.756345177664977}, "retryLat": {"mean": 0, "sqDiff": 0}, "rowsRead": {"mean": 4E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "svcLat": {"mean": 0.04113040387986464, "sqDiff": 0.0901331795568695}}} | NULL            |     f      | \x67a10dfb99638ead | \x1c0d24389254fc7a         | \xfc4bcd4933e6c787 | {"db": "schedules", "distsql": false, "fullScan": true, "implicitTxn": false, "query": "UPDATE airports SET country = _ WHERE city = _", "querySummary": "UPDATE airports SET country = _ WHERE city = _", "stmtType": "TypeDML", "vec": true} | {"execution_statistics": {"cnt": 8, "contentionTime": {"mean": 0, "sqDiff": 0}, "cpuSQLNanos": {"mean": 627036.5, "sqDiff": 13925259104.000004}, "maxDiskUsage": {"mean": 0, "sqDiff": 0}, "maxMemUsage": {"mean": 4.096E+4, "sqDiff": 0}, "mvccIteratorStats": {"blockBytes": {"mean": 135552.75, "sqDiff": 3827385413.4999995}, "blockBytesInCache": {"mean": 0, "sqDiff": 0}, "keyBytes": {"mean": 0, "sqDiff": 0}, "pointCount": {"mean": 47, "sqDiff": 0}, "pointsCoveredByRangeTombstones": {"mean": 0, "sqDiff": 0}, "rangeKeyContainedPoints": {"mean": 0, "sqDiff": 0}, "rangeKeyCount": {"mean": 0, "sqDiff": 0}, "rangeKeySkippedPoints": {"mean": 0, "sqDiff": 0}, "seekCount": {"mean": 1, "sqDiff": 0}, "seekCountInternal": {"mean": 1, "sqDiff": 0}, "stepCount": {"mean": 35, "sqDiff": 0}, "stepCountInternal": {"mean": 47, "sqDiff": 0}, "valueBytes": {"mean": 997.75, "sqDiff": 3.4999999999999662}}, "networkBytes": {"mean": 0, "sqDiff": 0}, "networkMsgs": {"mean": 0, "sqDiff": 0}}, "index_recommendations": ["creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"], "statistics": {"bytesRead": {"mean": 1637.067681895093, "sqDiff": 157.29272419628467}, "cnt": 591, "failureCount": 12, "firstAttemptCnt": 591, "genericCount": 591, "idleLat": {"mean": 0.03338678630626059, "sqDiff": 0.08815937714916616}, "indexes": ["156@1"], "kvNodeIds": [1], "lastErrorCode": "40001", "lastExecAt": "2025-05-29T21:12:58.000766Z", "latencyInfo": {"max": 0.052318125, "min": 0.00124225}, "maxRetries": 0, "nodes": [1], "numRows": {"mean": 0.9796954314720814, "sqDiff": 11.756345177664974}, "ovhLat": {"mean": 9.375414551607434E-7, "sqDiff": 3.4045222734348087E-11}, "parseLat": {"mean": 0.000003023688663282572, "sqDiff": 9.600118117271408E-8}, "planGists": ["AgG4AgIAHwAAAAMHDAUMIbgCAAA="], "planLat": {"mean": 0.00006055089678510999, "sqDiff": 0.0000022335368318887038}, "regions": [], "rowsRead": {"mean": 2E+1, "sqDiff": 0}, "rowsWritten": {"mean": 1, "sqDiff": 0}, "runLat": {"mean": 0.0016792890219966157, "sqDiff": 0.002866743281233353}, "sqlType": "TypeDML", "svcLat": {"mean": 0.0017438011489001686, "sqDiff": 0.002876205037837565}, "usedFollowerRead": false}} | {"Children": [], "Name": ""} | 01:00:00             | {"creation : CREATE INDEX ON schedules.public.airports (city) STORING (airport_code, name, country);"}
```

## Slow Performers
Now we can query the observability metrics for our last run to look for failures, retries, execution count, execution time, idle time, rows processed, amount of data read, amount of data written, almost anything you need.  Below is a simple example where we query for slow performers, relative to our workload.
```
cockroach sql --url "$conn_str" -e """
SELECT MAX(ROUND(CAST(statistics->'statistics'->'runLat'->'mean' AS FLOAT) * 1000, 2)) AS avg_ms,
       metadata->'query' AS QUERY
FROM test_run_statement_statistics
WHERE test_run = 20250330
  AND app_name = 'Transactions'
GROUP BY 2
ORDER BY 1 DESC
LIMIT 5;
"""

  avg_ms |                                                                                                                                                                            query
---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  266.92 | "SELECT flight_id FROM flights AS OF SYSTEM TIME follower_read_timestamp() ORDER BY random() LIMIT _"
   26.39 | "UPDATE flight_status SET status = (ARRAY[_, __more__])[_ + floor(random() * _)::INT8], updated_at = now() WHERE flight_id IN (_, __more__)"
   26.07 | "UPDATE seat_inventory SET seats_available = (seats_available::FLOAT8 * (_ + ((random() - _) / _)))::INT8, updated_at = now() WHERE flight_id IN (_, __more__)"
   25.36 | "UPDATE flight_prices SET price_usd = price_usd * (_ + ((random() - _) / _))::DECIMAL, updated_at = now() WHERE flight_id IN (_, __more__)"
    18.2 | "UPDATE flights SET scheduled_departure = scheduled_departure + (((CASE WHEN random() < _ THEN _ ELSE _ END) * (floor(random() * _) + _)::INT8) * _::INTERVAL), scheduled_arrival = scheduled_arrival + (((CASE WHEN random() < _ THEN _ ELSE _ END) * (floor(random() * _) + _)::INT8) * _::INTERVAL), updated_at = now() WHERE flight_id IN (_, __more__)"
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
--------------------------------------------------------------------------------------------------------------------------------------------------------

  planning time: 448µs
  execution time: 359ms
  distribution: local
  vectorized: true
  plan type: custom
  rows decoded from KV: 1,000,000 (144 MiB, 15 gRPC calls)
  cumulative time spent in KV: 345ms
  maximum memory usage: 10 MiB
  DistSQL network usage: 0 B (0 messages)
  sql cpu time: 101ms
  isolation level: serializable
  priority: normal
  quality of service: regular
  historical: AS OF SYSTEM TIME 2025-05-30 00:26:16.292396

  • top-k
  │ sql nodes: n1
  │ actual row count: 16
  │ execution time: 14ms
  │ estimated max memory allocated: 10 KiB
  │ sql cpu time: 14ms
  │ estimated row count: 16
  │ order: +column13
  │ k: 16
  │
  └── • render
      │
      └── • scan
            sql nodes: n1
            kv nodes: n1
            actual row count: 1,000,000
            KV time: 345ms
            KV rows decoded: 1,000,000
            KV bytes read: 144 MiB
            KV gRPC calls: 15
            estimated max memory allocated: 10 MiB
            sql cpu time: 87ms
            estimated row count: 1,000,001 (100% of the table; stats collected 4 hours ago; using stats forecast for 2 days in the future)
            table: flights@flights_pkey
            spans: FULL SCAN
(40 rows)

Time: 361ms
```

There's not an index that will help with this use case.  So our objective would be to find another strategy to get similar results without a full table scan.  Below uses a random offset based on a slice of the overall table data, rather than pulling a million records from disk.  Same outcome, but only needs to decode ~20k records in about 7% of the execution time.
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
  planning time: 556µs
  execution time: 25ms
  distribution: local
  vectorized: true
  plan type: custom
  rows decoded from KV: 20,488 (2.9 MiB, 1 gRPC calls)
  cumulative time spent in KV: 6ms
  maximum memory usage: 3.0 MiB
  DistSQL network usage: 0 B (0 messages)
  sql cpu time: 2ms
  isolation level: serializable
  priority: normal
  quality of service: regular
  historical: AS OF SYSTEM TIME 2025-05-30 01:00:55.571558

  • root
  │
  ├── • limit
  │   │ count: 16
  │   │
  │   └── • limit
  │       │ offset: floor(random() * @S1)::INT8
  │       │
  │       └── • scan
  │             sql nodes: n1
  │             kv nodes: n1
  │             actual row count: 16
  │             KV time: 6ms
  │             KV rows decoded: 20,488
  │             KV bytes read: 2.9 MiB
  │             KV gRPC calls: 1
  │             estimated max memory allocated: 3.0 MiB
  │             sql cpu time: 1ms
  │             estimated row count: 1,000,000 (100% of the table; stats collected 15 minutes ago)
  │             table: flights@flights_pkey  ----------------------  WARNING: the row count estimate is inaccurate, consider running 'ANALYZE flights'
  │             spans: FULL SCAN
  │
  └── • subquery
      │ id: @S1
      │ original sql: (SELECT (estimated_row_count / 10)::FLOAT8 AS flights FROM crdb_internal.table_row_statistics WHERE table_name = 'flights')
      │ exec mode: one row
      │
      └── • max1row
          │ sql nodes: n1
          │ actual row count: 1
          │ execution time: 58µs
          │ sql cpu time: 58µs
          │ estimated row count: 1
          │
          └── • render
              │
              └── • filter
                  │ sql nodes: n1
                  │ actual row count: 1
                  │ execution time: 21µs
                  │ sql cpu time: 20µs
                  │ filter: table_name = 'flights'
                  │
                  └── • virtual table
                        sql nodes: n1
                        actual row count: 341
                        execution time: 18ms
                        sql cpu time: 184µs
                        table: table_row_statistics@primary

(66 rows)

Time: 26ms
```




