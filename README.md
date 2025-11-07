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
- transaction_statistics
- statement_statistics

First we'll store the connection string as a variable in our terminal shell window.  On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://localhost:26257/schedules?sslmode=disable"
```

We can create the storage tables with TTL enabled and helper functions to load the tables by executing the following scripts based on your version of CRDB.
```
cockroach sql --url "$conn_str" -f 01-query-analysis-tables.sql

-- and if you want to use a cron job
cockroach sql --url "$conn_str" -f 02a-copy-obs-function.sql
-- OR leverage a python client to avoid potential contention
pip install "psycopg[binary]"
export CRDB_DSN="postgresql://root@localhost:26257/schedules?sslmode=disable"
python copy_obs_data.py
-- OR if you want to leverage triggers v24.3+
cockroach sql --url "$conn_str" -f 02b-copy-obs-triggers.sql

-- AND either v24
cockroach sql --url "$conn_str" -f 03-v24-query-analysis-procedure.sql
-- OR v25 script
cockroach sql --url "$conn_str" -f 03-v25-query-analysis-function.sql
```

If you're on a version <24.3 or you want to leverage a scheduled job to copy the observability metrics (instead of triggers) then you can setup a cron job to update your physical observability tables periodically by adding a similar line below to ```crontab -e```.
```
* * * * * /opt/homebrew/bin/cockroach sql --url "postgresql://localhost:26257/schedules?sslmode=disable" -e="SELECT workload_test.copy_test_run_observations();" >> /var/log/crdb/copy_test_run_observations.log 2>&1
```
OR if uisng the python client to capture observability metrics, first setup your script on the server
```
sudo mkdir -p /usr/local/bin /var/log/crdb
sudo cp copy_obs_data.py /usr/local/bin/copy_obs_data.py
sudo cp run_copy_obs_data.sh /usr/local/bin/run_copy_obs_data.sh
sudo chmod +x /usr/local/bin/copy_obs_data.py
/opt/homebrew/bin/pip3 install "psycopg[binary]"
sudo sh -c 'cat >/etc/copy_obs_data.env' <<'EOF'
CRDB_DSN=postgresql://root@localhost:26257/schedules?sslmode=disable
EOF
sudo chmod 600 /etc/copy_obs_data.env
```
Then update ```crontab -e``` with a line similar to below.
```
* * * * * /usr/local/bin/run_copy_obs_data.sh
```

You may also want to setup a log directory and configure log rotation
```
sudo mkdir -p /var/log/crdb
sudo chown $(whoami) /var/log/crdb
brew install logrotate
sudo tee /opt/homebrew/etc/logrotate.d/copy_test_run_observations << 'EOF'
/var/log/crdb/copy_test_run_observations.log {
    # don’t complain if the file is missing
    missingok
    # skip rotation if the file is empty
    notifempty
    # gzip old logs to save space
    compress
    # compress from the *previous* rotation, not immediately
    delaycompress
    # copy & truncate the live file so cron can keep writing
    copytruncate
    # keep 4 old logs before deleting
    rotate 4
    # rotate once a day
    daily
    # append YYYYMMDD to rotated names
    dateext
}
EOF
```
And then check the status with ```grep cron /var/log/system.log```, ```cat /var/mail/username``` and ```tail -100f /var/log/crdb/copy_test_run_observations.log```


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
  'load_test_2025_06_18',            -- your test_run name
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
run_name       Transactions.20250619_012455
start_time     2025-06-19 01:24:55
end_time       2025-06-19 02:24:57
test_duration  3602
-------------  ----------------------------

┌───────────┬────────────┬───────────┬───────────┬─────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
│   elapsed │ id         │   threads │   tot_ops │   tot_ops/s │   mean(ms) │   p50(ms) │   p90(ms) │   p95(ms) │   p99(ms) │   max(ms) │
├───────────┼────────────┼───────────┼───────────┼─────────────┼────────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
│     3,602 │ __cycle__  │        16 │    43,000 │          11 │   1,338.95 │  1,326.15 │  2,023.24 │  2,135.07 │  2,660.49 │  4,448.02 │
│     3,602 │ contention │        16 │    43,000 │          11 │       7.07 │      0.00 │     30.23 │     68.95 │     91.20 │    403.01 │
│     3,602 │ inventory  │        16 │    43,000 │          11 │     498.48 │    629.15 │    735.37 │    770.45 │    848.55 │  2,699.09 │
│     3,602 │ price      │        16 │    43,000 │          11 │     166.03 │      0.01 │    672.53 │    715.39 │    798.01 │  2,778.22 │
│     3,602 │ schedule   │        16 │    43,000 │          11 │      68.00 │      0.01 │    309.03 │    665.77 │    760.33 │  2,029.83 │
│     3,602 │ status     │        16 │    43,000 │          11 │     599.36 │    646.98 │    746.58 │    780.90 │    859.79 │  2,504.82 │
└───────────┴────────────┴───────────┴───────────┴─────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘
```


## Data Collection
We can also get more grainular information on transaction and statememt statistics from the CRDB Admin Console, which are backed by our CRDB internal observability tables.  And since we're exporting the data into our own physical tables can compare metrics between runs and measure performance improvements over time.  If you're not using triggers or a cron job to schedule copies then the following function call can be used to capture the metrics for any actively running tests.  This will include four tables for demonstration purposes to pull the contention records, query insights, statement and transaction statistics collected during the run.
```
cockroach sql --url "$conn_str" -e """
SELECT workload_test.copy_test_run_observations();
"""

                           copy_test_run_observations
--------------------------------------------------------------------------------
  {"contention": 81, "insights": 6317, "statements": 894, "transactions": 470}
```

And then, as an example, we can grab any exception in the workload's log output to investigate statements related to a failed transaction.
```
error_str=$(cat <<EOF
Error occurred: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError: write for key /Table/116/1/"\xb2\xe35\x96^\xd0EL\xb8R\vC_J\xf1;"/0 at timestamp 1750299870.665242000,0 too old; must write at or above 1750299870.665242000,2: "sql txn" meta={id=f50e63f3 key=/Table/116/1/"\xb2\xe35\x96^\xd0EL\xb8R\vC_J\xf1;"/0 iso=Serializable pri=0.00809748 epo=0 ts=1750299870.665242000,2 min=1750299870.665242000,0 seq=1} lock=true stat=PENDING rts=1750299870.665242000,0 wto=false gul=1750299871.165242000,0
EOF
)
```

and either v24
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

or v25 script
```
cockroach sql --url "$conn_str" -e """
SELECT * 
FROM workload_test.inspect_contention_from_exception(
  '$$(echo "$error_str")$$',
  'Transactions',    -- in app_name
  'public',          -- in schema_name
  'same_app'         -- in contention option
);
"""

...tbd...
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




