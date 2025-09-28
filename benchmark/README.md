# pgqrs-postgres-queue-benchmarks

This subproject contains a reproducible benchmark harness to compare three Postgres queue scenarios:

- Logged table without `FOR UPDATE SKIP LOCKED` (naive)
- Logged table with `FOR UPDATE SKIP LOCKED`
- Unlogged table with `FOR UPDATE SKIP LOCKED`

## What’s included

* `docker-compose.yml` — runs Postgres in Docker.
* `init_tables.sql` — creates three tables: queue_logged_naive, queue_logged_skiplocked, queue_unlogged_skiplocked.
* `locustfile.py` — Locust workload with producer and consumer users. Consumers use either the naive pattern or the FOR UPDATE SKIP LOCKED pattern depending on the SCENARIO env var.
* `run_benchmark.sh` — wrapper to run a headless Locust session and collect CSVs into results/.
* `scripts/wait-for-postgres.sh` — helper to wait for the DB to be ready.
* `requirements.txt` — Python deps (Locust, psycopg2).


### Setup Python Environment
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run the benchmark
```
SCENARIO=queue_logged_skiplocked docker-compose down -v && docker-compose up -d && ./scripts/wait-for-postgres.sh && psql -h localhost -U pgbench -f init_tables.sql && python locustfile.py

BATCH_SIZE=1 SCENARIO=queue_logged_skiplocked ./run_benchmark.sh 40 5 2m
```

## Performance Benchmark Report

### Environment and hardware

* Laptop: MacBook Pro M2 (single machine test).
* Postgres: Docker container running Postgres 15 on the laptop.
* Workload driver: Locust running in a Python venv using psycopg2 (greenlet-friendly via psycogreen where installed).
* Producers and consumers used in these runs: 22 producers, 23 consumers.

Each Locust "request" is a claim attempt; messages processed = requests * batch.

Each run produced Locust CSV files with request counts, median latency, and average requests/s, which were used to compute messages processed and approximate messages per second.

Benchmark code is available at

### Parameters

Three different scenarios were created:
* `queue_logged_naive`: Logged table. Consumer implements naive SELECT then per-row UPDATE claim logic (this is the pattern that causes contention).
* `queue_logged_skiplocked`: Logged table. Consumer uses WITH ... SELECT ... FOR UPDATE SKIP LOCKED + UPDATE ... FROM cte to atomically claim rows.
* `queue_unlogged_skiplocked`: UNLOGGED table. Same consumer pattern as skip-locked. No WAL writes for DML.

Two batch sizes were measured:
* batch = 1
* batch = 10.

### `query_logged_naive` and `query_logged_skiplocked`

The following measures are used for the comparison:
* Throughput: messages processed (and approximate messages/sec). This shows how many messages the system completed during each run.
* Latency: median request latency (ms). This shows per-claim cost.


|scenario|batch|requests|messages processed|median latency (ms)|
|--------|-----|--------|------------------|-------------------|
|queue_logged_naive|1|33,260|33,260|1.0|
|queue_logged_skiplocked|1|36,741|36,741|1.0|
|queue_logged_naive|10|14,829|148,290|5.0|
|queue_logged_skiplocked|10|19,039|190,390|3.0|


#### Skip-locked reduces contention and raises throughput
With batch = 10 the skip-locked pattern processed 190,390 messages versus 148,290 for naive, an increase of about **+28%**.
With batch = 1 the improvement is smaller, about +10% (36,741 vs 33,260). When batch is small, the claim overhead is larger per message, so the contention benefit is smaller but still material.

#### Skip-locked reduces median latency under load
Median request latency dropped from 5 ms (naive, batch=10) to 3 ms (skip-locked, batch=10). Lower per-request latency is consistent with less retry/blocking when consumers claim rows.

#### Batching amplifies effects
Batching (batch=10) increases overall messages processed during the same run window. Fewer claim operations per message means less overhead. The combination of skip-locked plus batching produces the highest throughput in these runs.

### Logged vs UNLOGGED (WAL comparison)

This section compares how much WAL Postgres generates when a queue claims messages.

The workload is the delete-on-claim pattern, which is representative of many queue implementations where consumers remove rows after processing.

WAL usage was measured by:
* Record a start and end WAL LSN using `SELECT pg_current_wal_lsn();`
* WAL bytes written using `SELECT pg_wal_lsn_diff(end_lsn, start_lsn);`


| scenario|batch|messages|processed	WAL bytes|
|---------|-----|--------|-------------------|
|queue_logged_skiplocked|1|41,670|92,563,504|
|queue_unlogged_skiplocked|1|41,068|4,114,952|
|queue_logged_skiplocked|10|344,150|117,056,432|
|queue_unlogged_skiplocked|10|364,510|3,842,688|

For easier comparison, the table below shows WAL bytes per 1,000,000 messages:

| scenario|batch|WAL bytes per 1M messages|
|---------|-----|--------|
|queue_logged_skiplocked|1|2,221,346,388|
|queue_unlogged_skiplocked|1|100,198,500|
|queue_logged_skiplocked|10|340,132,012|
|queue_unlogged_skiplocked|10|10,542,065|

#### UNLOGGED cuts WAL by orders of magnitude

The unlogged runs generated dramatically less WAL than the logged runs. For batch 10 the logged table produced about 340 million WAL bytes per 1M messages, while the unlogged table produced about 10.5 million WAL bytes per 1M messages. That is roughly a **32x** reduction in WAL at batch 10. At batch 1 the reduction is even larger.

#### Deleting rows is WAL-heavy
Deleting a row changes the heap and the index. Those changes are WAL-logged for durability and for correctness of the MVCC index structures. The delete-on-claim pattern therefore produces a lot of WAL for logged tables. If your queue instead used tiny in-place updates that can be HOT updates, WAL can be much smaller.

#### Batch size affects WAL per message
The logged batch-10 run had lower WAL per message than logged batch-1 in this data set. That can happen because batching reduces per-message overhead and amortizes index changes. Choose batch size based on the trade-off between throughput, latency, and how much work a consumer should take at once.
