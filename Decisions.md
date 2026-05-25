# NYC Taxi Medallion Pipeline

This file records every non-obvious design decision made in this project.
Each decision explains what was chosen, what the alternatives were, and
exactly why this option was picked. The goal is to show that nothing here
was accidental.

---

## Decision 1 — partitionBy("year", "month") not just ("month")

**Choice:** Both Bronze and Silver tables are partitioned by `year` and
`month` together as a composite partition key.

**Alternatives considered:**
- Partition by `month` only
- Partition by `trip_date` (daily partitions)
- No partitioning at all

**Why year + month:**

Partitioning only by `month` creates ambiguous folder paths on disk:

```
/month=01/   ← January 2023 and January 2024 are in the same folder
/month=02/   ← February 2023 and February 2024 are mixed together
```

A query with `WHERE month = 1 AND year = 2024` cannot prune the
`/month=01/` partition because year is not a partition column — Spark
must scan the entire partition and filter in memory. With year + month:

```
/year=2024/month=01/   ← only January 2024, ~50MB
/year=2023/month=01/   ← only January 2023, ~50MB
```

Both dimensions can be pruned independently. A query for January 2024
reads only ~50MB instead of ~100MB.

**Why not daily partitions:**
Daily partitions on this dataset would produce ~90 folders for 3 months,
each holding ~1.6M rows (~5MB). This causes the small file problem —
each Parquet file in the partition is too small to be read efficiently
by Spark's vectorized reader. Monthly partitions keep file sizes in the
50–150MB range, which is the Delta Lake recommended sweet spot.

**Why not no partitioning:**
Without partitions every query scans the full table. Acceptable at 150MB
today but becomes expensive when the dataset grows to 12+ months.
Partitioning is a one-time setup that scales indefinitely.

---

## Decision 2 — trigger(availableNow=True) not a continuous stream

**Choice:** Both Bronze and Silver `writeStream` calls use
`.trigger(availableNow=True)`.

**Alternatives considered:**
- `.trigger(processingTime="1 hour")` — continuous micro-batch
- `.trigger(once=True)` — legacy single-batch mode
- No trigger option — continuous streaming

**Why availableNow=True:**

Databricks Community Edition clusters auto-terminate after 2 hours of
inactivity. A continuous stream that runs indefinitely would be killed
by the cluster termination, leaving the checkpoint in an intermediate
state. Restarting the cluster and resuming the stream works correctly
because of the checkpoint, but wastes time and compute quota.

`availableNow=True` processes every file currently in the landing zone,
commits the checkpoint cleanly, then stops the stream. The cluster can
then terminate normally. The next run picks up exactly where it left off.

This also mirrors a real production pattern. Many pipelines at scale
run on a schedule — hourly or daily — rather than as a continuous stream.
This is not a workaround for a Community Edition limitation. It is the
correct architecture for a batch-oriented use case like monthly taxi data.

**Why not trigger(once=True):**
`trigger(once=True)` is deprecated as of Spark 3.4. `availableNow=True`
is the correct modern replacement. It also processes data faster than
`once=True` because it uses multiple micro-batches internally rather than
one giant batch.

---

## Decision 3 — foreachBatch in Silver instead of direct writeStream

**Choice:** The Silver transformation is wrapped inside `.foreachBatch()`
rather than chained directly on the streaming DataFrame.

**Alternatives considered:**
- Chain `.filter()`, `.withColumn()`, `.dropDuplicates()` directly on
  the streaming DataFrame and write with `.writeStream`
- Use Delta Live Tables instead of manual foreachBatch

**Why foreachBatch:**

The Structured Streaming API has a constraint: `dropDuplicates()` on a
streaming DataFrame only deduplicates within the current micro-batch.
It does not check whether a duplicate already exists in the Delta table
from a previous micro-batch.

This matters for the taxi dataset because the same file can be partially
re-ingested if Auto Loader replays a batch after a failure. Without
cross-batch deduplication, rows from the replayed file would appear twice
in Silver.

`foreachBatch` converts each micro-batch into a static DataFrame before
the write. On a static DataFrame, `dropDuplicates(["VendorID",
"pickup_datetime", "dropoff_datetime"])` checks the full batch for
duplicates correctly. A MERGE INTO on the Delta table then ensures
no row from any previous batch is written again.

The trade-off is slightly more verbose code. The benefit is correct
exactly-once semantics even across batch replays.

**Why not Delta Live Tables:**
DLT is the right answer in a production workspace with a paid plan.
On Community Edition, DLT pipelines cannot be run as scheduled jobs
and have limited support. The foreachBatch pattern is functionally
equivalent and runs on any Databricks tier.

---

## Decision 4 — Great Expectations alongside Delta constraints

**Choice:** Both Delta NOT NULL constraints and a Great Expectations
validation suite are used. Neither replaces the other.

**Alternatives considered:**
- Delta constraints only — remove Great Expectations
- Great Expectations only — remove Delta constraints
- Custom PySpark assertion functions instead of GE

**Why both:**

Delta constraints and Great Expectations solve different problems and
catch different categories of bad data.

Delta constraints are enforced at write time by the Delta transaction
log. If a row violates `fare_amount > 0`, the entire write transaction
is rolled back and the job fails with a `DeltaInvariantViolationException`.
This is the right behaviour for a structural rule that must never be
broken. The downside is that constraints are blunt — they cannot express
statistical or relational rules.

Great Expectations runs after the write as a validation layer. It can
check things that Delta constraints cannot:

- "The row count must be between 100,000 and 10,000,000" — catches a
  complete pipeline failure that wrote zero rows (which Delta would not
  catch because zero rows violates no constraints).
- "The average fare_amount must be between 5 and 100" — catches a unit
  conversion bug that turned dollars into cents.
- "The null rate in pickup_datetime must be below 0.1%" — catches
  upstream data quality degradation over time.

Running both gives defense in depth. Delta catches hard structural
violations at write time. Great Expectations catches soft statistical
anomalies after the write. Neither tool alone is sufficient.

---

## Decision 5 — schemaEvolutionMode = "rescue" in Auto Loader

**Choice:** Auto Loader is configured with
`.option("cloudFiles.schemaEvolutionMode", "rescue")`.

**Alternatives considered:**
- `failOnNewColumns` — fail the stream if the schema changes
- `addNewColumns` — automatically add new columns to Bronze
- `none` — ignore schema changes entirely

**Why rescue:**

Bronze is a raw archive layer. Its job is to preserve every byte that
arrives from the source, forever, without losing any data. Any mode that
rejects data (failOnNewColumns) or silently ignores new columns (none)
violates this principle.

The NYC TLC dataset is a real example: a `cbd_congestion_fee` column was
added in January 2025 when New York City introduced congestion pricing.
A pipeline using `failOnNewColumns` would break on that date and stop
ingesting data until manually fixed — losing days of records. A pipeline
using `addNewColumns` would silently change the Bronze schema, breaking
downstream Silver queries that expect a fixed set of columns.

`rescue` saves unexpected columns as a JSON string in a `_rescued_data`
column. The Bronze table never fails. The Silver layer can inspect
`_rescued_data` and decide what to do with the new column deliberately,
with human review, rather than automatically. Schema strictness is the
Silver layer's responsibility, not Bronze's.

---

## Decision 6 — Separate checkpointLocation and schemaLocation paths

**Choice:** Auto Loader uses two distinct DBFS paths:

```
checkpointLocation  →  dbfs:/taxi_project/checkpoints/bronze/
schemaLocation      →  dbfs:/taxi_project/schemas/bronze/
```

**Alternatives considered:**
- One shared path for both checkpoint and schema
- Store both inside the Bronze Delta table folder

**Why separate paths:**

Auto Loader writes fundamentally different internal structures to each
location. The checkpoint contains Spark streaming offset files and commit
logs that track which files have been processed. The schema location
contains JSON files describing the inferred Parquet column types.

If both point to the same folder, Auto Loader's internal file readers
mistake schema JSON files for offset files and vice versa. This produces
one of two failure modes: schema inference resets on every run (the
pipeline reprocesses all files from the start), or the checkpoint reader
throws a parse error and the stream fails to start.

Keeping them separate costs nothing and prevents an entire class of
hard-to-debug failures.

**Why not inside the Delta table folder:**
The Delta table folder (`_delta_log/`) is managed exclusively by the
Delta transaction protocol. Writing external files into it risks
corrupting the transaction log if Delta's internal cleanup (VACUUM)
deletes files it does not recognise.

---

## Decision 7 — _ingested_at and _source_file audit columns

**Alternatives considered:**
- No audit columns — write only the source data
- Store audit metadata in a separate audit table

**Why inline audit columns:**

These columns have no business value but high operational value.

`_ingested_at` answers: when did this data arrive in our system? This
is essential for SLA monitoring (did the January batch arrive before
the 6am deadline?) and for debugging late-arriving data (this record
has a pickup_datetime of December but arrived in February — why?).

`_source_file` answers: which source file did this specific row come
from? When a data quality issue surfaces in Gold three weeks after
ingestion, `_source_file` pinpoints the exact Parquet file in seconds
instead of requiring a full table scan to identify the bad batch.

Storing audit metadata inline rather than in a separate table means
the lineage travels with the data through all joins and aggregations.
A Gold row can be traced back through Silver to Bronze to the exact
source file with a single join on `_source_file`.

The underscore prefix is a convention that marks these as pipeline
infrastructure columns, not business columns. Downstream consumers
know to drop them before exposing data to business users.

---

## Decision 8 — availableNow=True makes the pipeline idempotent

**Choice:** Every notebook can be re-run safely without creating
duplicate data or breaking downstream tables.

**How this is achieved:**

Bronze write uses `mode("append")` with Auto Loader checkpoint — the
checkpoint guarantees each source file is processed exactly once even
across retries and re-runs.

Silver write uses `foreachBatch` with `dropDuplicates` followed by a
MERGE INTO — duplicate source rows are dropped before the merge, and
the merge itself is a conditional upsert that never inserts a row that
already exists.

Gold writes use `mode("overwrite")` — Gold tables are fully recomputed
from Silver on each run, so re-running produces the same result every
time.

This idempotency means the pipeline can be run on a schedule without
any manual state management between runs.