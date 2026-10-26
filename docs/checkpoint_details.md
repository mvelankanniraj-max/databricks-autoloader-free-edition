Your screenshot shows that Databricks Auto Loader has successfully created a Structured Streaming checkpoint directory in your Unity Catalog Volume.

This is a good sign! It indicates that your Auto Loader streaming query has initialized its checkpoint and is tracking its progress.

Let's understand each folder, why Databricks creates it, and how it is used when new files arrive.

## 1. Understanding your Auto Loader checkpoint folder

Your checkpoint location is:

```
/Volumes/workspace/autoloader_demo/autoloader_files/checkpoints/orders
```

The folder structure shown in your screenshot is:

orders/ Checkpoint directory

metadata

Streaming query metadata

commits/

Completed streaming batches

offsets/

Source progress and batch offsets

sources/

Auto Loader file-discovery state

## 2. Explanation of each folder

### A. `metadata` — Streaming query information

This file contains metadata identifying the streaming query, including its unique query ID.

Example (illustrative):

```
{
  "id": "b3a8d7e2-1234-5678-9abc-123456789abc"
}
```

Purpose: Helps Spark identify and recover the streaming query when it restarts.

### B. `commits/` — Successfully completed batches

This folder records which micro-batches have completed successfully.

Example:

```
commits/
    0
    1
    2
    3
```

Each numbered file represents a committed streaming batch.

For example:

| Batch | Meaning           |
| ----- | ----------------- |
| 0     | Batch 0 completed |
| 1     | Batch 1 completed |
| 2     | Batch 2 completed |
| 3     | Batch 3 completed |

Why important? When Auto Loader restarts, Spark uses the commit log to determine which batches were completed.

### C. `offsets/` — Tracks streaming progress

This folder records the source offsets associated with each micro-batch.

Example:

```
offsets/
    0
    1
    2
    3
```

The offsets help Spark determine the input progress recorded for each batch.

Difference between offsets and commits:

- `offsets/1` means Spark has recorded the source progress for batch 1.
- `commits/1` means Spark has successfully completed batch 1.

If an offset exists but the corresponding commit does not, Spark can attempt to reprocess that batch after recovery.

### D. `sources/` — Tracks discovered source files

This is especially important for Auto Loader.

A typical directory structure is:

```
sources/
    0/
       ...
```

Auto Loader maintains file-discovery state here, including information about which source files have been discovered and processed.

For example, suppose your input folder contains:

```
input/orders/
    orders_001.csv
    orders_002.csv
    orders_003.csv
```

Auto Loader uses its checkpoint state to avoid treating these files as new inputs every time the streaming job restarts.

Important: The source state is not necessarily a simple readable list of filenames. Databricks manages it internally.

## 3. How all these folders work together

Source CSV files

orders_001.csv, orders_002.csv

Databricks Auto Loader

cloudFiles streaming source

Checkpoint directory

metadata

Query identity

sources/

Discovered files

offsets/

Batch progress

commits/

Completed batches

Bronze Delta table

New records written to target

## 4. Example: What happens when Auto Loader runs?

Assume you execute this PySpark code:

```
from pyspark.sql.functions import current_timestampsource_path = "/Volumes/workspace/autoloader_demo/autoloader_files/input/orders"checkpoint_path = "/Volumes/workspace/autoloader_demo/autoloader_files/checkpoints/orders"df = (    spark.readStream    .format("cloudFiles")    .option("cloudFiles.format", "csv")    .option("header", "true")    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")    .load(source_path))query = (    df.writeStream    .format("delta")    .option("checkpointLocation", checkpoint_path)    .trigger(availableNow=True)    .toTable("workspace.autoloader_demo.bronze_orders"))query.awaitTermination()
```

This assumes the source folder exists, CSV schema inference is supported in your configuration, and the destination table can be created. A separate schema directory is often preferable for organization.

### First execution

Suppose the input contains 3 CSV files.

Auto Loader discovers the files, processes their records, writes them to the Bronze Delta table, and updates its checkpoint.

### Second execution

Suppose you add:

```
orders_004.csv
orders_005.csv
```

When the same query runs again with the same checkpoint, Auto Loader uses its saved progress to identify the new files.

It processes the 2 newly added files instead of reprocessing the original 3.

This is one of the major advantages of Auto Loader.

## 5. Can you delete the checkpoint folder?

Do not delete it while the streaming query is running.

If you delete the checkpoint and restart the job, Auto Loader will generally treat the run as a new stream and may reprocess existing source files.

That could create duplicate records in your Bronze table if you are appending to the same target.

Checkpointing supports fault-tolerant recovery and, together with a compatible Delta sink, helps provide exactly-once processing behavior.

## 6. How can you inspect Auto Loader's file processing status?

Databricks provides the `cloud_files_state` SQL table-valued function.

Try this SQL:

```
SELECT
    path,
    discovery_time,
    processed_time,
    commit_time
FROM cloud_files_state(
    '/Volumes/workspace/autoloader_demo/autoloader_files/checkpoints/orders'
);
```

This lets you inspect the file-discovery and processing state maintained by Auto Loader, where supported by your Databricks runtime.

You can also check your Bronze table:

```
SELECT COUNT(*)
FROM workspace.autoloader_demo.bronze_orders;
```

## 7. Important interview question

Question: How does Databricks Auto Loader prevent processing the same file multiple times?

Sample answer:

"Databricks Auto Loader uses Structured Streaming checkpoints to persist file-discovery state, source offsets, and completed micro-batches. When the pipeline restarts, Auto Loader uses the checkpoint to resume processing from its previously recorded progress rather than starting from scratch. Combined with Delta Lake's transactional writes, this supports reliable, fault-tolerant ingestion and exactly-once processing semantics under the supported configuration."

One important distinction: The checkpoint folder is not your Bronze table and does not contain your actual ingested business records. It stores operational state needed to manage streaming progress and recovery.


Databricks Auto Loader supports parallel processing by leveraging Apache Spark's distributed computing architecture. It can discover thousands or millions of incoming files and process multiple files simultaneously across Spark executors.

The important distinction is that Auto Loader discovers and tracks files, while Spark executes the data processing in parallel.

## 1. How parallel execution works in Auto Loader

Imagine you have 1,000 CSV files containing 10 million order records.

Source: Unity Catalog Volume

1,000 CSV files · 10 million records

Auto Loader — File Discovery

Tracks new files and prepares micro-batches

Spark distributed execution

Worker 1

Parallel tasks

Worker 2

Parallel tasks

Worker 3

Parallel tasks

Worker 4

Parallel tasks

Each worker executes multiple tasks, subject to available CPU cores

Bronze Delta Table

Distributed writes with Delta transactions

Spark does not necessarily assign one file to one worker. Small files can be grouped into partitions, and large splittable files can be processed using multiple tasks.

## 2. Example of parallel execution

Assume your Auto Loader receives 1,000 files.

For illustration, your cluster has four workers, each with four available CPU cores.

# 4

Workers

# 16

Concurrent task slots

# 1,000

Incoming files

Spark can execute approximately 16 single-core tasks at once in this simplified configuration. As tasks finish, new tasks are scheduled until the batch completes.

This is task-level parallelism, not necessarily 16 files at a time.

## 3. How do you control parallel processing?

There are three important controls.

| Configuration                   | Purpose                                                                    |
| ------------------------------- | -------------------------------------------------------------------------- |
| `cloudFiles.maxFilesPerTrigger` | Soft limit on new files per micro-batch                                    |
| `cloudFiles.maxBytesPerTrigger` | Soft limit on input bytes per micro-batch                                  |
| `spark.sql.shuffle.partitions`  | Number of partitions for shuffle operations such as joins and aggregations |

For example:

```
df = (    spark.readStream    .format("cloudFiles")    .option("cloudFiles.format", "csv")    .option("cloudFiles.maxFilesPerTrigger", 100)    .load(source_path))
```

This asks Auto Loader to process approximately 100 new files per micro-batch, subject to its admission controls.

It does not mean 100 files will execute simultaneously. The actual parallelism depends on Spark partitions, executor resources, and the operations performed.

## 4. What happens to checkpoints during parallel execution?

This connects directly to the `commits`, `offsets`, and `sources` folders in your screenshot.

Suppose 100 files are included in micro-batch 0.

1. Auto Loader identifies the files using its discovery state.
2. Spark records the batch's source offsets in `offsets/0`.
3. Spark distributes the input partitions across workers.
4. Workers process their assigned partitions in parallel.
5. Spark completes the Delta write and records the successful batch in `commits/0`.

If a worker fails, Spark can retry its task. If the entire streaming query fails before the batch is committed, the checkpoint enables recovery and replay.

## 5. Interactive example: Batch size versus parallelism

Change the settings below to see how the number of batches and concurrent tasks differ.

Incoming files

1,000

Max files per micro-batch

100

Workers (4 cores each)

4

Estimated micro-batches

# 10

Concurrent task slots

# 16

Illustrative task-slot allocation

1

2

3

4

5

6

7

8

9

10

11

12

13

14

15

16

Illustrative model assuming four usable task slots per worker, full file batches, and no byte-limit constraints. Actual task counts and batch sizes may differ.

## 6. Can two Auto Loader jobs run in parallel?

Yes. For example, you can run two separate streaming queries:

- Job 1: `orders` CSV files → `bronze_orders`
- Job 2: `customers` CSV files → `bronze_customers`

Each should have its own checkpoint directory:

```
checkpoints/
    orders/
    customers/
```

Never use the same checkpoint directory for two independently running streaming queries. Their progress and recovery state must remain isolated.

Also, avoid running two concurrent instances of the same streaming query against the same checkpoint. Use one active query and scale its Spark resources or partitions instead.

## 7. How to check actual parallelism in Databricks

After running your Auto Loader notebook:

1. Open the Spark UI for the compute running the query.
2. Go to the Stages tab.
3. Inspect the number of tasks for each stage.
4. Check how many tasks execute concurrently.
5. Review task duration, input size, and shuffle metrics.

This will show you the actual parallel execution rather than just the configured batch size.

Interview-ready answer:

"Databricks Auto Loader achieves parallelism through Spark's distributed execution engine. Auto Loader handles incremental file discovery and checkpoint-based progress tracking, while Spark distributes file-reading, transformation, and writing tasks across executors. We control ingestion batch sizes using `maxFilesPerTrigger` and `maxBytesPerTrigger`, and execution parallelism through Spark partitions and compute resources. Checkpointing provides fault tolerance and allows processing to resume after failures."

For your current Databricks Free Edition Auto Loader project, the next useful exercise is to upload 100 small CSV files, run Auto Loader, and examine the Spark task execution and checkpoint changes. This will demonstrate the difference between file ingestion batching and actual parallel processing.
