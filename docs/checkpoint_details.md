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
