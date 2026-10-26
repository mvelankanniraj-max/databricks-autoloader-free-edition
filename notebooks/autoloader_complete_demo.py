# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader — Free Edition, complete 3-file demo
# MAGIC Run the cells in order on serverless compute. See README.md for setup and reset steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 — Configuration and setup

# COMMAND ----------

# EDIT ONLY IF YOUR CATALOG IS NOT NAMED "workspace".
# Run SHOW CATALOGS to check the available catalogs.
catalog_name = "workspace"
schema_name = "autoloader_demo"
volume_name = "autoloader_files"
table_name = "orders_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`")
base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
source_path = f"{base_path}/input"
schema_path = f"{base_path}/schema/orders"
checkpoint_path = f"{base_path}/checkpoints/orders"
target_table = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
dbutils.fs.mkdirs(source_path)
print("Input:", source_path)
print("Target:", target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 — Sample files (matching data/ in GitHub)

# COMMAND ----------

# Fixtures are embedded here on purpose: this notebook can run after
# import even if the current workspace directory is not the Git checkout.
# The identical, independently downloadable CSV files live in data/.
csv_files = {
    "orders_1.csv": "order_id,customer_name,product,amount\n101,John,Laptop,1200\n102,Emma,Mouse,25\n103,David,Keyboard,75\n",
    "orders_2.csv": "order_id,customer_name,product,amount\n104,Sophia,Monitor,300\n105,Michael,Headphones,150\n106,Olivia,Webcam,90\n",
    "orders_3.csv": "order_id,customer_name,product,amount\n107,James,Tablet,450\n108,Ava,Charger,35\n109,Daniel,SSD,110\n",
}
print("Fixture files:", list(csv_files))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — Stage the first CSV

# COMMAND ----------

# Start with an empty demo input and a fresh checkpoint for first-time use.
# Do not overwrite previously ingested files on a subsequent run.
existing_names = {f.name for f in dbutils.fs.ls(source_path)}
assert "orders_1.csv" not in existing_names, (
    "orders_1.csv already exists. Run the reset cell at the bottom "
    "to restart the full demo, or continue to the incremental cells."
)
dbutils.fs.put(f"{source_path}/orders_1.csv", csv_files["orders_1.csv"], overwrite=False)
display(dbutils.fs.ls(source_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 — Auto Loader reader and reusable ingestion function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, col

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
])

def run_autoloader():
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.useNotifications", "false")  # directory listing, no cloud credentials
        .option("header", "true")
        .schema(order_schema)
        .load(source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()
    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))
    return query

print("Streaming reader configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 — First ingestion (3 rows)

# COMMAND ----------

run_autoloader()
display(spark.table(target_table).orderBy("order_id"))
assert spark.table(target_table).count() == 3
print("PASS: 3 rows from orders_1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 — Add orders_2.csv (new file)

# COMMAND ----------

dbutils.fs.put(f"{source_path}/orders_2.csv", csv_files["orders_2.csv"], overwrite=False)
display(dbutils.fs.ls(source_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 — Incremental ingestion (6 total)

# COMMAND ----------

run_autoloader()
display(spark.table(target_table).orderBy("order_id"))
assert spark.table(target_table).count() == 6
print("PASS: 6 total rows; first file not ingested twice")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08 — Restart without any new files

# COMMAND ----------

run_autoloader()
assert spark.table(target_table).count() == 6
print("PASS: still 6 rows after a no-new-file restart")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09 — Add orders_3.csv

# COMMAND ----------

dbutils.fs.put(f"{source_path}/orders_3.csv", csv_files["orders_3.csv"], overwrite=False)
print("Third file staged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Third ingestion and results (9 total)

# COMMAND ----------

run_autoloader()
display(spark.table(target_table).orderBy("order_id"))
assert spark.table(target_table).count() == 9
print("PASS: 9 total rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 — Verify source counts and order-level uniqueness

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum as spark_sum
result = spark.table(target_table).agg(
    count("*").alias("row_count"),
    countDistinct("order_id").alias("unique_orders"),
    countDistinct("source_file").alias("file_count"),
    spark_sum("amount").alias("total_amount")
).first()
assert result["row_count"] == 9, result
assert result["unique_orders"] == 9, result
assert result["file_count"] == 3, result
assert result["total_amount"] == 2435.0, result

display(spark.sql(f"""SELECT source_file, COUNT(*) AS records,
    SUM(amount) AS total_amount FROM {target_table}
    GROUP BY source_file ORDER BY source_file"""))
print("PASS: 9 rows, 9 unique order IDs, 3 files, $2435 total amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 — Inspect checkpoint location

# COMMAND ----------

display(dbutils.fs.ls(checkpoint_path))
print("Preserve this checkpoint if you want incremental ingestion to resume.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 — OPTIONAL reset: run separately, only after finishing

# COMMAND ----------

# WARNING: Delete ONLY the sample data/table/checkpoint created by this demo.
# The Volume and schema remain. Re-run the notebook from Cell 1 afterwards.
# UNCOMMENT the lines below if you deliberately want to reset.
# spark.sql(f"DROP TABLE IF EXISTS {target_table}")
# for path in [source_path, schema_path, checkpoint_path]:
#     dbutils.fs.rm(path, True)
# dbutils.fs.mkdirs(source_path)
# print("Reset completed")
