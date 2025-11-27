# Databricks notebook: 01_bronze_ingestion.py
# -----------------------------------------------------
# Bronze Layer — Raw ingestion with metadata tracking
# Reads CSV via Autoloader, adds audit columns, writes Delta
# -----------------------------------------------------
#
# Scale notes (7M+ orders):
#   - Autoloader handles schema evolution automatically.
#   - For files > 1 GB, enable partitioning hints (see repartition below).
#   - maxFilesPerTrigger controls micro-batch size; tune for your cluster.
#   - If using Autoloader in FILE_NOTIFICATION mode with Azure Event Grid,
#     ingestion is near-real-time as new blobs land.
#   - For initial backfill of 7M rows, use trigger(availableNow=True) which
#     processes everything in available micro-batches then stops.
#   - Consider spark.sql.shuffle.partitions = 200+ for large shuffles.

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Ingestion
# MAGIC
# MAGIC Reads raw order data from the `bronze` container, adds ingestion metadata,
# MAGIC and writes a Delta table for downstream consumption.
# MAGIC
# MAGIC Supports both the 50-row sample and 7M+ row generated datasets.

# COMMAND ----------
# Configuration — adjust these for your environment
storage_account = "stmedalliondev"
bronze_container = "bronze"
silver_container = "silver"
raw_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/raw/"
bronze_path = (
    f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/orders/"
)
checkpoint_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/checkpoints/orders_ingestion/"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

# Define explicit schema for the orders CSV
orders_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("order_date", DateType(), False),
        StructField("region", StringType(), False),
    ]
)

# COMMAND ----------
# Autoloader — incrementally ingest new CSV files
# maxFilesPerTrigger: limits micro-batch size for streaming-style processing.
#   At 7M rows (~1.5 GB CSV), a value of 1000 keeps each batch manageable.
#   For faster initial backfill on a large cluster, increase to 5000+.
raw_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}_schema")
    .option("header", "true")
    .option("sep", ",")
    .option("maxFilesPerTrigger", "1000")
    .schema(orders_schema)
    .load(raw_path)
)

# COMMAND ----------
# Add ingestion metadata columns
bronze_df = (
    raw_df.withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("csv_orders"))
    .withColumn("file_name", F.input_file_name())
)

# COMMAND ----------
# Write to Delta — append mode for incremental ingestion
# repartition hint: for large files (> 500 MB), repartition by a good
# distribution key before write to avoid tiny files in ADLS.
# Uncomment the line below if source CSVs are very large single files:
#   bronze_df = bronze_df.repartition(200)
(
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files, then stop
    .start(bronze_path)
)

# COMMAND ----------
# Verify: read back the bronze table
# For 7M+ rows, use approximate count for quick validation.
# exact:  spark.read.format("delta").load(bronze_path).count()
approx_count = spark.read.format("delta").load(bronze_path).rdd.countApprox(5000)
print(f"Bronze orders (approx): {approx_count}")

display(
    spark.read.format("delta").load(bronze_path).orderBy(F.col("order_id")).limit(100)
)

# COMMAND ----------
# Register as a Delta table for SQL consumers
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.orders
    USING DELTA
    LOCATION '{bronze_path}'
""")

# COMMAND ----------
# Data Quality: Bronze Validation
# -----------------------------------------------------
# Run Great Expectations validation on the bronze layer before
# allowing downstream silver transformation.
# Uncomment the block below when running in Databricks.
#
# import great_expectations as gx
# context = gx.get_context()
# checkpoint_result = context.run_checkpoint(
#     checkpoint_name="bronze_checkpoint",
#     batch_request={
#         "runtime_parameters": {
#             "path": bronze_path
#         },
#         "batch_identifiers": {
#             "default_identifier_name": "bronze_batch"
#         },
#     }
# )
# if not checkpoint_result.success:
#     raise ValueError("Bronze validation failed — check Data Docs for details")
