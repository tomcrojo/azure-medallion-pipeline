# Databricks notebook: 02_silver_transformation.py
# -----------------------------------------------------
# Silver Layer — Clean, validate, enrich, and deduplicate
# Reads from Bronze Delta, applies transformations, writes to Silver Delta
# -----------------------------------------------------
#
# Scale notes (7M+ rows):
#   - The dimension tables (products, customers) are small (~500 and ~200K).
#     Use broadcast() joins so Spark ships them to every executor instead of
#     shuffling the large orders table.
#   - Cache() the bronze DataFrame if reused across multiple transformations.
#   - For production, partition silver output by order_year for efficient
#     downstream reads (partition pruning).
#   - Delta MERGE on 7M rows: ensure the target table is Z-ORDERed on the
#     merge key (order_id) for optimal performance.

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Transformation
# MAGIC
# MAGIC Reads raw data from the Bronze layer, applies deduplication, type validation,
# 魉 null handling, and enrichment. Uses Delta MERGE for idempotent upserts.

# COMMAND ----------
storage_account = "stmedalliondev"
bronze_container = "bronze"
silver_container = "silver"
bronze_path = (
    f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/orders/"
)
silver_path = (
    f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/orders/"
)

# Optional dimension paths for enrichment joins
customers_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/raw/sample_customers.csv"
products_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/raw/sample_products.csv"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# Read bronze Delta table
# Cache the bronze DataFrame for reuse across transformations.
# At 7M rows this uses ~2-4 GB executor memory — fine for 16 GB+ clusters.
bronze_df = spark.read.format("delta").load(bronze_path).cache()

# COMMAND ----------
# Step 1: Deduplicate — keep the earliest ingestion per order_id
deduped_df = (
    bronze_df.withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("ingestion_timestamp").asc())
        ),
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------
# Step 2: Type casting & null handling
cleaned_df = (
    deduped_df.filter(F.col("order_id").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("unit_price").isNotNull())
    .withColumn("quantity", F.col("quantity").cast("integer"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    .withColumn("region", F.initcap(F.trim(F.col("region"))))
)

# COMMAND ----------
# Step 3: Enrich — derive total_amount, extract year/month
enriched_df = (
    cleaned_df.withColumn(
        "total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn(
        "data_quality_flag",
        F.when(
            (F.col("quantity") <= 0) | (F.col("unit_price") <= 0),
            F.lit("invalid_values"),
        ).otherwise(F.lit("valid")),
    )
    .withColumn("silver_timestamp", F.current_timestamp())
)

# COMMAND ----------
# Step 3b (optional): Enrich with dimension data via broadcast joins.
# broadcast() tells Spark to replicate the small table to all executors,
# avoiding a shuffle of the 7M-row fact table.
# Uncomment the block below if sample_customers.csv / sample_products.csv exist.
#
# customers_df = (
#     spark.read.option("header", "true").csv(customers_path)
#     .select("customer_id", "customer_name", "loyalty_tier")
# )
# products_df = (
#     spark.read.option("header", "true").csv(products_path)
#     .select("product_id", "product_name", "category")
# )
#
# enriched_df = (
#     enriched_df
#     .join(F.broadcast(customers_df), "customer_id", "left")
#     .join(F.broadcast(products_df), "product_id", "left")
# )

# COMMAND ----------
# Step 4: Select final columns in canonical order
silver_final = enriched_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_date",
    "order_year",
    "order_month",
    "region",
    "data_quality_flag",
    "ingestion_timestamp",
    "silver_timestamp",
    "source_system",
    "file_name",
)

# COMMAND ----------
# Step 5: MERGE (upsert) into silver table for idempotency
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)
    (
        silver_table.alias("target")
        .merge(silver_final.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run: write initial table
    # Production strategy: partition by order_year for efficient reads.
    # Uncomment the .partitionBy() line for production-scale deployments:
    #   silver_final.repartition("order_year").write.format("delta") \
    #       .mode("overwrite").partitionBy("order_year") \
    #       .option("overwriteSchema", "true").save(silver_path)
    (
        silver_final.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

# COMMAND ----------
# Register as a Delta table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.orders
    USING DELTA
    LOCATION '{silver_path}'
""")

# COMMAND ----------
# Verify — limit output for 7M+ rows
approx_count = spark.read.format("delta").load(silver_path).rdd.countApprox(5000)
print(f"Silver orders (approx): {approx_count}")

display(spark.read.format("delta").load(silver_path).orderBy("order_id").limit(100))

# Free cached DataFrame
bronze_df.unpersist()

# COMMAND ----------
# Data Quality: Silver Validation
# -----------------------------------------------------
# Run Great Expectations validation on the silver layer before
# allowing downstream gold aggregation.
# Uncomment the block below when running in Databricks.
#
# import great_expectations as gx
# context = gx.get_context()
# checkpoint_result = context.run_checkpoint(
#     checkpoint_name="silver_checkpoint",
#     batch_request={
#         "runtime_parameters": {
#             "path": silver_path
#         },
#         "batch_identifiers": {
#             "default_identifier_name": "silver_batch"
#         },
#     }
# )
# if not checkpoint_result.success:
#     raise ValueError("Silver validation failed — check Data Docs for details")
