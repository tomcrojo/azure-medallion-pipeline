# Databricks notebook: 03_gold_aggregation.py
# -----------------------------------------------------
# Gold Layer — Business-ready aggregated tables
# Reads from Silver Delta, computes KPIs, writes Gold Delta tables
# -----------------------------------------------------
#
# Scale notes (7M+ rows):
#   - Repartition by region before Z-ORDER to co-locate data and avoid
#     small file problems at scale.
#   - Use approximate count (rdd.countApprox) instead of exact count()
#     for quick validation on millions of rows.
#   - OPTIMIZE / ZORDER commands run as compaction jobs — schedule them
#     as a separate job step in production.

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Aggregation
# MAGIC
# MAGIC Produces three business-ready tables:
# MAGIC - `daily_sales_summary` — revenue by date and region
# MAGIC - `top_products` — product performance ranking
# MAGIC - `customer_metrics` — customer lifetime value indicators

# COMMAND ----------
storage_account = "stmedalliondev"
silver_container = "silver"
gold_container = "gold"
silver_path = (
    f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/orders/"
)
gold_base = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------
silver_df = spark.read.format("delta").load(silver_path)

# Filter to valid records only for gold aggregations
valid_df = silver_df.filter(F.col("data_quality_flag") == "valid")

# =====================================================================
# Gold Table 1: daily_sales_summary
# =====================================================================
# repartition by region before aggregation for better file layout at scale.
# This ensures each region's data lands in contiguous file blocks, making
# the subsequent Z-ORDER on (order_date, region) more effective.
daily_sales = (
    valid_df.repartition("region")
    .groupBy("order_date", "region")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("gold_timestamp", F.current_timestamp())
)

daily_sales_path = f"{gold_base}daily_sales_summary/"
(
    daily_sales.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(daily_sales_path)
)

# Z-ORDER for fast filtering by date and region
spark.sql(f"""
    OPTIMIZE delta.`{daily_sales_path}`
    ZORDER BY (order_date, region)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.daily_sales_summary
    USING DELTA
    LOCATION '{daily_sales_path}'
""")

# =====================================================================
# Gold Table 2: top_products
# =====================================================================
top_products = (
    valid_df.groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .withColumn("gold_timestamp", F.current_timestamp())
    .orderBy(F.col("total_revenue").desc())
)

top_products_path = f"{gold_base}top_products/"
(
    top_products.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(top_products_path)
)

spark.sql(f"""
    OPTIMIZE delta.`{top_products_path}`
    ZORDER BY (product_id)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.top_products
    USING DELTA
    LOCATION '{top_products_path}'
""")

# =====================================================================
# Gold Table 3: customer_metrics
# =====================================================================
customer_metrics = (
    valid_df.groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("total_spend"),
        F.min("order_date").alias("first_order"),
        F.max("order_date").alias("last_order"),
    )
    .withColumn(
        "customer_tenure_days", F.datediff(F.col("last_order"), F.col("first_order"))
    )
    .withColumn("gold_timestamp", F.current_timestamp())
    .orderBy(F.col("total_spend").desc())
)

customer_metrics_path = f"{gold_base}customer_metrics/"
(
    customer_metrics.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(customer_metrics_path)
)

spark.sql(f"""
    OPTIMIZE delta.`{customer_metrics_path}`
    ZORDER BY (customer_id)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.customer_metrics
    USING DELTA
    LOCATION '{customer_metrics_path}'
""")

# =====================================================================
# Summary — use approximate counts for 7M+ rows
# =====================================================================
print("=" * 60)
print("Gold layer complete.")
ds_count = spark.read.format("delta").load(daily_sales_path).rdd.countApprox(5000)
tp_count = spark.read.format("delta").load(top_products_path).rdd.countApprox(5000)
cm_count = spark.read.format("delta").load(customer_metrics_path).rdd.countApprox(5000)
print(f"  daily_sales_summary : ~{ds_count} rows")
print(f"  top_products        : ~{tp_count} rows")
print(f"  customer_metrics    : ~{cm_count} rows")
print("=" * 60)

display(
    spark.read.format("delta")
    .load(daily_sales_path)
    .orderBy(F.col("order_date").desc())
    .limit(10)
)

# COMMAND ----------
# Data Quality: Gold Validation
# -----------------------------------------------------
# Run Great Expectations validation on the gold layer to ensure
# business-ready aggregations meet quality standards.
# Uncomment the block below when running in Databricks.
#
# import great_expectations as gx
# context = gx.get_context()
# checkpoint_result = context.run_checkpoint(
#     checkpoint_name="gold_checkpoint",
#     batch_request={
#         "runtime_parameters": {
#             "path": daily_sales_path
#         },
#         "batch_identifiers": {
#             "default_identifier_name": "gold_batch"
#         },
#     }
# )
# if not checkpoint_result.success:
#     raise ValueError("Gold validation failed — check Data Docs for details")
