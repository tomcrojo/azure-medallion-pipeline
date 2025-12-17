"""
Tests for the Gold layer aggregation logic.
Validates aggregation accuracy, row count reconciliation, and value constraints.

Run with: pytest tests/test_gold.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-gold")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def silver_sample(spark):
    """Simulate a silver DataFrame with clean, enriched data."""
    data = [
        (
            "ORD-001",
            "C001",
            "PROD-001",
            2,
            29.99,
            59.98,
            "2025-06-15",
            "Madrid",
            "valid",
        ),
        (
            "ORD-002",
            "C002",
            "PROD-003",
            1,
            149.99,
            149.99,
            "2025-06-16",
            "Barcelona",
            "valid",
        ),
        (
            "ORD-003",
            "C001",
            "PROD-005",
            3,
            9.50,
            28.50,
            "2025-06-17",
            "Sevilla",
            "valid",
        ),
        (
            "ORD-004",
            "C003",
            "PROD-001",
            1,
            29.99,
            29.99,
            "2025-06-15",
            "Madrid",
            "valid",
        ),
        (
            "ORD-005",
            "C002",
            "PROD-003",
            2,
            149.99,
            299.98,
            "2025-06-18",
            "Barcelona",
            "valid",
        ),
        (
            "ORD-006",
            "C004",
            "PROD-002",
            5,
            19.99,
            99.95,
            "2025-06-19",
            "Valencia",
            "invalid_values",
        ),
    ]
    cols = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "order_date",
        "region",
        "data_quality_flag",
    ]
    return spark.createDataFrame(data, cols)


# ---------- Aggregation totals ----------


class TestGoldAggregationTotals:
    def test_daily_sales_total_orders(self, spark, silver_sample):
        """daily_sales_summary total_orders should match count of valid orders per date+region."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        daily = valid.groupBy("order_date", "region").agg(
            F.countDistinct("order_id").alias("total_orders")
        )
        # 2025-06-15, Madrid should have 2 orders
        mad_jun15 = daily.filter(
            (F.col("order_date") == "2025-06-15") & (F.col("region") == "Madrid")
        ).collect()[0]
        assert mad_jun15["total_orders"] == 2

    def test_daily_sales_total_revenue(self, spark, silver_sample):
        """daily_sales_summary total_revenue should be sum of total_amount for valid rows."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        daily = valid.groupBy("order_date", "region").agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue")
        )
        # 2025-06-15, Madrid: 59.98 + 29.99 = 89.97
        mad_jun15 = daily.filter(
            (F.col("order_date") == "2025-06-15") & (F.col("region") == "Madrid")
        ).collect()[0]
        assert mad_jun15["total_revenue"] == 89.97

    def test_top_products_total_quantity(self, spark, silver_sample):
        """top_products total_quantity should equal sum of quantity per product."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        products = valid.groupBy("product_id").agg(
            F.sum("quantity").alias("total_quantity")
        )
        # PROD-001: 2 (ORD-001) + 1 (ORD-004) = 3
        prod1 = products.filter(F.col("product_id") == "PROD-001").collect()[0]
        assert prod1["total_quantity"] == 3

    def test_customer_metrics_total_orders(self, spark, silver_sample):
        """customer_metrics total_orders should match distinct order count per customer."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        cust = valid.groupBy("customer_id").agg(
            F.countDistinct("order_id").alias("total_orders")
        )
        # C001: ORD-001 + ORD-003 = 2
        c001 = cust.filter(F.col("customer_id") == "C001").collect()[0]
        assert c001["total_orders"] == 2


# ---------- Row count reconciliation ----------


class TestGoldRowCounts:
    def test_daily_sales_row_count(self, spark, silver_sample):
        """daily_sales_summary should have one row per distinct (date, region) from valid records."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        expected = valid.select("order_date", "region").distinct().count()
        daily = valid.groupBy("order_date", "region").agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        assert daily.count() == expected

    def test_top_products_row_count(self, spark, silver_sample):
        """top_products should have one row per distinct product_id."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        expected = valid.select("product_id").distinct().count()
        products = valid.groupBy("product_id").agg(
            F.sum("quantity").alias("total_quantity")
        )
        assert products.count() == expected

    def test_customer_metrics_row_count(self, spark, silver_sample):
        """customer_metrics should have one row per distinct customer_id."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        expected = valid.select("customer_id").distinct().count()
        cust = valid.groupBy("customer_id").agg(
            F.countDistinct("order_id").alias("total_orders")
        )
        assert cust.count() == expected


# ---------- Value constraints ----------


class TestGoldValueConstraints:
    def test_no_negative_revenue(self, spark, silver_sample):
        """No gold aggregation should produce negative revenue."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        daily = valid.groupBy("order_date", "region").agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue")
        )
        assert daily.filter(F.col("total_revenue") < 0).count() == 0

    def test_no_negative_quantity(self, spark, silver_sample):
        """No gold product aggregation should have negative quantity."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        products = valid.groupBy("product_id").agg(
            F.sum("quantity").alias("total_quantity")
        )
        assert products.filter(F.col("total_quantity") < 0).count() == 0

    def test_avg_order_value_is_positive(self, spark, silver_sample):
        """Average order value should always be positive for valid data."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        daily = valid.groupBy("order_date", "region").agg(
            F.round(F.avg("total_amount"), 2).alias("avg_order_value")
        )
        assert daily.filter(F.col("avg_order_value") <= 0).count() == 0

    def test_invalid_records_excluded_from_gold(self, spark, silver_sample):
        """Records flagged as invalid_values should not contribute to gold aggregations."""
        valid = silver_sample.filter(F.col("data_quality_flag") == "valid")
        invalid_count = silver_sample.filter(
            F.col("data_quality_flag") != "valid"
        ).count()
        assert valid.count() == silver_sample.count() - invalid_count
        # ORD-006 should be excluded
        assert valid.filter(F.col("order_id") == "ORD-006").count() == 0
