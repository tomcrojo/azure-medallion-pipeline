"""
Tests for the Silver layer transformation logic.
Validates data type enforcement, null handling, enrichment calculations, and deduplication.

Run with: pytest tests/test_silver.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
    LongType,
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def bronze_sample(spark):
    """Simulate a bronze DataFrame with known test data."""
    data = [
        ("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "madrid"),
        ("ORD-002", "C002", "PROD-003", 1, 149.99, "2025-06-16", "barcelona"),
        ("ORD-003", "C001", "PROD-005", 3, 9.50, "2025-06-17", "sevilla"),
        ("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "madrid"),  # duplicate
        (
            "ORD-004",
            "C003",
            "PROD-002",
            None,
            49.99,
            "2025-06-18",
            "valencia",
        ),  # null quantity
    ]
    schema = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_date",
        "region",
    ]
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    return df


# ---------- Type validation ----------


class TestSilverDataTypes:
    def test_quantity_is_integer(self, spark):
        """quantity column must be integer type after casting."""
        df = spark.createDataFrame([("ORD-001", 2)], ["order_id", "quantity"])
        df = df.withColumn("quantity", F.col("quantity").cast("integer"))
        assert dict(df.dtypes)["quantity"] == "int"

    def test_unit_price_is_double(self, spark):
        """unit_price must be double type after casting."""
        df = spark.createDataFrame([("ORD-001", 29.99)], ["order_id", "unit_price"])
        df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
        assert dict(df.dtypes)["unit_price"] == "double"

    def test_order_date_is_date(self, spark):
        """order_date must be DateType after transformation."""
        df = spark.createDataFrame(
            [("ORD-001", "2025-06-15")], ["order_id", "order_date"]
        )
        df = df.withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        assert dict(df.dtypes)["order_date"] == "date"


# ---------- Null handling ----------


class TestSilverNullHandling:
    def test_no_nulls_in_key_columns(self, spark):
        """Key columns (order_id, quantity, unit_price) must not contain nulls after cleaning."""
        data = [
            ("ORD-001", "C001", 2, 29.99),
            (None, "C002", 1, 149.99),  # null order_id
            ("ORD-003", "C003", None, 9.50),  # null quantity
            ("ORD-004", "C004", 1, None),  # null unit_price
        ]
        df = spark.createDataFrame(
            data, ["order_id", "customer_id", "quantity", "unit_price"]
        )
        cleaned = df.filter(
            F.col("order_id").isNotNull()
            & F.col("quantity").isNotNull()
            & F.col("unit_price").isNotNull()
        )
        assert cleaned.count() == 1
        for col_name in ["order_id", "quantity", "unit_price"]:
            assert cleaned.filter(F.col(col_name).isNull()).count() == 0


# ---------- Enrichment ----------


class TestSilverEnrichment:
    def test_total_amount_calculation(self, spark):
        """total_amount should equal quantity * unit_price."""
        data = [("ORD-001", 2, 29.99), ("ORD-002", 3, 10.00)]
        df = spark.createDataFrame(data, ["order_id", "quantity", "unit_price"])
        df = df.withColumn(
            "total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
        )

        rows = df.select("total_amount").collect()
        assert rows[0]["total_amount"] == 59.98
        assert rows[1]["total_amount"] == 30.00

    def test_year_extraction(self, spark):
        """order_year should match the year from order_date."""
        data = [("ORD-001", "2025-06-15")]
        df = spark.createDataFrame(data, ["order_id", "order_date"])
        df = df.withColumn("order_date", F.to_date("order_date"))
        df = df.withColumn("order_year", F.year("order_date"))
        assert df.collect()[0]["order_year"] == 2025

    def test_month_extraction(self, spark):
        """order_month should match the month from order_date."""
        data = [("ORD-001", "2025-06-15")]
        df = spark.createDataFrame(data, ["order_id", "order_date"])
        df = df.withColumn("order_date", F.to_date("order_date"))
        df = df.withColumn("order_month", F.month("order_date"))
        assert df.collect()[0]["order_month"] == 6


# ---------- Deduplication ----------


class TestSilverDeduplication:
    def test_deduplication_removes_duplicate_order_ids(self, spark):
        """After dedup, each order_id appears exactly once."""
        data = [
            ("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid"),
            ("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid"),
            ("ORD-002", "C002", "PROD-003", 1, 149.99, "2025-06-16", "Barcelona"),
        ]
        schema = [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_date",
            "region",
        ]
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("ingestion_timestamp", F.current_timestamp())

        deduped = (
            df.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("order_id").orderBy("ingestion_timestamp")
                ),
            )
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        assert deduped.count() == 2
        order_ids = [r.order_id for r in deduped.select("order_id").collect()]
        assert order_ids.count("ORD-001") == 1
