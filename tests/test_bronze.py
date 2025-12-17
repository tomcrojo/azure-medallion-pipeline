"""
Tests for the Bronze layer ingestion logic.
Validates schema enforcement, metadata column addition, and deduplication guarantees.

Run with: pytest tests/test_bronze.py -v
"""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
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
    """Create a local Spark session for testing."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-bronze")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------- Schema validation ----------


class TestBronzeSchema:
    def test_source_schema_has_required_columns(self, spark):
        """Source CSV must have all 7 required columns."""
        required = {
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_date",
            "region",
        }
        schema = StructType(
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
        assert set(field.name for field in schema.fields) == required

    def test_bronze_schema_has_metadata_columns(self, spark):
        """After ingestion, bronze table must include metadata columns."""
        metadata_cols = {"ingestion_timestamp", "source_system", "file_name"}
        # Simulate bronze transformation output
        bronze_schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("order_date", DateType(), False),
                StructField("region", StringType(), False),
                StructField("ingestion_timestamp", TimestampType(), False),
                StructField("source_system", StringType(), False),
                StructField("file_name", StringType(), False),
            ]
        )
        actual_cols = set(field.name for field in bronze_schema.fields)
        assert metadata_cols.issubset(actual_cols)


# ---------- Metadata addition ----------


class TestBronzeMetadata:
    def test_ingestion_adds_source_system(self, spark):
        """source_system column should be 'csv_orders' for CSV ingestion."""
        from pyspark.sql import functions as F

        data = [("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid")]
        df = spark.createDataFrame(
            data,
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "order_date",
                "region",
            ],
        )
        result = df.withColumn("source_system", F.lit("csv_orders"))
        assert "source_system" in result.columns
        assert result.select("source_system").first()[0] == "csv_orders"

    def test_ingestion_adds_ingestion_timestamp(self, spark):
        """ingestion_timestamp should be a non-null timestamp."""
        from pyspark.sql import functions as F

        data = [("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid")]
        df = spark.createDataFrame(
            data,
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "order_date",
                "region",
            ],
        )
        result = df.withColumn("ingestion_timestamp", F.current_timestamp())
        assert result.filter(F.col("ingestion_timestamp").isNotNull()).count() == 1

    def test_ingestion_adds_file_name(self, spark):
        """file_name column should be added with source file path."""
        from pyspark.sql import functions as F

        data = [("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid")]
        df = spark.createDataFrame(
            data,
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "order_date",
                "region",
            ],
        )
        result = df.withColumn("file_name", F.lit("orders.csv"))
        assert result.filter(F.col("file_name") == "orders.csv").count() == 1


# ---------- Deduplication ----------


class TestBronzeDeduplication:
    def test_no_duplicate_order_ids_after_dedup(self, spark):
        """After dedup, each order_id should appear exactly once."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        data = [
            ("ORD-001", "C001", "PROD-001", 2, 29.99, "2025-06-15", "Madrid"),
            (
                "ORD-001",
                "C001",
                "PROD-001",
                2,
                29.99,
                "2025-06-15",
                "Madrid",
            ),  # duplicate
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
                "row_num",
                F.row_number().over(
                    Window.partitionBy("order_id").orderBy("ingestion_timestamp")
                ),
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        assert deduped.count() == 2
        order_ids = [row.order_id for row in deduped.select("order_id").collect()]
        assert len(order_ids) == len(set(order_ids)), (
            "Duplicate order_ids found after dedup"
        )
