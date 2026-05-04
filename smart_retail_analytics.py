"""
Smart Retail Analytics Platform — RetailX
==========================================
This script covers the full data pipeline for RetailX's analytics platform.
It's structured in 6 phases, each building on the previous one:

    Phase 1 -> RDD basics (map, filter, reduceByKey)
    Phase 2 -> DataFrames + Spark SQL (joins, aggregations, trends)
    Phase 3 -> Medallion Lakehouse layers (Bronze / Silver / Gold)
    Phase 4 -> Structured Streaming with demand spike detection
    Phase 5 -> Power BI star-schema model export
    Phase 6 -> DAX measures + dashboard page definitions

Datasets come from: https://github.com/himanshusar123/Datasets
    - sales_data.csv
    - customer_data.csv
    - product_data.csv

They get auto-downloaded on first run into the data/ folder.
If you're offline, just drop the CSVs there manually.

A few things worth noting upfront:
    - No credentials are hardcoded anywhere. All paths/thresholds live in Config.
    - Every DataFrame is loaded with an explicit schema — no inferSchema surprises.
    - Logging is used throughout so you get a clean audit trail.
    - Each phase is wrapped in its own try/except in main() so one failure
      doesn't kill the whole run.
"""

import os
import sys
import csv
import json
import time
import random
import logging
import warnings
from datetime import datetime
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType, FloatType
    )
except ImportError as e:
    sys.exit(f"PySpark is not installed. Run: pip install pyspark\nError: {e}")

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("retailx")


# ---------------------------------------------------------------------------
# Config
# All tunable values in one place. Change thresholds here, not inside functions.
# ---------------------------------------------------------------------------
class Config:
    # GitHub raw URLs for the three datasets
    _BASE = "https://raw.githubusercontent.com/himanshusar123/Datasets/main/"
    SALES_URL    = _BASE + "sales_data.csv"
    CUSTOMER_URL = _BASE + "customer_data.csv"
    PRODUCT_URL  = _BASE + "product_data.csv"

    # Local paths — files land here after download
    DATA_DIR      = Path("data")
    SALES_PATH    = DATA_DIR / "sales_data.csv"
    CUSTOMER_PATH = DATA_DIR / "customer_data.csv"
    PRODUCT_PATH  = DATA_DIR / "product_data.csv"

    # Lakehouse layer roots (simulates OneLake folder structure locally)
    LAKEHOUSE     = Path("lakehouse")
    BRONZE        = LAKEHOUSE / "bronze"
    SILVER        = LAKEHOUSE / "silver"
    GOLD          = LAKEHOUSE / "gold"

    # Streaming paths
    STREAM_IN         = Path("stream_input")
    STREAM_OUT        = Path("stream_output")
    STREAM_CHECKPOINT = Path("stream_checkpoint")

    # Business rules
    HIGH_VALUE_THRESHOLD  = 500.0   # mark a transaction as high-value above this
    SALES_ALERT_THRESHOLD = 1000.0  # Data Activator fires if daily sales drop below
    SPIKE_FACTOR          = 2.0     # demand spike = qty > avg * this multiplier

    # Spark
    APP_NAME = "RetailX_Analytics"
    MASTER   = "local[*]"


# ---------------------------------------------------------------------------
# Schemas
# Defining these explicitly avoids the cost of inferSchema and prevents
# silent type mismatches when the source files change.
# ---------------------------------------------------------------------------
SALES_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  nullable=False),
    StructField("customer_id",    StringType(),  nullable=False),
    StructField("product_id",     StringType(),  nullable=False),
    StructField("store_id",       StringType(),  nullable=True),
    StructField("quantity",       IntegerType(), nullable=True),
    StructField("price",          FloatType(),   nullable=True),
    StructField("timestamp",      StringType(),  nullable=True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name",        StringType(), nullable=True),
    StructField("city",        StringType(), nullable=True),
    StructField("segment",     StringType(), nullable=True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("category",   StringType(), nullable=True),
    StructField("brand",      StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    """
    Spin up (or reuse) a SparkSession.
    shuffle.partitions is set low because we're running locally —
    the default of 200 is overkill for dev-sized data.
    """
    spark = (
        SparkSession.builder
        .appName(Config.APP_NAME)
        .master(Config.MASTER)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def makedirs(*paths: Path) -> None:
    """Create one or more directories, no-op if they already exist."""
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def fetch_csv(url: str, dest: Path) -> None:
    """
    Download a CSV file only if it isn't already cached locally.
    Uses stdlib urllib so there's no extra dependency.
    """
    if dest.exists():
        log.info("Already have %s, skipping download.", dest.name)
        return
    import urllib.request
    log.info("Downloading %s ...", dest.name)
    try:
        urllib.request.urlretrieve(url, dest)
    except Exception as exc:
        # Non-fatal — caller will catch the missing file later
        log.warning("Could not download %s: %s", dest.name, exc)


def check_df(df, label: str, required: list) -> None:
    """
    Lightweight sanity check before we do anything with a DataFrame.
    Raises ValueError if required columns are missing or the frame is empty.
    This catches schema drift early rather than letting it blow up mid-pipeline.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing columns: {missing}")
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"{label} loaded 0 rows — check the source file.")
    log.info("%s looks good: %d rows", label, row_count)


# ---------------------------------------------------------------------------
# Phase 1 — RDD Operations
#
# The goal here is to show the low-level Spark API before moving to
# DataFrames. Real pipelines rarely use RDDs directly anymore, but
# understanding them helps when you need fine-grained control or
# have to work with unstructured text.
# ---------------------------------------------------------------------------

def phase1_rdd_operations(spark: SparkSession) -> None:
    log.info("--- Phase 1: RDD Operations ---")

    sc = spark.sparkContext

    # Load the raw file as a text RDD, strip the header row
    raw = sc.textFile(str(Config.SALES_PATH))
    header = raw.first()
    rows = raw.filter(lambda line: line != header)

    log.info("Partitions in raw RDD: %d", rows.getNumPartitions())

    def parse_row(line: str):
        """
        Split a CSV line and return a dict.
        Returns None if the line is malformed so we can filter it out cleanly
        instead of letting a bad cast crash the whole task.
        """
        try:
            parts = line.split(",")
            if len(parts) < 6:
                return None
            return {
                "transaction_id": parts[0].strip(),
                "customer_id":    parts[1].strip(),
                "product_id":     parts[2].strip(),
                "store_id":       parts[3].strip(),
                "quantity":       int(parts[4].strip()),
                "price":          float(parts[5].strip()),
                "timestamp":      parts[6].strip() if len(parts) > 6 else "",
            }
        except (ValueError, IndexError):
            return None

    # Map: parse each line
    parsed = rows.map(parse_row)

    # Filter: drop nulls and records with zero/negative price or quantity
    valid = parsed.filter(
        lambda r: r is not None and r["price"] > 0 and r["quantity"] > 0
    )

    dropped = parsed.count() - valid.count()
    log.info("Dropped %d invalid records during RDD filter.", dropped)

    # Map again: turn each record into a (product_id, revenue) pair
    revenue_pairs = valid.map(lambda r: (r["product_id"], r["price"] * r["quantity"]))

    # ReduceByKey: sum revenue per product across all partitions
    product_revenue = revenue_pairs.reduceByKey(lambda a, b: a + b)

    # Collect top 10 — sortBy triggers a shuffle, take() limits data pulled to driver
    top10 = product_revenue.sortBy(lambda x: x[1], ascending=False).take(10)

    log.info("Top 10 products by revenue (RDD):")
    for i, (pid, rev) in enumerate(top10, 1):
        log.info("  %2d.  %-15s  Rs %.2f", i, pid, rev)

    log.info("Phase 1 done.\n")


# ---------------------------------------------------------------------------
# Phase 2 — DataFrames & Spark SQL
#
# This is where most of the analytical work happens. We load all three
# datasets, clean the sales data, register temp views, then run SQL queries
# for the business questions RetailX actually cares about.
# ---------------------------------------------------------------------------

def phase2_dataframes_sql(spark: SparkSession):
    """
    Returns the cleaned sales DataFrame, customer/product dims, and the
    fully joined enriched DataFrame. Downstream phases all use these.
    """
    log.info("--- Phase 2: DataFrames & Spark SQL ---")

    # Load with explicit schemas — much safer than letting Spark guess types
    sales_df = (
        spark.read
        .option("header", "true")
        .schema(SALES_SCHEMA)
        .csv(str(Config.SALES_PATH))
    )
    customer_df = (
        spark.read
        .option("header", "true")
        .schema(CUSTOMER_SCHEMA)
        .csv(str(Config.CUSTOMER_PATH))
    )
    product_df = (
        spark.read
        .option("header", "true")
        .schema(PRODUCT_SCHEMA)
        .csv(str(Config.PRODUCT_PATH))
    )

    check_df(sales_df,    "sales_df",    ["transaction_id", "price", "quantity"])
    check_df(customer_df, "customer_df", ["customer_id", "city", "segment"])
    check_df(product_df,  "product_df",  ["product_id", "category", "brand"])

    # Clean sales: parse timestamp, compute revenue, drop rows missing key fields
    sales_clean = (
        sales_df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("revenue", F.col("price") * F.col("quantity"))
        .dropna(subset=["transaction_id", "customer_id", "product_id", "price", "quantity"])
        .filter((F.col("price") > 0) & (F.col("quantity") > 0))
    )

    # High-value filter — useful for premium segment analysis
    hv = sales_clean.filter(F.col("revenue") >= Config.HIGH_VALUE_THRESHOLD)
    log.info("High-value transactions (>= Rs %.0f): %d", Config.HIGH_VALUE_THRESHOLD, hv.count())

    # Register views so we can write plain SQL below
    sales_clean.createOrReplaceTempView("sales")
    customer_df.createOrReplaceTempView("customers")
    product_df.createOrReplaceTempView("products")

    # --- Revenue by city ---
    # Joining sales to customers to get the city dimension
    revenue_by_city = spark.sql("""
        SELECT
            c.city,
            ROUND(SUM(s.price * s.quantity), 2) AS total_revenue,
            COUNT(s.transaction_id)             AS total_transactions
        FROM sales s
        JOIN customers c ON s.customer_id = c.customer_id
        GROUP BY c.city
        ORDER BY total_revenue DESC
    """)
    log.info("Revenue by city:")
    revenue_by_city.show(10, truncate=False)

    # --- Full enriched dataset (sales + customer + product) ---
    # This is the base table for Gold layer aggregations
    enriched_df = spark.sql("""
        SELECT
            s.transaction_id,
            s.timestamp,
            s.quantity,
            s.price,
            ROUND(s.price * s.quantity, 2) AS revenue,
            c.customer_id,
            c.name    AS customer_name,
            c.city,
            c.segment,
            p.product_id,
            p.category,
            p.brand
        FROM sales s
        JOIN customers c ON s.customer_id = c.customer_id
        JOIN products  p ON s.product_id  = p.product_id
    """)
    log.info("Enriched dataset: %d rows", enriched_df.count())

    # --- Top 5 products by revenue ---
    top5 = spark.sql("""
        SELECT
            p.product_id,
            p.category,
            p.brand,
            ROUND(SUM(s.price * s.quantity), 2) AS total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        GROUP BY p.product_id, p.category, p.brand
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    log.info("Top 5 products by revenue:")
    top5.show(truncate=False)

    # --- Monthly sales trend ---
    monthly = spark.sql("""
        SELECT
            DATE_FORMAT(timestamp, 'yyyy-MM') AS month,
            ROUND(SUM(price * quantity), 2)   AS monthly_revenue,
            COUNT(transaction_id)             AS num_transactions
        FROM sales
        WHERE timestamp IS NOT NULL
        GROUP BY DATE_FORMAT(timestamp, 'yyyy-MM')
        ORDER BY month
    """)
    log.info("Monthly sales trend:")
    monthly.show(24, truncate=False)

    log.info("Phase 2 done.\n")
    return sales_clean, customer_df, product_df, enriched_df


# ---------------------------------------------------------------------------
# Phase 3 — Medallion Lakehouse (Bronze / Silver / Gold)
#
# This mirrors what you'd build in Microsoft Fabric with OneLake.
# Locally we write Parquet files, but the logic is identical — you'd just
# swap the output paths for abfss:// URIs pointing at your Fabric workspace.
#
# Bronze  = raw files, no changes, exactly as they arrived
# Silver  = cleaned, validated, partitioned for efficient reads
# Gold    = aggregated tables that Power BI queries directly
# ---------------------------------------------------------------------------

def phase3_lakehouse(spark, sales_clean, customer_df, product_df, enriched_df) -> None:
    log.info("--- Phase 3: Medallion Lakehouse ---")

    makedirs(Config.BRONZE, Config.SILVER, Config.GOLD)

    # BRONZE — just land the raw CSVs as Parquet, no transformation
    log.info("[Bronze] Ingesting raw files ...")
    for name, path in [
        ("sales",     Config.SALES_PATH),
        ("customers", Config.CUSTOMER_PATH),
        ("products",  Config.PRODUCT_PATH),
    ]:
        df = spark.read.option("header", "true").csv(str(path))
        df.write.mode("overwrite").parquet(str(Config.BRONZE / name))
        log.info("  %s -> %d rows written", name, df.count())

    # SILVER — cleaned data, partitioned by store_id for sales
    # Partitioning by store_id means queries filtered on a single store
    # only scan that partition instead of the whole table.
    log.info("[Silver] Writing cleaned data ...")
    (
        sales_clean.write
        .mode("overwrite")
        .partitionBy("store_id")
        .parquet(str(Config.SILVER / "sales"))
    )
    customer_df.dropna(subset=["customer_id"]).write.mode("overwrite").parquet(str(Config.SILVER / "customers"))
    product_df.dropna(subset=["product_id"]).write.mode("overwrite").parquet(str(Config.SILVER / "products"))
    log.info("  Silver layer ready.")

    # GOLD — four aggregated tables for Power BI consumption
    log.info("[Gold] Building aggregated tables ...")

    # 1. Revenue by city
    (
        enriched_df
        .groupBy("city")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("transaction_id").alias("total_transactions"),
            F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
        .write.mode("overwrite").parquet(str(Config.GOLD / "revenue_by_city"))
    )

    # 2. Revenue by product category and brand
    (
        enriched_df
        .groupBy("category", "brand")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
        )
        .orderBy(F.desc("total_revenue"))
        .write.mode("overwrite").parquet(str(Config.GOLD / "revenue_by_category"))
    )

    # 3. Customer segment breakdown
    (
        enriched_df
        .groupBy("segment")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("revenue"), 2).alias("avg_spend"),
        )
        .write.mode("overwrite").parquet(str(Config.GOLD / "customer_segments"))
    )

    # 4. Monthly trend
    (
        enriched_df
        .withColumn("month", F.date_format("timestamp", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.round(F.sum("revenue"), 2).alias("monthly_revenue"),
            F.count("transaction_id").alias("num_transactions"),
        )
        .orderBy("month")
        .write.mode("overwrite").parquet(str(Config.GOLD / "monthly_trend"))
    )

    log.info("  Gold layer ready.")

    # Data Activator simulation
    # In Fabric, you'd configure a real-time rule in Data Activator.
    # Here we just flag the days that would have triggered an alert.
    log.info("[Data Activator] Checking for low-sales days ...")
    daily = (
        enriched_df
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("date")
        .agg(F.round(F.sum("revenue"), 2).alias("daily_revenue"))
        .orderBy("date")
    )
    alerts = daily.filter(F.col("daily_revenue") < Config.SALES_ALERT_THRESHOLD)
    n = alerts.count()
    if n > 0:
        log.warning("ALERT: %d day(s) fell below the Rs %.0f sales threshold.", n, Config.SALES_ALERT_THRESHOLD)
        alerts.show(10, truncate=False)
    else:
        log.info("  No low-sales alerts for this dataset.")

    log.info("Phase 3 done.\n")


# ---------------------------------------------------------------------------
# Phase 4 — Structured Streaming
#
# We simulate a live event feed by writing small CSV batches to stream_input/.
# In production you'd swap the CSV source for an Event Hub or Kafka connector.
#
# The use case: detect when a product's order quantity spikes within a
# 1-minute window — useful for inventory alerts or flash-sale detection.
# ---------------------------------------------------------------------------

def _write_fake_events(n_files: int = 5, rows_per_file: int = 30) -> None:
    """
    Generate synthetic sales event files to feed the streaming query.
    Intentionally keeps a few high-quantity rows to trigger spike detection.
    """
    makedirs(Config.STREAM_IN)

    products  = [f"P{i:03d}" for i in range(1, 21)]
    customers = [f"C{i:04d}" for i in range(1, 101)]
    stores    = [f"S{i:02d}"  for i in range(1, 6)]

    for batch in range(n_files):
        fpath = Config.STREAM_IN / f"batch_{batch:03d}.csv"
        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["transaction_id", "customer_id", "product_id",
                             "store_id", "quantity", "price", "timestamp"])
            for row in range(rows_per_file):
                # Occasionally inject a high-quantity row to simulate a spike
                qty = random.randint(40, 100) if row % 10 == 0 else random.randint(1, 15)
                writer.writerow([
                    f"T{batch:03d}{row:04d}",
                    random.choice(customers),
                    random.choice(products),
                    random.choice(stores),
                    qty,
                    round(random.uniform(50, 2000), 2),
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ])
        time.sleep(0.05)
    log.info("Generated %d synthetic event files in %s", n_files, Config.STREAM_IN)


def phase4_streaming(spark: SparkSession) -> None:
    log.info("--- Phase 4: Structured Streaming ---")

    makedirs(Config.STREAM_IN, Config.STREAM_OUT, Config.STREAM_CHECKPOINT)
    _write_fake_events()

    # Read the CSV files as a streaming source
    stream_df = (
        spark.readStream
        .option("header", "true")
        .schema(SALES_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .csv(str(Config.STREAM_IN))
    )

    # Compute per-product totals within 1-minute event-time windows.
    # Watermark of 5 minutes handles late-arriving records gracefully.
    windowed = (
        stream_df
        .withColumn("revenue", F.col("price") * F.col("quantity"))
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "5 minutes")
        .groupBy(F.window("event_time", "1 minute"), "product_id")
        .agg(
            F.sum("quantity").alias("total_qty"),
            F.round(F.sum("revenue"), 2).alias("window_revenue"),
        )
    )

    # trigger(once=True) processes everything currently in the source then stops.
    # Good for batch-style streaming runs in dev/test.
    query = (
        windowed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", str(Config.STREAM_OUT))
        .option("checkpointLocation", str(Config.STREAM_CHECKPOINT))
        .trigger(once=True)
        .start()
    )
    query.awaitTermination(timeout=120)
    log.info("Streaming query finished.")

    # Post-processing: read the output and flag demand spikes
    out_files = list(Config.STREAM_OUT.rglob("*.parquet"))
    if out_files:
        results = spark.read.parquet(str(Config.STREAM_OUT))
        avg_qty = results.agg(F.avg("total_qty")).collect()[0][0] or 0
        spikes = results.filter(F.col("total_qty") > avg_qty * Config.SPIKE_FACTOR)
        n = spikes.count()
        if n:
            log.warning("SPIKE ALERT: %d product-windows exceeded %.1fx average demand.", n, Config.SPIKE_FACTOR)
            spikes.orderBy(F.desc("total_qty")).show(10, truncate=False)
        else:
            log.info("No demand spikes detected in this batch.")
    else:
        log.warning("No streaming output found — the query may not have processed any files.")

    log.info("Phase 4 done.\n")


# ---------------------------------------------------------------------------
# Phase 5 — Power BI Data Model
#
# Exports a JSON file describing the star schema so there's a clear record
# of what the Power BI model should look like. In Fabric, Power BI connects
# directly to the Gold Lakehouse via the SQL Analytics Endpoint — you don't
# need to export anything manually. This file is mainly for documentation
# and for teams that import Parquet files into Power BI Desktop.
# ---------------------------------------------------------------------------

def phase5_powerbi_model(spark: SparkSession, enriched_df) -> None:
    log.info("--- Phase 5: Power BI Data Model ---")

    # Star schema definition
    # Fact: Sales | Dims: Customer, Product
    model = {
        "model_name": "RetailX_StarSchema",
        "description": (
            "Star schema for RetailX. Sales is the fact table. "
            "Customer and Product are dimension tables. "
            "In Fabric, connect Power BI to the Gold Lakehouse via the SQL Analytics Endpoint."
        ),
        "fact_table": {
            "name": "Sales",
            "lakehouse_path": "lakehouse/gold/",
            "columns": [
                "transaction_id", "customer_id", "product_id",
                "store_id", "quantity", "price", "revenue", "timestamp"
            ],
        },
        "dimension_tables": [
            {
                "name": "Customer",
                "lakehouse_path": "lakehouse/silver/customers",
                "columns": ["customer_id", "name", "city", "segment"],
                "relationship": {
                    "from_col": "Sales[customer_id]",
                    "to_col":   "Customer[customer_id]",
                    "cardinality": "Many-to-One",
                    "cross_filter": "Single",
                },
            },
            {
                "name": "Product",
                "lakehouse_path": "lakehouse/silver/products",
                "columns": ["product_id", "category", "brand"],
                "relationship": {
                    "from_col": "Sales[product_id]",
                    "to_col":   "Product[product_id]",
                    "cardinality": "Many-to-One",
                    "cross_filter": "Single",
                },
            },
        ],
    }

    out = Path("powerbi_model_schema.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(model, f, indent=2)
    log.info("Model schema written to %s", out)

    log.info("Sample rows from the enriched fact table:")
    enriched_df.select(
        "transaction_id", "customer_id", "product_id",
        "revenue", "city", "segment", "category"
    ).show(5, truncate=False)

    log.info("Phase 5 done.\n")


# ---------------------------------------------------------------------------
# Phase 6 — DAX Measures & Dashboard Spec
#
# These are the measures you'd create inside Power BI Desktop.
# They're stored here as strings so they're version-controlled alongside
# the pipeline code — easier to review and track changes than hunting
# through a .pbix file.
# ---------------------------------------------------------------------------

# Each key is the measure name as it appears in Power BI
DAX_MEASURES = {

    "Total Sales": """
Total Sales =
    SUMX(
        Sales,
        Sales[price] * Sales[quantity]
    )""",

    "Average Order Value": """
Average Order Value =
    DIVIDE(
        [Total Sales],
        DISTINCTCOUNT(Sales[transaction_id]),
        0
    )""",

    # Month-over-month growth. DIVIDE with 0 as fallback handles the first month
    # where there's no previous period to compare against.
    "Sales Growth %": """
Sales Growth % =
    VAR ThisMonth =
        CALCULATE([Total Sales], DATESMTD(Sales[timestamp]))
    VAR LastMonth =
        CALCULATE([Total Sales], DATEADD(DATESMTD(Sales[timestamp]), -1, MONTH))
    RETURN
        DIVIDE(ThisMonth - LastMonth, LastMonth, 0)""",

    # Returns the name of the single highest-revenue category
    "Top Category": """
Top Category =
    CALCULATE(
        FIRSTNONBLANK(Product[category], 1),
        TOPN(
            1,
            SUMMARIZE(Sales, Product[category], "rev", [Total Sales]),
            [rev],
            DESC
        )
    )""",

    "Customer Count": """
Customer Count =
    DISTINCTCOUNT(Sales[customer_id])""",

    "Revenue by Segment": """
Revenue by Segment =
    CALCULATE(
        [Total Sales],
        ALLEXCEPT(Customer, Customer[segment])
    )""",
}

# Dashboard layout spec — describes what goes on each report page.
# Hand this to whoever is building the .pbix file.
DASHBOARD_PAGES = [
    {
        "page": "Executive Overview",
        "visuals": [
            {"type": "KPI Card", "measure": "Total Sales"},
            {"type": "KPI Card", "measure": "Average Order Value"},
            {"type": "KPI Card", "measure": "Sales Growth %"},
            {"type": "KPI Card", "measure": "Customer Count"},
        ],
    },
    {
        "page": "Sales by Geography",
        "visuals": [
            {"type": "Filled Map", "location": "Customer[city]", "value": "Total Sales"},
            {"type": "Bar Chart",  "axis": "Customer[city]",     "value": "Total Sales"},
        ],
    },
    {
        "page": "Sales Trend",
        "visuals": [
            {"type": "Line Chart", "axis": "Sales[timestamp] (Month)", "value": "Total Sales"},
            {"type": "Area Chart", "axis": "Sales[timestamp] (Month)", "value": "Sales Growth %"},
        ],
    },
    {
        "page": "Top Products",
        "visuals": [
            {"type": "Bar Chart", "axis": "Product[brand]",    "value": "Total Sales"},
            {"type": "Treemap",   "group": "Product[category]","value": "Total Sales"},
        ],
    },
    {
        "page": "Customer Segmentation",
        "visuals": [
            {"type": "Donut Chart", "legend": "Customer[segment]", "value": "Total Sales"},
            {"type": "Table", "columns": ["Customer[segment]", "Total Sales", "Average Order Value", "Customer Count"]},
        ],
    },
]


def phase6_dax_dashboard() -> None:
    log.info("--- Phase 6: DAX Measures & Dashboard Spec ---")

    for name, dax in DAX_MEASURES.items():
        log.info("Measure [%s]:%s", name, dax)

    out = Path("dashboard_spec.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"dax_measures": DAX_MEASURES, "dashboard_pages": DASHBOARD_PAGES}, f, indent=2)
    log.info("Dashboard spec written to %s", out)

    log.info("Phase 6 done.\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("RetailX Smart Analytics — starting pipeline")

    # Make sure the data directory exists and pull the CSVs if needed
    makedirs(Config.DATA_DIR)
    fetch_csv(Config.SALES_URL,    Config.SALES_PATH)
    fetch_csv(Config.CUSTOMER_URL, Config.CUSTOMER_PATH)
    fetch_csv(Config.PRODUCT_URL,  Config.PRODUCT_PATH)

    # Bail early if any file is still missing after the download attempt
    missing = [p for p in [Config.SALES_PATH, Config.CUSTOMER_PATH, Config.PRODUCT_PATH] if not p.exists()]
    if missing:
        log.error("Missing data files: %s", [str(p) for p in missing])
        log.error("Place the CSVs in the data/ folder and re-run.")
        sys.exit(1)

    spark = get_spark()

    try:
        # Phase 1 — RDD
        try:
            phase1_rdd_operations(spark)
        except Exception as e:
            log.error("Phase 1 failed: %s", e, exc_info=True)

        # Phase 2 — DataFrames & SQL (must succeed; everything else depends on it)
        try:
            sales_clean, customer_df, product_df, enriched_df = phase2_dataframes_sql(spark)
        except Exception as e:
            log.error("Phase 2 failed: %s", e, exc_info=True)
            sys.exit(1)

        # Phase 3 — Lakehouse
        try:
            phase3_lakehouse(spark, sales_clean, customer_df, product_df, enriched_df)
        except Exception as e:
            log.error("Phase 3 failed: %s", e, exc_info=True)

        # Phase 4 — Streaming
        try:
            phase4_streaming(spark)
        except Exception as e:
            log.error("Phase 4 failed: %s", e, exc_info=True)

        # Phase 5 — Power BI model
        try:
            phase5_powerbi_model(spark, enriched_df)
        except Exception as e:
            log.error("Phase 5 failed: %s", e, exc_info=True)

        # Phase 6 — DAX (no Spark needed)
        try:
            phase6_dax_dashboard()
        except Exception as e:
            log.error("Phase 6 failed: %s", e, exc_info=True)

    finally:
        spark.stop()
        log.info("Spark stopped.")

    log.info("Pipeline complete. Outputs are in lakehouse/, powerbi_model_schema.json, dashboard_spec.json")


if __name__ == "__main__":
    main()
