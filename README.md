RetailX — Smart Retail Analytics Platform
This project builds an end-to-end data pipeline for RetailX, a retail company operating across multiple cities in India. The pipeline covers everything from raw data ingestion to Power BI dashboards, using PySpark and a Medallion Lakehouse architecture that mirrors what you'd deploy on Microsoft Fabric.

What's in the project
.
├── smart_retail_analytics.py   # the whole pipeline, one file, six phases
├── requirements.txt
├── README.md
├── data/                       # CSVs land here (auto-downloaded on first run)
│   ├── sales_data.csv
│   ├── customer_data.csv
│   └── product_data.csv
├── lakehouse/
│   ├── bronze/                 # raw Parquet, no changes from source
│   ├── silver/                 # cleaned and validated
│   └── gold/                   # aggregated, ready for Power BI
├── stream_input/               # synthetic event files for Phase 4
├── stream_output/              # streaming query results
├── stream_checkpoint/          # Spark streaming checkpoint
├── powerbi_model_schema.json   # star schema definition (Phase 5)
└── dashboard_spec.json         # DAX measures + dashboard layout (Phase 6)
Datasets
The three source files come from https://github.com/himanshusar123/Datasets. The script downloads them automatically into data/ on the first run. If you're working offline, just drop the files there manually.

File	Columns
sales_data.csv	transaction_id, customer_id, product_id, store_id, quantity, price, timestamp
customer_data.csv	customer_id, name, city, segment
product_data.csv	product_id, category, brand
Getting started
You need Python 3.9+ and Java 8 or 11 (Spark requires Java on the PATH).

pip install -r requirements.txt
python smart_retail_analytics.py
That's it. The script downloads the data, runs all six phases, and writes outputs to the folders listed above.

How the pipeline is structured
Phase 1 — RDD Operations
Loads the raw sales CSV as a text RDD, parses each line, filters out bad records, then uses map and reduceByKey to compute total revenue per product. This phase is mainly about demonstrating the low-level Spark API before moving to DataFrames.

Phase 2 — DataFrames & Spark SQL
This is where the real analytical work happens. All three datasets are loaded with explicit schemas, cleaned, and registered as temp views. Then we run SQL queries for the business questions that matter:

total revenue by city
top 5 products by revenue
month-over-month sales trend
full enriched dataset (sales joined with customer and product dims)
Phase 3 — Medallion Lakehouse
Writes data through three layers:

Raw CSVs
   |
   v
Bronze  — raw Parquet, exactly as it arrived, no transformation
   |
   v
Silver  — cleaned, nulls dropped, sales partitioned by store_id
   |
   v
Gold    — four aggregated tables: city revenue, category revenue,
          customer segments, monthly trend
   |
   v
Power BI (reads from Gold via SQL Analytics Endpoint in Fabric)
Also simulates a Data Activator rule that fires when daily sales drop below the configured threshold.

Phase 4 — Structured Streaming
Generates synthetic sales event files and processes them with Spark Structured Streaming using 1-minute event-time windows. After the query finishes, it scans the output for demand spikes — products where the windowed quantity exceeds 2x the average. In production you'd replace the CSV source with an Event Hub or Kafka connector.

Phase 5 — Power BI Data Model
Exports powerbi_model_schema.json describing the star schema:

Fact table: Sales
Dimension: Customer (Sales[customer_id] → Customer[customer_id], Many-to-One)
Dimension: Product (Sales[product_id] → Product[product_id], Many-to-One)
In Fabric, Power BI connects directly to the Gold Lakehouse via the SQL Analytics Endpoint — no manual export needed. This file is for documentation and for teams importing Parquet files into Power BI Desktop.

Phase 6 — DAX Measures & Dashboard
Writes dashboard_spec.json with all DAX measures and a page-by-page dashboard layout. The measures are also logged so you can copy them directly into Power BI Desktop.

DAX measures
Total Sales =
    SUMX(Sales, Sales[price] * Sales[quantity])

Average Order Value =
    DIVIDE([Total Sales], DISTINCTCOUNT(Sales[transaction_id]), 0)

Sales Growth % =
    VAR ThisMonth = CALCULATE([Total Sales], DATESMTD(Sales[timestamp]))
    VAR LastMonth = CALCULATE([Total Sales], DATEADD(DATESMTD(Sales[timestamp]), -1, MONTH))
    RETURN DIVIDE(ThisMonth - LastMonth, LastMonth, 0)

Top Category =
    CALCULATE(
        FIRSTNONBLANK(Product[category], 1),
        TOPN(1, SUMMARIZE(Sales, Product[category], "rev", [Total Sales]), [rev], DESC)
    )
Full set of measures is in dashboard_spec.json.

Dashboard pages
Page	What's on it
Executive Overview	KPI cards — Total Sales, AOV, Growth %, Customer Count
Sales by Geography	Filled map + bar chart by city
Sales Trend	Line and area charts by month
Top Products	Bar chart by brand, treemap by category
Customer Segmentation	Donut chart + table by segment
A few design decisions worth noting
Explicit schemas everywhere — loading CSVs with inferSchema=True is convenient but fragile. If a source file changes column order or a value looks like a different type, Spark silently infers the wrong schema. Defining schemas explicitly catches that at load time.

Config class — all thresholds, paths, and Spark settings live in one place. Nothing is hardcoded inside functions. If the alert threshold changes from Rs 1000 to Rs 2000, you change one line.

Per-phase error handling — each phase runs inside its own try/except in main(). Phase 2 is the only hard dependency (everything downstream needs the cleaned DataFrames), so that one exits the script on failure. The rest log the error and continue so you still get partial output.

Streaming checkpoint — the checkpoint directory means the streaming query can resume from where it left off if it gets interrupted, rather than reprocessing everything from scratch.

Evaluation criteria mapping
Criteria	Where to look
PySpark Implementation (20%)	phase1_rdd_operations
DataFrame & SQL Usage (15%)	phase2_dataframes_sql
Fabric Architecture (20%)	phase3_lakehouse + lakehouse/ output
Real-time Analytics (10%)	phase4_streaming
Power BI Modelling (15%)	phase5_powerbi_model + powerbi_model_schema.json
DAX Measures (10%)	phase6_dax_dashboard + dashboard_spec.json
Visualization & Insights (10%)	dashboard_spec.json + this README
