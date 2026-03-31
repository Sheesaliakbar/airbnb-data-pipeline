Built a production-grade, end-to-end ELT pipeline that processes 12 months of Sydney Airbnb listing data using a Modern Data Stack and Medallion Architecture — automating the full journey from raw CSV files all the way to business-ready analytics marts.

This project was driven by a real question: how do you take messy, inconsistent, multi-source data and turn it into something that analysts and decision-makers can actually trust and use? The answer is a well-structured data pipeline with clear separation between raw ingestion, cleaning, and business logic — and that is exactly what this pipeline delivers.

---

🏗️ ARCHITECTURE OVERVIEW

The pipeline follows the Medallion Architecture pattern — a layered approach to data quality that ensures every transformation is intentional, traceable, and reversible.

The full flow looks like this:

CSV Files + Census Data + LGA Mappings → Apache Airflow → PostgreSQL (Bronze) → dbt Staging (Silver) → dbt Marts (Gold) → Analytics

Each layer has a single, well-defined responsibility. Data only moves forward when it meets the quality standards of the next layer. Nothing is overwritten — the full history is always preserved.

---

📥 LAYER 1 — DATA SOURCES

The pipeline ingests data from three distinct sources — each with its own structure, format, and quality challenges:

The first source is 12 monthly CSV files containing Airbnb property listings for Sydney, Australia. Each file covers one month of listing data — property details, host information, pricing, availability, and review scores. The challenge here is consistency — the same field can have different formatting across different monthly files. Prices contain currency symbols. Dates appear in multiple formats. Some fields are present in certain months but missing in others.

The second source is Census demographic data — suburb-level population, income, and household statistics. This data is used to enrich the listing data with the socioeconomic context of each neighbourhood.

The third source is LGA (Local Government Area) mapping data — a geographic reference table that maps Sydney suburbs to their respective local government areas. This enables neighbourhood-level aggregations and regional comparisons in the Gold layer.

Bringing these three sources together in a consistent, reliable way is what makes this pipeline genuinely complex — and genuinely useful.

---

⚙️ LAYER 2 — ORCHESTRATION (Apache Airflow)

Apache Airflow is the orchestration engine that drives the entire pipeline. It schedules, monitors, and manages the execution of every task — from initial data ingestion all the way through to dbt transformation runs.

The most technically interesting aspect of the Airflow implementation is the use of dynamic DAG generation. Rather than writing a separate DAG for each of the 12 monthly CSV files — which would mean 12 separate pieces of code doing essentially the same thing — I used Python loops inside Airflow to dynamically generate ingestion tasks at runtime.

A single DAG template handles all 12 months automatically. The loop iterates over a list of monthly file paths, and for each file, Airflow generates a corresponding ingestion task. When a new monthly file becomes available, adding it to the pipeline requires changing a single line of configuration — not writing new code.

This approach is significantly more maintainable than hardcoded DAGs. It also demonstrates a core principle of good data engineering — building systems that are easy to extend, not just easy to build the first time.

Airflow also orchestrates the dbt runs that follow each ingestion cycle — ensuring that Silver and Gold layer transformations always execute after fresh data has been loaded into Bronze. The full pipeline is automated end-to-end, with task dependencies defined explicitly so that no transformation runs on incomplete data.

---

🥉 LAYER 3 — BRONZE (Raw Ingestion into PostgreSQL)

The Bronze layer is the raw landing zone — data arrives here exactly as it exists in the source files, with no transformations applied. Every CSV row, every Census record, every LGA mapping is loaded into PostgreSQL as-is.

Python scripts using Pandas and SQLAlchemy handle the actual ingestion. Pandas reads each CSV file into a DataFrame, and SQLAlchemy manages the database connection and write operations. The scripts are intentionally kept simple at this stage — the goal is faithful ingestion, not transformation.

The entire PostgreSQL database runs inside a Docker container, managed by Docker Compose. This means the infrastructure is fully reproducible — anyone can clone the repository and have the complete Bronze layer environment running locally within minutes.

Keeping raw data in Bronze is a critical architectural decision. It means that if a transformation in Silver or Gold turns out to be wrong, you can always go back to the original data and rebuild. Nothing is ever lost. This is especially important when working with 12 months of historical data where fixing a mistake later is far more costly than preserving the raw data upfront.

---

🥈 LAYER 4 — SILVER (Cleaning and Standardization with dbt)

The Silver layer is where raw data becomes trustworthy data. dbt (data build tool) powers all transformations at this stage, using modular SQL models written with Jinja templating.

The Silver layer handles several categories of data quality issues:

Deduplication: When ingesting 12 months of data from separate files, the same listing can appear in multiple months. Silver models identify and remove duplicate records while preserving the most recent version of each listing.

Currency and numeric cleaning: Airbnb listing prices in the raw data contain currency symbols — values like "$150.00" or "$1,200" that cannot be used for numeric calculations. Silver models strip these symbols and cast the values to proper decimal types.

Null value handling: Many fields in the raw data contain nulls — some intentional, some representing missing data that should default to a known value. Silver models apply consistent null-handling logic across all tables, replacing nulls with appropriate defaults where necessary and flagging records where nulls indicate genuine data quality issues.

Date standardization: Dates appear in multiple formats across different monthly files. Silver models parse all date strings into a single standardized format, enabling reliable date-based filtering and calculations in the Gold layer.

Schema enforcement: Column names, data types, and field structures are standardized across all monthly files so that the Gold layer can query them as a single consistent dataset.

One of the most architecturally significant features of the Silver layer is the implementation of SCD Type 2 (Slowly Changing Dimension Type 2) tracking using dbt Snapshots. Airbnb listing prices and availability change over time — a property listed at $120 per night in January might be $150 in June. SCD Type 2 preserves the full history of these changes by maintaining a record for every version of each listing, with valid_from and valid_to timestamps marking the period during which each version was active.

This means the pipeline does not just show you what listings look like today — it shows you exactly what every listing looked like at any point in the past 12 months. This is essential for accurate trend analysis and for understanding how pricing has evolved over time.

---

🥇 LAYER 5 — GOLD (Business Analytics with dbt)

The Gold layer contains the business-ready aggregations that analysts and dashboards consume directly. dbt mart models transform the clean Silver data into purpose-built tables optimized for specific analytical questions.

Three core mart models power the Gold layer:

Host Performance Mart: Aggregates listing data at the host level — total listings per host, average review scores, occupancy rates, pricing trends, and revenue estimates. This mart answers questions like: which hosts are consistently highly rated? Which hosts have the most dynamic pricing strategies?

Neighbourhood Analytics Mart: Aggregates listing data at the suburb and LGA level, joined with Census demographic data. This mart enables geographic analysis — how does pricing vary across different parts of Sydney? Is there a correlation between neighbourhood income levels and listing prices?

Property Type Trends Mart: Tracks how different property types — entire apartments, private rooms, shared spaces — have trended in price and availability over the 12-month period. This mart uses window functions extensively to calculate month-over-month changes.

The Gold layer makes heavy use of advanced SQL window functions:

LAG() is used to calculate month-over-month growth rates — comparing each month's metrics to the previous month's values without requiring a self-join. This enables clean, efficient trend calculations across the full 12-month dataset.

PERCENTILE_CONT() is used to calculate median pricing — a more robust measure of central tendency than the mean for a dataset like Airbnb listings, where extreme outliers (ultra-luxury properties) can significantly skew averages.

Both of these functions are implemented as dbt models — meaning they are versioned, tested, documented, and reproducible.

---

🔧 CUSTOM dbt MACROS

One of the more advanced features of the dbt implementation is the use of custom Jinja macros for schema management.

The pipeline maintains separate schemas for staging (Silver) and mart (Gold) models in PostgreSQL — keeping transformed data clearly separated from raw ingestion data. Rather than hardcoding schema names throughout the SQL models, I built a custom dbt macro that generates the correct schema name dynamically based on the environment.

This means the same dbt project can be deployed against a development database, a staging database, and a production database — each with its own schema — without changing a single line of SQL. The macro handles the environment-specific configuration automatically.

This is the kind of infrastructure thinking that separates production data engineering from ad-hoc scripts. It makes the pipeline safe to iterate on without risking production data.

---

🐳 INFRASTRUCTURE — Docker Compose

The entire infrastructure — PostgreSQL, Airflow, and all supporting services — runs inside Docker containers orchestrated by Docker Compose. This means the full pipeline is portable and reproducible across any machine.

Running the complete local environment requires a single command:

docker-compose up -d

After that, the Airflow UI is available at localhost:8080, and PostgreSQL is accessible at localhost:5432. No manual installation of any tool is required.

---

🛠️ FULL TECH STACK

Python · Apache Airflow · dbt · PostgreSQL · Docker · Docker Compose · Pandas · SQLAlchemy · Jinja SQL

---

📊 WHAT THIS PIPELINE DELIVERS

By the time data reaches the Gold layer, the following business questions can be answered reliably:

- How have Airbnb listing prices in Sydney changed month-over-month over the past year?
- Which Sydney neighbourhoods have the highest occupancy rates?
- Which host categories generate the most consistent revenue?
- How do listing prices correlate with suburb-level demographics?
- Which property types have seen the fastest price growth over 12 months?

These are not simple questions to answer from raw CSV files. They require clean data, consistent schemas, proper historical tracking, and efficient aggregations. That is exactly what this pipeline provides.

---

🔗 Links

GitHub: https://github.com/Sheesaliakbar/airbnb-data-pipeline
Live Architecture: https://sheesaliakbar.github.io/airbnb-data-pipeline/airbnb-pipeline.html
