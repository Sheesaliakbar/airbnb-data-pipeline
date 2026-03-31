🏠 Airbnb End-to-End Data Pipeline (Sydney)


An automated ELT/ETL pipeline designed to process 12 months of Sydney Airbnb data. This project demonstrates a complete Modern Data Stack integration using Airflow for orchestration, dbt for modular SQL transformations, and PostgreSQL hosted in Docker containers.

🏗️ Architecture: The Medallion Approach
This project follows the Medallion Architecture to ensure data quality and reliability as it moves through the pipeline:

Bronze (Raw Layer): Initial ingestion of 12+ monthly CSV files, Census data, and LGA mappings into PostgreSQL using Python-based Airflow DAGs.

Silver (Staging Layer): Data cleaning, deduplication, and schema enforcement using dbt. This includes handling currency symbols, null values, and date formatting.

Gold (Mart Layer): High-level business aggregates and dimension tables (e.g., Host Performance, Neighborhood Analytics, and Property Type Trends).

🛠️ Technology Stack
Orchestration: Apache Airflow (Dockerized)

Transformation: dbt (data build tool)

Database: PostgreSQL

Infrastructure: Docker & Docker Compose

Language: Python (Pandas & SQLAlchemy) & SQL (Jinja)

🚀 Key Features
Dynamic DAG Generation: Uses Python loops in Airflow to dynamically create ingestion tasks for 12 months of historical data.

SCD Type 2 (Snapshots): Implemented dbt snapshots to track slowly changing dimensions in listing prices and census data over time.

Advanced SQL Analytics: Utilizes Window Functions (LAG, PERCENTILE_CONT) for month-over-month growth and median calculations.

Custom dbt Macros: Built a custom schema generator to manage separate staging and mart environments automatically.

📁 Project Structure
Plaintext
├── dags/                       # Airflow DAGs (Ingestion logic)
├── airbnb_project/             # dbt project directory
│   ├── models/                 # SQL transformations (Staging & Marts)
│   ├── snapshots/              # SCD Type 2 logic
│   └── macros/                 # Custom dbt functions
├── data/                       # Raw CSV source files (ignored by Git)
├── docker-compose.yaml         # Container orchestration
└── .gitignore                  # Keeps the repo clean of logs and temp files
⚙️ How to Run
Clone the repo:

Bash
git clone https://github.com/Sheesaliakbar/airbnb-data-pipeline.git
Start the environment:

Bash
docker-compose up -d
Access the tools:

Airflow UI: localhost:8080 (User/Pass: admin/admin)

Postgres: localhost:5432


🗺️ Interactive Architecture [👉 View Live Diagram](https://sheesaliakbar.github.io/airbnb-data-pipeline/airbnb-pipeline.html)
