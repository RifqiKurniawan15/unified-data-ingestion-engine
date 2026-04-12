# 🎓 End-to-End Student Data Pipeline

This is a **Metadata-Driven ETL Pipeline** engineered with **Apache Airflow**, **Docker**, and **PostgreSQL**. The project implements a robust **Medallion Architecture**, providing a clear path for data as it flows from raw ingestion to business-ready insights.
By using a metadata-driven approach, this pipeline is highly scalable; new data sources can be onboarded simply by updating a configuration file, without modifying the core DAG code.
**🌟 Key Features**
**Metadata-Driven Workflow**: Tasks are dynamically generated from a central configuration (e.g., schedules.csv), allowing for rapid scaling and easy maintenance.
**Containerized Ingestion (Docker-in-Docker)**: Uses the DockerOperator to run ingestion scripts in isolated environments, ensuring dependency isolation and environment consistency.
**Idempotent Design**: Every stage of the pipeline is designed to be re-run safely. We use TRUNCATE and ROW_NUMBER() logic to ensure that data remains clean and unique, even after multiple executions.

**Medallion Architecture Implementation**:
**Bronze (Raw)**: Captures raw data from CSV/JSON sources "as-is" into landing tables.
**Silver (Cleansed)**: Deduplicates records, enforces schemas, and applies data-type casting using SQL Stored Procedures.
**Gold (Curated)**: Performs complex joins and aggregations to produce high-value Fact Tables for BI and reporting.

**Automated Error Logging**: Integrated PL/pgSQL error handling that captures and logs transformation failures into a dedicated audit table.

## 🏗️ Architecture
- **Bronze Layer**: Data ingestion dari CSV/JSON menggunakan Python di dalam Docker Container.
- **Silver Layer**: Data cleaning, casting, dan deduplikasi menggunakan PostgreSQL Stored Procedures.
- **Gold Layer**: Aggregated Fact Table untuk analisis performa kelas.

## 🛠️ Technology Stack
- **Orchestrator**: Apache Airflow
- **Database**: PostgreSQL 13
- **Containerization**: Docker & Docker Compose
- **Language**: Python & PL/pgSQL
  
📈 **Data Pipeline Flow**
**Extract**: Airflow triggers the DockerOperator to spin up a Python container that reads raw data and loads it into the Bronze schema.
**Transform (Silver)**: A PostgresOperator calls a stored procedure to clean the Bronze data and move it to the Silver schema, removing duplicates based on unique identifiers.
**Aggregate (Gold)**: Final transformations combine multiple Silver tables into a consolidated Gold Fact table (e.g., fact_class_performance), ready for visualization.
## 🚀 How to Run
1. Clone repository ini.
2. Salin `.env.example` menjadi `.env` dan sesuaikan path foldernya.
3. Jalankan `docker-compose up -d`.
4. Akses Airflow di `localhost:8081` dan buat koneksi `postgres_dwh`.
5. Trigger DAG `parralel_student_pipeline_00_00`.
