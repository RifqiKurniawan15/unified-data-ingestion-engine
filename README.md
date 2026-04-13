# 🎓 End-to-End Student Data Pipeline

This is a **Metadata-Driven ETL Pipeline** engineered with **Apache Airflow**, **Docker**, and **PostgreSQL**. The project implements a robust **Medallion Architecture**, providing a clear path for data as it flows from raw ingestion to business-ready insights.
By using a metadata-driven approach, this pipeline is highly scalable; new data sources can be onboarded simply by updating a configuration file, without modifying the core DAG code.

**🌟 Key Features**
**Metadata-Driven Workflow**: Tasks are dynamically generated from a central configuration (e.g., schedules.csv), allowing for rapid scaling and easy maintenance.

**Containerized Ingestion (Docker-in-Docker)**: Uses the DockerOperator to run ingestion scripts in isolated environments, ensuring dependency isolation and environment consistency.

**Idempotent Design**: Every stage of the pipeline is designed to be re-run safely. We use TRUNCATE and ROW_NUMBER() logic to ensure that data remains clean and unique, even after multiple executions.

**Medallion Architecture Implementation**:
Medallion Architecture Implementation: Bronze (Raw): Captures raw data from CSV/JSON sources "as-is" into landing tables. Silver (Cleansed): Deduplicates records, enforces schemas, and applies data-type casting using SQL Stored Procedures. Gold (Curated): Performs complex joins and aggregations to produce high-value Fact Tables for BI and reporting.

<img width="657" height="313" alt="image" src="https://github.com/user-attachments/assets/c8c85e6f-cc65-480e-ac17-4d368a0719de" />

## 🏗️ Architecture
**Bronze Layer (Raw)**: Automates data ingestion from multiple formats (CSV/JSON) using isolated Python environments within Docker containers to preserve raw data integrity.

<img width="364" height="158" alt="image" src="https://github.com/user-attachments/assets/f18c6da6-0be7-4a26-8bde-a33ea6b9440f" />


**Silver Layer (Cleansed)**: Executes data cleansing, schema enforcement, and record deduplication via optimized PostgreSQL Stored Procedures to ensure a "single source of truth."

<img width="468" height="156" alt="image" src="https://github.com/user-attachments/assets/1ab85b04-045b-409c-b71a-cf40c2867cc4" />


**Gold Layer (Curated)**: Transforms cleansed data into high-performance Aggregated Fact Tables, specifically modeled for classroom performance analytics and business intelligence.

<img width="746" height="155" alt="image" src="https://github.com/user-attachments/assets/e98161a8-d98a-4615-846e-a2c52768f446" />


**Automated Error Logging**: Integrated PL/pgSQL error handling that captures and logs transformation failures into a dedicated audit table.

<img width="347" height="286" alt="image" src="https://github.com/user-attachments/assets/396f9a4e-9505-44fb-a9ba-b40d3a1ae6ad" />

<img width="295" height="266" alt="image" src="https://github.com/user-attachments/assets/e5c13bcc-706d-4df3-b4e2-9788faaeec26" />


## 🛠️ Technology Stack
- **Orchestrator**: Apache Airflow
- **Database**: PostgreSQL 13
- **Containerization**: Docker & Docker Compose
- **Language**: Python & PL/pgSQL
  
## 📈 Data Pipeline Flow
**Extract**: Airflow triggers the DockerOperator to spin up a Python container that reads raw data and loads it into the Bronze schema.
**Transform (Silver)**: A PostgresOperator calls a stored procedure to clean the Bronze data and move it to the Silver schema, removing duplicates based on unique identifiers.
**Aggregate (Gold)**: Final transformations combine multiple Silver tables into a consolidated Gold Fact table (e.g., fact_class_performance), ready for visualization.

<img width="824" height="276" alt="image" src="https://github.com/user-attachments/assets/0fc6cc8f-eb19-484e-aea2-eee1163ec30a" />


## 🚀 How to Run
Follow these steps to get the pipeline up and running on your local machine:

**1. Clone the Repository**
git clone https://github.com/RifqiKurniawan15/unified-data-ingestion-engine.git
cd unified-data-ingestion-engine

**2. Configure Environment Variables**
Copy the provided example environment file and update the RAW_DATA_PATH to match your local directory path:

cp .env.example .env
Note: Open .env and ensure all database credentials and file paths are correct.

**3. Spin Up the Infrastructure**
Deploy the Airflow and PostgreSQL containers using Docker Compose:

docker-compose up -d

**4. Set Up Airflow Connection**
Access the Airflow UI at http://localhost:8081 (Login: admin / admin).
Navigate to Admin > Connections.

Create a new connection named postgres_dwh with your PostgreSQL credentials.

**5. Execute the Pipeline**

- Locate the DAG named parralel_student_pipeline_00_00.
- Unpause the DAG and click the Trigger DAG button to start the ETL process.

## 📦  Miscellaneous
Project Structure

<img width="274" height="134" alt="image" src="https://github.com/user-attachments/assets/7cdee1a8-3d16-433f-a34a-91f3e4cfd89d" />
