# 🎓 End-to-End Student Data Pipeline

Project ETL dinamis menggunakan **Airflow**, **Docker**, dan **Postgres** dengan implementasi **Medallion Architecture**.

## 🏗️ Architecture
- **Bronze Layer**: Data ingestion dari CSV/JSON menggunakan Python di dalam Docker Container.
- **Silver Layer**: Data cleaning, casting, dan deduplikasi menggunakan PostgreSQL Stored Procedures.
- **Gold Layer**: Aggregated Fact Table untuk analisis performa kelas.

## 🛠️ Tech Stack
- **Orchestrator**: Apache Airflow
- **Database**: PostgreSQL 13
- **Containerization**: Docker & Docker Compose
- **Language**: Python & PL/pgSQL

## 🚀 How to Run
1. Clone repository ini.
2. Salin `.env.example` menjadi `.env` dan sesuaikan path foldernya.
3. Jalankan `docker-compose up -d`.
4. Akses Airflow di `localhost:8081` dan buat koneksi `postgres_dwh`.
5. Trigger DAG `parralel_student_pipeline_00_00`.