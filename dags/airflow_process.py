from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import csv
from io import StringIO
import pendulum
import logging
import os

# ================================================================================================ # 
# 1️⃣ FUNCTION DECLARATION
# ================================================================================================ # 
jkt_timezone = pendulum.timezone("Asia/Jakarta")
raw_data_path_env = os.getenv('RAW_DATA_PATH')
if raw_data_path_env:
    dynamic_raw_data_path = raw_data_path_env.replace('\\', '/')
else:
    # Logic fallback jika dijalankan lokal tanpa docker-compose
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    dynamic_raw_data_path = os.path.join(parent_dir, 'raw_data').replace('\\', '/')
    
def get_task_config():
    """Read task configurations from CSV file in S3"""
    tasks = []
    target_sched = "00:00"
    file_path = "/opt/airflow/dags/schedules.csv" 

    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                schedules = [s.strip() for s in row["schedule"].split("|")]
                
                if target_sched in schedules:
                    tasks.append({
                        "task_name": row["task_name"],
                        "parent_list": row["parent_list"],
                        "process_type": row["process_type"].lower().strip(),
                        "parameter": row["parameter"]
                    })
        return tasks
    except Exception as e:
        logging.error(f"Error reading CSV: {str(e)}")
        return []
    
# =========================================================
# 2️⃣DEFAULT ARGS (retry for all tasks)
# =========================================================
retries = 2
max_active_runs = 1
max_active_tasks = 3

default_args = {
    "retries": retries,
    "retry_delay": timedelta(minutes=2)
}

# ================================================================================================ #
# 3️⃣ AIRFLOW DAG DEFINITION
# ================================================================================================ #
with DAG(
    dag_id='parralel_student_pipeline_00_00',
    start_date=pendulum.datetime(2026, 2, 1, tz=jkt_timezone),
    schedule='0 0 * * 2-6',
    catchup=False,
    max_active_runs=max_active_runs,
    max_active_tasks=max_active_tasks,
    default_args=default_args
) as dag:   
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Load task configuration
    task_configs = get_task_config()

    # Create dynamic tasks
    tasks = {}
    script_args = {}
    for config in task_configs:
        task_id = config['task_name']
        proc_type = config['process_type']
        param = config['parameter']
        
        # --- JALUR 1: PYTHON (DOCKER) ---
        if proc_type == 'python':
            parts = [p.strip() for p in param.split("|")]
            if len(parts) >= 3:
                # Ambil koneksi & target (split cuma 5 kali supaya sisanya tetap utuh)
                # args: [user, host, port, dbname, table_name, url]
                args = parts[2].split(".", 5) 
                
                tasks[task_id] = DockerOperator(
                    task_id=task_id,
                    image=parts[0],
                    container_name=f"{parts[1]}_{task_id}_{dag.dag_id}",
                    auto_remove=True,
                    mount_tmp_dir=False,
                    mounts=[
                    {
                        "source": dynamic_raw_data_path, 
                        "target": "/app/raw_data", 
                        "type": "bind",
                    }
                    ],
                    command=[
                        f"--user={args[0]}",
                        f"--password=root",
                        f"--host={args[1]}",
                        f"--port={args[2]}",
                        f"--dbname={args[3]}",
                        f"--table_name={args[4]}",
                        f"--url={args[5]}" # Sekarang args[5] akan berisi 'raw_data/students.csv' secara utuh
                    ],
                    docker_url="unix://var/run/docker.sock",
                    network_mode="docker_default"
                )

        elif proc_type == 'postgres':
            # Anggap parameter isinya cuma nama procedure: "bronze.usp_move_to_silver()"
            tasks[task_id] = PostgresOperator(
                task_id=task_id,
                postgres_conn_id='postgres_dwh',
                sql=f"CALL {param};"
            )

    # Set dependencies based on parent_id
    for config in task_configs:
        t_id = config['task_name']
        parents = config['parent_list']
        if parents:
            for p in parents.split("|"):
                p = p.strip()
                if p in tasks:
                    tasks[p] >> tasks[t_id]

    # Identify root tasks (no parents)
    for config in task_configs:
        t_id = config['task_name']
        if not config['parent_list']: # Jika parent kosong
            start >> tasks[t_id]

    # Identify leaf tasks (not listed as any parent)
    all_parents = []
    for config in task_configs:
        if config['parent_list']:
            all_parents.extend([p.strip() for p in config['parent_list'].split("|")])
    
    for t_id in tasks:
        if t_id not in all_parents:
            tasks[t_id] >> end
