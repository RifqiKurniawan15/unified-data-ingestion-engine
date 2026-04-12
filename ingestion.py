import pandas as pd
import os
import shutil
from sqlalchemy import create_engine, text
import argparse

class UnifiedIngestor:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self._prepare_db()

    def _prepare_db(self):
        """Creates the schema and the error tracking table if they don't exist."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bronze.rejected_records (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT,
                    raw_data TEXT,
                    error_message TEXT,
                    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.commit()

    def read_file(self, file_path):
        """Detects file extension and reads the file into a DataFrame."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            return pd.read_csv(file_path)
        elif ext == '.json':
            return pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def _move_file(self, source_path, status):
        """Moves the file to a 'success' or 'reject' subfolder."""
        base_dir = os.path.dirname(source_path)
        file_name = os.path.basename(source_path)
        
        # Define and create the target directory (success/reject)
        target_dir = os.path.join(base_dir, status)
        os.makedirs(target_dir, exist_ok=True)
        
        # Move the file
        destination_path = os.path.join(target_dir, file_name)
        shutil.move(source_path, destination_path)
        print(f"📁 File moved to: {target_dir}")

    def ingest(self, relative_file_path, table_name):
        """Main pipeline: Reads, ingests, handles errors, and moves the file."""
        # Create an absolute path relative to where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(script_dir, relative_file_path)
    
        if not os.path.exists(full_path):
            print(f"❌ Error: File not found at {full_path}")
            return

        file_name = os.path.basename(full_path)
        df = self.read_file(full_path)
        
        # Standardize column names: lowercase and replace spaces with underscores
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        # Use this flag to decide which folder the file goes to
        has_error = False

        try:
            # Step 1: Attempt fast Bulk Insert
            df.to_sql(table_name, self.engine, schema='bronze', if_exists='append', index=False)
            print(f"✅ Successfully ingested {file_name} via bulk insert.")
        except Exception as e:
            # Step 2: If bulk fails, process row-by-row to find specific issues
            print(f"⚠️ Bulk insert failed for {file_name}. Switching to row-level processing...")
            has_error = True
            for _, row in df.iterrows():
                try:
                    row_df = pd.DataFrame([row])
                    row_df.to_sql(table_name, self.engine, schema='bronze', if_exists='append', index=False)
                except Exception as row_err:
                    self.log_error(file_name, row.to_json(), str(row_err))

        # Step 3: Organize the file based on the outcome
        final_status = 'reject' if has_error else 'success'
        self._move_file(full_path, final_status)

    def log_error(self, file_name, raw_data, error_msg):
        """Inserts failed rows into the rejected_records table."""
        query = text("""
            INSERT INTO bronze.rejected_records (file_name, raw_data, error_message)
            VALUES (:file, :data, :error)
        """)
        with self.engine.connect() as conn:
            conn.execute(query, {"file": file_name, "data": raw_data, "error": error_msg})
            conn.commit()

# --- Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process ingestion file')

    parser.add_argument('--user', required=True, help='username')
    parser.add_argument('--password', required=True, help='password')
    parser.add_argument('--host', required=True, help='host')
    parser.add_argument('--port', required=True, help='port')
    parser.add_argument('--dbname', required=True, help='database')
    parser.add_argument('--table_name', required=True, help='tablename')
    parser.add_argument('--url', required=True, help='raw data url')

    args = parser.parse_args()
    # Update with your actual Docker database credentials
    DB_URL = f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}'
    ingestor = UnifiedIngestor(DB_URL)
    
    # Run the ingestion for a specific file inside your raw_data folder
    ingestor.ingest(args.url, args.table_name)
    # ingestor.ingest('raw_data/students.csv', 'stg_students')