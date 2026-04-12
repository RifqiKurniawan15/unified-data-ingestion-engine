FROM python:3.11.1

# 1. Update and install system-level dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python libraries
RUN pip install pandas sqlalchemy argparse psycopg2

WORKDIR /app

# 3. Copy your script (and your data folder if it's local!)
COPY ingestion.py ingestion.py
# If your csv is in a local folder called raw_data, you need this too:
COPY raw_data/ /app/raw_data/ 

ENTRYPOINT ["python", "ingestion.py"]