import pandas as pd
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
import time
import os

# -------- Configuration --------

# Database information
user = 'g6_qf5214_2'
password = 'qf5214_2'
host = 'rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com'
port = 3306
db = 'nasa_power'
table_name = 'radiation'

# CSV file path
csv_file = 'US_Radiation_20180101_20241231_Daily.csv'

# Number of rows per batch
chunksize = 50000

# -------- Main Script --------

# Check CSV columns
df_head = pd.read_csv(csv_file, nrows=1)
csv_columns = df_head.columns.tolist()

print("CSV Columns:", csv_columns)

# Establish database connection
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4')

# Get table columns from database
with engine.connect() as conn:
    result = conn.execute(f"DESCRIBE {table_name}")
    db_columns = [row[0] for row in result.fetchall()]
print("Table Columns:", db_columns)

# Check column consistency
if csv_columns != db_columns:
    raise ValueError("CSV columns do not match the database table columns")

# Count total rows
total_rows = sum(1 for _ in open(csv_file)) - 1

print(f"Total rows to import: {total_rows}")

error_log = []

start_time = time.time()

with tqdm(total=total_rows, desc="Import Progress") as pbar:
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        try:
            chunk = chunk.fillna(0)  # Fill NaN with 0
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            print("Error occurred, logged.")
            error_log.append({"error": str(e), "data": chunk})
        pbar.update(len(chunk))

end_time = time.time()

print(f"Import completed in {(end_time - start_time) / 60:.2f} minutes")

# Save error logs if exists
if error_log:
    error_dir = "import_errors"
    os.makedirs(error_dir, exist_ok=True)
    for idx, log in enumerate(error_log):
        log["data"].to_csv(f"{error_dir}/error_chunk_{idx}.csv", index=False)
    print(f"{len(error_log)} error chunks saved to {error_dir}/")
else:
    print("All data imported successfully without errors.")

