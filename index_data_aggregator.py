from datetime import datetime
import psycopg2
from tqdm import tqdm
from io import StringIO

# Author: Ruslana Kruk
# This script aggregates index data from multiple PostgreSQL nodes and inserts the aggregated data into a table.

# Node addresses and database credentials
node_addresses = ["p-prod0", "p-prod1", "p-prod2", "p-prod3"]
db_credentials = {"dbname": "xxxxxx", "user": "xxxxxxxx", "password": "secure_password"}  # Password anonymized

# SQL query
sql_query = """
    SELECT CURRENT_TIMESTAMP AS snapshot_datetime, srv_ip, relid, indexrelid, schemaname, relname, indexrelname,
    idx_scan, idx_tup_read, idx_tup_fetch FROM vw_index_by_node v
"""

# Function to connect and execute SQL query
def connect_and_execute_query(connection_info, query):
    with psycopg2.connect(**connection_info) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

# Function to insert data using copy_from
def insert_data_copy(node_address, data, column_names):
    node_db_credentials = db_credentials.copy()
    node_db_credentials["host"] = node_address

    with psycopg2.connect(**node_db_credentials) as conn:
        with conn.cursor() as cursor:
            data_io = StringIO()
            for row in data:
                data_io.write('\t'.join(map(str, row)) + '\n')
            data_io.seek(0)
            cursor.copy_from(data_io, 'index_data', columns=column_names, sep='\t')
            conn.commit()

print(f"Start index data collection at {datetime.now()}")

# Accumulate data from all nodes
accumulated_data = []
for node_address in tqdm(node_addresses, desc="Retrieving data"):
    node_data = connect_and_execute_query(db_credentials | {"host": node_address}, sql_query)
    accumulated_data.extend(node_data)

# Column names for the table
column_names = ["snapshot_datetime", "srv_ip", "relid", "indexrelid", "schemaname", "relname", "indexrelname",
                "idx_scan", "idx_tup_read", "idx_tup_fetch"]

# Batch size for data insertion
batch_size = 500
for i in tqdm(range(0, len(accumulated_data), batch_size), desc="Inserting data"):
    batch = accumulated_data[i:i + batch_size]
    insert_data_copy(node_addresses[0], batch, column_names)

print(f"Data accumulated and written to public.Index_DATA on {node_addresses[0]}")
print(f"End index data collection at {datetime.now()}")
