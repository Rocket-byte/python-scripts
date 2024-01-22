import os
from pymongo import UpdateOne
from tqdm import tqdm
import cx_Oracle

# Author: Ruslana Kruk
# This script integrates MongoDB and Oracle databases.
# It updates MongoDB document status and inserts documents into an Oracle table.

# Environment variables and connection setup
oracle_host = "enter_oracle_host"
oracle_port = "enter_oracle_port"
oracle_service_name = "enter_oracle_service_name"
oracle_user = "enter_oracle_user"
oracle_password = "enter_oracle_password"

mongo_db_name = "enter_mongo_db_name"
mongo_collection_name = "enter_mongo_collection_name"

dsn = cx_Oracle.makedsn(host=oracle_host, port=oracle_port, service_name=oracle_service_name)

# Update MongoDB document status in bulk
def update_mongo_status_bulk(document_ids, collection):
    bulk_operations = [UpdateOne({"_id": doc["_id"]}, {"$set": {"_scrape_status": 1}}) for doc in document_ids]
    collection.bulk_write(bulk_operations)

# Insert batch of documents into Oracle
def insert_batch_to_oracle(table_name, field_mapping, batch, cursor):
    try:
        batch_data = []
        for doc in batch:
            data = {dest: process_field(doc, src, data_type) for src, (dest, data_type) in field_mapping.items()}
            batch_data.append(data)

        placeholders = ', '.join([f':{dest}' for dest, _ in field_mapping.values()])
        field_list = ', '.join([f':{dest} AS {dest}' for dest, (dest, _) in field_mapping.items()])

        merge_statement = f"""
            MERGE INTO {table_name} dst
            USING (
                SELECT {field_list}
                FROM dual
            ) src
            ON (dst.ID = src.ID)
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f'dst.{dest} = src.{dest}' for dest, _ in field_mapping.values() if dest != "ID"])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join([dest for dest, _ in field_mapping.values()])})
                VALUES ({placeholders})
        """

        cursor.executemany(merge_statement, batch_data)
        cursor.connection.commit()
    except Exception as e:
        cursor.connection.rollback()
        raise e

# Process field data according to the mapping
def process_field(document, field_path, data_type):
    value = document
    for key in field_path.split('.'):
        if '[' in key:
            key, index = key.split('[')
            index = int(index.rstrip(']'))
            value = value.get(key, [])[index] if isinstance(value.get(key, []), list) else None
        else:
            value = value.get(key)

        if value is None:
            return None

    if data_type.startswith("VARCHAR2"):
        return str(value)[:4000]
    return value

# Main function to integrate MongoDB and Oracle
def insert_collections_to_oracle(collection_name, table_name, field_mapping, batch_size):
    os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

    with cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn) as oracle_connection, \
         oracle_connection.cursor() as oracle_cursor:

        mongo_client = MongoClient(mongo_db_name)
        mongo_collection = mongo_client[mongo_collection_name]

        query = {"_scrape_status": 0}
        total_documents = mongo_collection.count_documents(query)

        with tqdm(total=total_documents, desc=f"Inserting {collection_name} into {table_name}") as pbar:
            batch = []
            for doc in mongo_collection.find(query):
                batch.append(doc)
                if len(batch) >= batch_size:
                    insert_batch_to_oracle(table_name, field_mapping, batch, oracle_cursor)
                    update_mongo_status_bulk(batch, mongo_collection)
                    batch.clear()
                    pbar.update(batch_size)

            if batch:
                insert_batch_to_oracle(table_name, field_mapping, batch, oracle_cursor)
                update_mongo_status_bulk(batch, mongo_collection)
                pbar.update(len(batch))
