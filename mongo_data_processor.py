from datetime import datetime
from helpers_json import correct_datetime_fields, remove_extra_fields, deep_compare_json
from scrape_helpers import db

# Author: Ruslana Kruk
# This script processes and writes data to MongoDB collections, handling users, statistics, and activity data.

# MongoDB collections
collection_file_stacks = db["file_stacks"]
collection_users = db["users"]
collection_statistics = db["statistics"]
collection_activity = db["activity"]

# Field names
field_date_create = "_scrape_date_create"
field_date_update = "_scrape_date_update"
field_update_status = "_scrape_status"

# Add work fields to the document
def add_work_fields(document, add_create_date):
    if add_create_date:
        document[field_date_create] = datetime.now()
    document[field_date_update] = datetime.now()
    document[field_update_status] = 0

# Prepare file stack document by removing specific keys
def prepare_fs_document(document):
    keys_to_remove = ["user", "crop", "dominantColor"]
    for key in keys_to_remove:
        document.pop(key, None)
    correct_datetime_fields(document)

# Write file stack data to MongoDB
def write_fs_to_mongo(documents, counters):
    for document in documents:
        document_id = document["_id"]
        existing_document = collection_file_stacks.find_one({"_id": document_id})
        update_or_insert_document(existing_document, document, collection_file_stacks, counters, "fs")

# Prepare user document by removing specific keys
def prepare_user_document(document):
    keys_to_remove = ["follow_user_ids", "subscribed_user_ids", "subscriber_user_ids", "user_pivot"]
    for key in keys_to_remove:
        document.pop(key, None)
    correct_datetime_fields(document)

# Write user data to MongoDB
def write_user_to_mongo(document, counters):
    user_id = document["_id"]
    existing_document = collection_users.find_one({"_id": user_id})
    update_or_insert_document(existing_document, document, collection_users, counters, "user")

# Write statistics to MongoDB
def write_statistic_to_mongo(documents, account_id, counters):
    for document in documents:
        document_id = document["_id"]
        document["account_id"] = account_id
        correct_datetime_fields(document)
        existing_document = collection_statistics.find_one({"_id": document_id})
        update_or_insert_document(existing_document, document, collection_statistics, counters, "statistic")

# Write activity data to MongoDB and return the minimum date
def write_activity_to_mongo(documents, account_id, counters):
    min_date = datetime.now()
    for document in documents:
        document_id = document["_id"]
        document["account_id"] = account_id
        correct_datetime_fields(document)
        existing_document = collection_activity.find_one({"_id": document_id})
        if existing_document:
            if min_date > document["updated_at"]:
                min_date = document["updated_at"]
        update_or_insert_document(existing_document, document, collection_activity, counters, "activity")
    return min_date

# General function to update or insert document in MongoDB
def update_or_insert_document(existing_document, new_document, collection, counters, counter_type):
    if existing_document:
        existing_document_clear = remove_extra_fields(existing_document, new_document)
        if not deep_compare_json(existing_document_clear, new_document):
            add_work_fields(new_document, add_create_date=False)
            collection.update_one({"_id": new_document["_id"]}, {"$set": new_document}, upsert=True)
            counters[f'{counter_type} Updated'] += 1
        else:
            counters[f'{counter_type} Unchanged'] += 1
    else:
        add_work_fields(new_document, add_create_date=True)
        collection.update_one({"_id": new_document["_id"]}, {"$set": new_document}, upsert=True)
        counters[f'{counter_type} Inserted'] += 1
