from collections import OrderedDict
from datetime import datetime
import requests
from tqdm import tqdm

# Author: Ruslana Kruk
# This script scrapes activity data and writes to MongoDB, managing the scraping process for multiple accounts.

# Import authorization functions for different creators
from scrape_authorize import run_authorization
from scrape_authorize_creators import authorize_creator_01_lina, authorize_creator_02_bad_liz, \
    authorize_creator_03_young_mary, authorize_creator_04_princess, authorize_creator_05_anotherblonde
from scrape_mongo_writers import write_activity_to_mongo

# URL for activity scraping
activity_api_url = "enter_activity_api_url"  # The API URL for scraping activities

# Request and process activity data by limit
def request_by_limit(step_name, account_id, url_params, headers, offset_start, limit_size, rows_to_scan, days_to_scan):
    rows_scanned, offset, limit = 0, offset_start, limit_size
    fs_counters = OrderedDict([('Total', 0), ('Inserted', 0), ('Updated', 0), ('Unchanged', 0),
                               ('User Inserted', 0), ('User Updated', 0)])
    progress_desc_mask = "Scrape {step_name} | Limit: {limit} | Offset: {offset} | Rows: {rows}"
    progress_bar = tqdm(total=rows_to_scan // limit, desc=progress_desc_mask.format(step_name=step_name, limit=limit, offset=offset, rows=rows_scanned),
                        postfix=fs_counters, leave=True, smoothing=0.3)

    while rows_scanned < rows_to_scan:
        response = requests.get(activity_api_url, params={"offset": offset, "limit": limit, **url_params}, headers=headers)
        offset += limit
        rows_scanned += limit

        if response.status_code == 200:
            documents = response.json()
            min_date = write_activity_to_mongo(documents, account_id, fs_counters)
            progress_bar.set_description(progress_desc_mask.format(step_name=step_name, limit=limit, offset=offset, rows=rows_scanned))
            progress_bar.set_postfix(fs_counters)
            progress_bar.update()

            if (datetime.now() - min_date).days >= days_to_scan or len(documents) < limit_size:
                break
        else:
            print(f"Request failed with status code: {response.status_code}\t{response.text}")
            break

    progress_bar.close()

# Scrape activity for a specified number of days
def scrape_activity(authorize_function, days_to_scan):
    user_id, headers, cookies = authorize_function()
    scrape_headers = {"sort": '{"created_at":"desc"}'}
    request_by_limit(step_name=f"Statistics for {user_id}", account_id=user_id,
                     url_string=activity_api_url, url_params=scrape_headers, url_headers=headers,
                     offset_start=0, limit_size=40, rows_to_scan=999999, days_to_scan=days_to_scan)

# Main scraping process
days_to_scan = 4
for authorize_function in [authorize_creator_01_lina, authorize_creator_02_bad_liz, authorize_creator_03_young_mary, authorize_creator_04_princess, authorize_creator_05_anotherblonde]:
    scrape_activity(authorize_function, days_to_scan)
