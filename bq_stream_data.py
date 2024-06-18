#    Copyright 2024 google.com

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import argparse
import datetime
from datetime import date, timedelta
import time
from google.cloud import bigquery, pubsub_v1
from google.api_core.exceptions import NotFound
import json

def process_newly_added_rows(rows, this_sync_start, this_sync_end, proj_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(proj_id, topic_id)
    print(f"{this_sync_start} to {this_sync_end} - {rows.total_rows} rows added in BQ")
    published_count = 0
    for row in rows:
        data = {}
        data["event_timestamp"] = row["event_timestamp"]
        data["user_id"] = row["user_id"]
        data["device_category"] = row["device_category"]
        data["geo_country"] = row["geo_country"]
        data["ecommerce_purchase_revenue_in_usd"] = row["ecommerce_purchase_revenue_in_usd"]
        data = json.dumps(data)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        published_count += 1
    print(f"{published_count} messages published to Pub/Sub (with BigQuery data)")

def get_table_id(gcp_project, bq_dataset, bq_table_prefix, bq_table_date):
    """
    As per the docs: 
    https://support.google.com/analytics/answer/7029846?sjid=4108693678746207990-EU
    A table events_intraday_YYYYMMDD is created. 
    This table is populated continuously as events are recorded throughout the day. 
    This table is deleted at the end of each day once events_YYYYMMDD is complete.

    This functions checks for the deletion of events_intraday_YYYYMMDD. 

    If deletion has occured, switch over to events_intraday_YYYYMMDD+1.
    """
    client = bigquery.Client()
    table_id = f"{gcp_project}.{bq_dataset}.{bq_table_prefix}_{bq_table_date}"
    try:
        client.get_table(table_id) 
        #proj.dataset.prefix_YYYYMMDD still exists, continue to use it 
        return table_id
    except NotFound:
        #proj.dataset.prefix_YYYYMMDD no longers exists
        #as per docs, assume auto-deletion
        #as per docs, switch to proj.dataset.prefix_YYYYMMDD+1
        deleted_date = date.fromiosformat(bq_table_date)
        new_date = deleted_date + timedelta(days=1)
        new_date_str = new_date.strftime("%Y%m%d")
        new_table_id = f"{gcp_project}.{bq_dataset}.{bq_table_prefix}_{new_date}"
        return new_table_id

def process_data_in_bq(
        gcp_project, 
        bq_dataset, 
        bq_table_prefix, 
        bq_table_date, 
        this_sync_start, 
        poll_rate_s,
        topic_id):
    client = bigquery.Client()
    now = datetime.datetime.now(datetime.timezone.utc)
    this_sync_end = str(now)
    while(True):
        bq_table = get_table_id(gcp_project, bq_dataset, bq_table_prefix, bq_table_date)
        QUERY = f"""
        SELECT
            event_timestamp,
            user_id,
            device_category,
            geo_country,
            ecommerce_purchase_revenue_in_usd,
            _CHANGE_TYPE AS change_type,
            _CHANGE_TIMESTAMP AS change_time
        FROM
            APPENDS(
                TABLE `{bq_table}`, 
                PARSE_TIMESTAMP("%F %H:%M:%E*S%Ez", "{this_sync_start}"), --start_ts
                PARSE_TIMESTAMP("%F %H:%M:%E*S%Ez", "{this_sync_end}")    --end_ts
                );
        """
        rows = client.query_and_wait(QUERY)    
        process_newly_added_rows(rows, this_sync_start, this_sync_end, gcp_project, topic_id)
        time.sleep(1.0*int(poll_rate_s))
        now = datetime.datetime.now(datetime.timezone.utc)
        this_sync_start = this_sync_end #the previous end becomes the next start
        this_sync_end = str(now)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate data in biquery.'
        )
    parser.add_argument(
        '--gcp_project',  
        type=str, 
        help='target gcp project'
        )
    parser.add_argument(
        '--bq_dataset',  
        type=str, 
        help='target bigquery dataset'
        )
    parser.add_argument(
        '--bq_table_prefix',  
        type=str, 
        help='target bigquery table prefix'
        ) 
    parser.add_argument(
        '--bq_table_date',  
        type=str, 
        help='target wildcard bigquery table suffix YYYMMDD'
        )      
    parser.add_argument(
        '--poll_rate_s',  
        type=str, 
        help='seconds between each check for newly added rows'
        ) 
    parser.add_argument(
        '--this_sync_start',  
        type=str, 
        help='time to start this sync from'
        )
    parser.add_argument(
        '--topic_id',  
        type=str, 
        help='the pub/sub topic to publish to'
        )                  
    args = parser.parse_args()
    process_data_in_bq(
        args.gcp_project,
        args.bq_dataset,
        args.bq_table_prefix,
        args.bq_table_date,
        args.this_sync_start,
        args.poll_rate_s,
        args.topic_id
    )