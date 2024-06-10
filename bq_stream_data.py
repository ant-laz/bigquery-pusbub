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
from google.cloud import bigquery

def process_data_in_bq(gcp_project, bq_dataset, bq_table, this_sync_start):
    client = bigquery.Client()
    now = datetime.datetime.now()
    this_sync_end = str(now)
    while(True):
        # grab all rows inserted into table since last_sync_time
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
                TABLE `{gcp_project}.{bq_dataset}.{bq_table}`, 
                PARSE_TIMESTAMP("%F %H:%M:%E*S", "{this_sync_start}"), --start_timestamp
                PARSE_TIMESTAMP("%F %H:%M:%E*S", "{this_sync_end}")    --end_timestamp
                );
        """
        print(QUERY)
        query_job = client.query(QUERY)
        rows = query_job.result()
        # process data retreived
        for row in rows:
            print(row)
        # move sync windows forward
        now = datetime.datetime.now()
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
        '--bq_table',  
        type=str, 
        help='target bigquery table'
        )  
    parser.add_argument(
        '--this_sync_start',  
        type=str, 
        help='time to start this sync from'
        )            
    args = parser.parse_args()
    process_data_in_bq(
        args.gcp_project,
        args.bq_dataset,
        args.bq_table,
        args.this_sync_start
    )