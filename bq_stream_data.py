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

def process_data_in_bq(gcp_project, bq_dataset, bq_table):
    client = bigquery.Client()
    now = datetime.datetime.now()
    now_str = str(now)
    # Perform a query.
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
            APPENDS(TABLE `{gcp_project}.{bq_dataset}.{bq_table}`, 
                    NULL, --start_timestamp
                    PARSE_TIMESTAMP("%F %H:%M:%E*S", "{now_str}") --end_timestamp
                    );
    """
    print(QUERY)      
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        print(row)    

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
    args = parser.parse_args()
    process_data_in_bq(
        args.gcp_project,
        args.bq_dataset,
        args.bq_table
    )