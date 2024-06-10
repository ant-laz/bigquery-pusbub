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
from google.cloud import bigquery

def process_data_in_bq():
    client = bigquery.Client()

    # Perform a query.
    QUERY = (
        'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
        'WHERE state = "TX" AND name IS NOT NULL '
        'LIMIT 100')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        print(row.name)    

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
        '--interval', 
        type=int,
        help='seconds between appending new records'
        )
    args = parser.parse_args()
    process_data_in_bq()