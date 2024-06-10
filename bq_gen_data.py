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
import time

######################################################################################## 
def gen_data(gcp_project, bq_dataset, bq_table, interval, total):

    # create a BigQuery client
    client = bigquery.Client()
    # Configure the query job.
    job_config = bigquery.QueryJobConfig()
    job_config.destination = f"{gcp_project}.{bq_dataset}.{bq_table}"
    job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
    # Generate some synthetic data to be appended to the taget table
    query = """
       SELECT
         UNIX_MICROS(CURRENT_TIMESTAMP()) as event_timestamp, 
         CONCAT(
         CAST(ROUND(100 + RAND() * (100-1)) AS string), "-",
         CAST(ROUND(100 + RAND() * (100-1)) AS string), "-",
         CAST(ROUND(100 + RAND() * (100-1)) AS string)) AS user_id,
         'mobile' as device_category,
         'us' as geo_country,
         ROUND(RAND() * 100.0) as ecommerce_purchase_revenue_in_usd
    """    
    for i in range(total):
        # Run the query.
        client.query_and_wait( query, job_config=job_config)
        print(f"generated record {i+1} out of {total}")
        time.sleep(interval)

########################################################################################
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
    parser.add_argument(
        '--total', 
        type=int,
        help='total number of records to generate'
        )    
    args = parser.parse_args()
    gen_data(
        args.gcp_project,
        args.bq_dataset,
        args.bq_table,
        args.interval,
        args.total
    )
