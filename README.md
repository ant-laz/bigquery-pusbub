# Streaming BigQuery wildcard tables to Pub/Sub
Demonstrates the usage of BigQuery change history TVF to move new records in BQ to Pub/Sub

## Why this repo ? 

Announced at Next '24, BigQuery now has [continuous queries.](https://www.youtube.com/watch?v=Zo_y34J16yg)

Given a BigQuery table, continuous queries allows for newly appended records to be
streamed to a Pub/Sub topic. 

However, this does not work with all possible BigQuery tables. 

For example GA4 [exports](https://support.google.com/analytics/answer/7029846?sjid=4108693678746207990-EU)
to BigQuery produce [wildcard](https://cloud.google.com/bigquery/docs/querying-wildcard-tables) tables: 

* events_YYYYMMDD
* events_intraday_YYYYMMDD

BigQuery's continuous queries feature does not work with wildcard tables. 

This repo demonstrates the usage of BigQuery [change history TVF](https://cloud.google.com/bigquery/docs/change-history)
to stream, in a micro batch manner, newly appended records to a wildcard table to Pub/Sub.


## Creating demo input - an example wildcard table

### create an dataset & table

set some evironmental variables

```shell
export GCP_PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")

export GCP_PROJECT_NUM=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectNumber)")

export DEMO_DATE="20240610"

export GCP_LOCATION="europe-west2"

export BQ_DATASET="wildcard_stream_demo"

export BQ_TABLE="events_intraday_${DEMO_DATE}"
```

create a dataset for this demo
```shell
bq --location=${GCP_LOCATION} mk \
--dataset \
${GCP_PROJECT_ID}:${BQ_DATASET}

```

create a wildcard table for this demo
```shell
bq mk \
 --table \
 --description "Demo wildcard table" \
 ${GCP_PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}
 event_timestamp:INTEGER,user_id:STRING,device_category:STRING,geo_country:STRING,ecommerce_purchase_revenue_in_usd:FLOAT
```

### append data into the demo wildcard table

```shell
python bq_gen_data.py \
--gcp_project=${GCP_PROJECT_ID} \
--bq_dataset=${BQ_DATASET} \
--bq_table=${BQ_TABLE} \
--interval=1 \
--total=10
```

## Creating demo app - the core program that does the micro-batch streaming

```shell
python bq_stream_data.py \
--gcp_project=${GCP_PROJECT_ID} \
--bq_dataset=${BQ_DATASET} \
--bq_table=${BQ_TABLE}
```