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

## Required python version

Python version 3.11+ is required to run this code. 

## Creating demo input - an example wildcard table

### create a dataset & wildcard table

set some evironmental variables

```shell
export GCP_PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")

export GCP_LOCATION="europe-west2"

export BQ_DATASET="wildcard_stream_demo"

export BQ_TABLE_PREFIX="events_intraday"

export BQ_TABLE_DATE="20240618"

export BQ_POLL_RATE_SECONDS="5"

export BQ_SYNC_START="2024-06-18 9:49:26.776000+00:00"

export PUBSUB_TOPIC="wildcard_stream_topic"

export PUBSUB_SUB="wildcard_stream_subscription"

export PUBSUB_TIMEOUT="100"
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

### append initial data into the demo wildcard table

```shell
python bq_gen_data.py \
--gcp_project=${GCP_PROJECT_ID} \
--bq_dataset=${BQ_DATASET} \
--bq_table_prefix=${BQ_TABLE} \
--bq_table_date=${BQ_TABLE} \
--interval=1 \
--total=100
```

## Creating demo output - an example pub/sub topic

create a topic 

```shell
gcloud pubsub topics create ${PUBSUB_TOPIC}
```

create a subscription to this topic

```shell
gcloud pubsub subscriptions create ${PUBSUB_SUB} --topic ${PUBSUB_TOPIC}
```

pushing fake data to topic
```shell
gcloud pubsub topics publish ${PUBSUB_TOPIC} --message="hello3"
```


## Running micro batch streaming demo

### Demo 1-of-3: create stream of input into BigQuery

```shell
python bq_gen_data.py \
--gcp_project=${GCP_PROJECT_ID} \
--bq_dataset=${BQ_DATASET} \
--bq_table_prefix=${BQ_TABLE_PREFIX} \
--bq_table_date=${BQ_TABLE_DATE} \
--interval=1 \
--total=100
```

### Demo 2-of-3: micro batch BigQuery updates to Pub/Sub

```shell
python bq_stream_data.py \
--gcp_project=${GCP_PROJECT_ID} \
--bq_dataset=${BQ_DATASET} \
--bq_table_prefix=${BQ_TABLE_PREFIX} \
--bq_table_date=${BQ_TABLE_DATE} \
--poll_rate_s=${BQ_POLL_RATE_SECONDS} \
--this_sync_start="${BQ_SYNC_START}" \
--topic_id=${PUBSUB_TOPIC}
```

### Demo 3-of-3: check Pub/Sub topics has updates from BigQuery


```shell
python bq_check_stream.py \
--gcp_project=${GCP_PROJECT_ID} \
--pub_sub_subscription_id=${PUBSUB_SUB} \
--timeout=${PUBSUB_TIMEOUT}
```
