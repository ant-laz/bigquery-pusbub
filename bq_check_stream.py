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

# based on 
# https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import argparse
import json

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = message.data.decode("utf-8")
    data = json.loads(data)
    uid = data["user_id"]
    ets = data["event_timestamp"]
    print(f"Received msg from BQ: user_id={uid}, event_ts={ets}")

    message.ack()

def read_pub_sub_data(project_id, subscription_id, timeout):

    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Reading data from pub/sub topic.'
        )
    parser.add_argument(
        '--gcp_project',  
        type=str, 
        help='target gcp project'
        )
    parser.add_argument(
        '--pub_sub_subscription_id',  
        type=str, 
        help='target bigquery dataset'
        )
    parser.add_argument(
        '--timeout',  
        type=str, 
        help='Number of seconds the subscriber should listen for messages'
        )        
    args = parser.parse_args()
    read_pub_sub_data(
        args.gcp_project,
        args.pub_sub_subscription_id,
        int(args.timeout)
    )