import os

# Set the path to the uploaded service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/content/encouxxxxxxxxxxxxxxxxxxx016.json"

from google.cloud import pubsub_v1
import json
import time
import pandas as pd

# Set the Google Cloud project ID and Pub/Sub topic name
project_id = "enco****************500"
topic_name = "strea***************_bq"

# Create a publisher client
publisher = pubsub_v1.PublisherClient()

# Get the full topic path
topic_path = publisher.topic_path(project_id, topic_name)

# Define a function to generate and publish data in JSON format
def publish_json_data():
    df1 = pd.read_csv('/content/data.csv')
    for i in range(100):
        # Convert all values to strings
        sales_live_data = df1.iloc[i].apply(lambda x: str(x)).to_dict()
        # Convert data to JSON
        json_data = json.dumps(sales_live_data)
        # Publish JSON data to the topic
        future = publisher.publish(topic_path, json_data.encode("utf-8"))
        print(sales_live_data)
        time.sleep(10)  # Sleep for 10 seconds between messages

# Call the function to publish JSON data
publish_json_data()
