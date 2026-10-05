from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import numpy as np

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
project_id = "project-2edd459c-6e8e-42b5-9dd"
topic_name = "csv-produced"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")


def msg_from_row(ID, row):
    temp = float(row["temperature"]) if row["temperature"].strip() else None
    hum = float(row["humidity"]) if row["humidity"].strip() else None
    pres = float(row["pressure"]) if row["pressure"].strip() else None
    return {
        "ID": ID,
        "time": int(float(row["time"])),
        "profile_name": row["profileName"],
        "temperature": temp,
        "humidity": hum,
        "pressure": pres,
    }


ID = np.random.randint(0, 10000000)
with open("Labels.csv", "r") as f:
    data = csv.DictReader(f)
    for row in data:
        row = msg_from_row(ID, row)
        ID = ID + 1
        future = publisher.publish(topic_path, json.dumps(row).encode("utf-8"))
        future.result()
        print(f"Published message {row}")
