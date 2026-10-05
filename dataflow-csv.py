from google.cloud import pubsub_v1
import glob
import json
import os

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "project-2edd459c-6e8e-42b5-9dd"
publisher = pubsub_v1.PublisherClient()
input_topic = "csv-injested"
input_topic_path = publisher.topic_path(project_id, input_topic)
output_topic = "csv-converted"
output_topic_path = publisher.topic_path(project_id, output_topic)


def filter_row(data):
    return not (
        (data["temperature"] is None)
        | (data["humidity"] is None)
        | (data["pressure"] is None)
    )


def convert_row(data):
    data["temperature"] = data["temperature"] * 1.8 + 32
    data["pressure"] = data["pressure"] / 6.895
    return data


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,  # Required for Pub/Sub streaming
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic_path)
            | "toDict" >> beam.Map(lambda x: json.loads(x))
            | "Filter" >> beam.Filter(filter_row)
            | "Convert" >> beam.Map(convert_row)
        )
        rows | "to byte" >> beam.Map(
            lambda x: json.dumps(x).encode("utf8")
        ) | "to Pub/sub" >> beam.io.WriteToPubSub(topic=output_topic_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
