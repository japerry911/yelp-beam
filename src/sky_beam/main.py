import argparse
import json
import logging
from time import perf_counter

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .utils import try_key_all


column_names = set()
column_names.add("business_id:STRING")


def flatten_parse(row):
    return try_key_all(row)


def record_all_columns(row):
    for key in row.keys():
        column_names.add(f"{key}:STRING")

    return row


def run(
    folder_name: str = "yelp-data/2023-04-30",
    argv=None,
):
    """INSERT DOCSTRING HERE"""

    BUCKET_NAME = "sky-beam-raw-data"
    GCP_PROJECT_ID = "sky-beam"

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--folder_name",
        type=str,
        default=folder_name,
        help="Folder name for the data to be read from.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    print("@")
    print(column_names)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from GCS"
            >> beam.io.ReadFromText(
                f"gs://{BUCKET_NAME}/{folder_name}/yelp_academic_dataset_business.json",
            )
            | "Parse JSON" >> beam.Map(json.loads)
            | "Flatten Parse" >> beam.Map(flatten_parse)
            | "Record all Columns" >> beam.Map(record_all_columns)
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="yelp_academic_dataset_business",
                dataset="yelp",
                project=GCP_PROJECT_ID,
                schema=",".join(column_names),
            )
        )


# 150346


def main(*args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s :: %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("---STARTING APACHE-BEAM---")
    start_time = perf_counter()

    logger.info("Turning on Pipeline")
    run(argv=args)
    logger.info("Pipeline completed")

    end_time = perf_counter()
    total_time = end_time - start_time
    logger.info("---STOPPING APACHE-BEAM---")
    logger.info(f"---TOTAL TIME: {total_time:.2f} seconds---")


if __name__ == "__main__":
    main()
