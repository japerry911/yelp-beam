import argparse
import json
import logging
from time import perf_counter

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .utils import try_key_all
from .schemas.yelp_academic_dataset_business import (
    bq_schema as yelp_academic_dataset_business_bq_schema,
)
from .schemas.yelp_academic_dataset_check_in import (
    bq_schema as yelp_academic_dataset_check_in_bq_schema,
)


# noinspection PyAbstractClass
class JsonLoads(beam.DoFn):
    def process(self, record, **_kwargs):
        yield json.loads(record)


# noinspection PyAbstractClass
class FlattenParse(beam.DoFn):
    def process(self, record, **_kwargs):
        yield try_key_all(record)


# noinspection PyAbstractClass
class ConvertValuesToStrings(beam.DoFn):
    def process(self, record, **_kwargs):
        for key in record:
            record[key] = str(record[key])
        yield record


all_columns = set()


# ToDo: remove after development
# class test(beam.DoFn):
#     def process(self, record, **_kwargs):
#         for key in record:
#             all_columns.add(key)
#
#         yield record


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

    parser.add_argument(
        "--temp_location",
        type=str,
        default="gs://sky-beam-dev/test/",
        help="Folder name for the data to be read from.",
    )

    parser.add_argument(
        "--runner",
        type=str,
        default="DirectRunner",
        help="The Runner being used for Apache Beam data pipeline",
    )

    parser.add_argument(
        "--project",
        type=str,
        default=GCP_PROJECT_ID,
        help="The GCP Project ID",
    )

    parser.add_argument(
        "--region",
        type=str,
        default="us-central1",
        help="The GCP Region",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    yelp_academic_dataset_business_table_spec = bigquery.TableReference(
        projectId=GCP_PROJECT_ID,
        datasetId="yelp",
        tableId="yelp_academic_dataset_business",
    )
    yelp_academic_dataset_check_in_table_spec = bigquery.TableReference(
        projectId=GCP_PROJECT_ID,
        datasetId="yelp",
        tableId="yelp_academic_dataset_check_in",
    )

    FILENAME_BASE = f"gs://{BUCKET_NAME}/{folder_name}/{{}}.json"

    with beam.Pipeline(options=pipeline_options) as p:
        # --- Yelp Academic Check In ---
        # noinspection PyTypeChecker
        (
            p
            | "Read from GCS"
            >> beam.io.ReadFromText(
                FILENAME_BASE.format(
                    "yelp_academic_dataset_checkin",
                )
            )
            | "Parse JSON" >> beam.ParDo(JsonLoads())
            | "Flatten Parse" >> beam.ParDo(FlattenParse())
            | "Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                yelp_academic_dataset_check_in_table_spec,
                schema=", ".join(yelp_academic_dataset_check_in_bq_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                custom_gcs_temp_location=known_args.temp_location,
            )
        )

        # --- Yelp Academic Dataset Business ---
        # noinspection PyTypeChecker
        (
            p
            | "Read from GCS"
            >> beam.io.ReadFromText(
                FILENAME_BASE.format(
                    "yelp_academic_dataset_business",
                )
            )
            | "Parse JSON" >> beam.ParDo(JsonLoads())
            | "Flatten Parse" >> beam.ParDo(FlattenParse())
            | "Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                yelp_academic_dataset_business_table_spec,
                schema=", ".join(yelp_academic_dataset_business_bq_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                custom_gcs_temp_location=known_args.temp_location,
            )
        )


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
