import argparse
import ast
import json
import logging
from time import perf_counter

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


# --- Utilities START ---
def try_key_all(
    d: dict,
    main_key: str = "",
) -> dict:
    """Try to get all keys from a main_key dictionary, returning an empty dictionary if
    it doesn't exist.
    """
    if d is None:
        return {}

    return _extract_keys(d, main_key)


def _extract_keys(
    d: dict,
    prefix_alias: str,
) -> dict:
    """Extract keys from a dictionary."""
    return_dict = {}

    for key in d.keys():
        if prefix_alias == "":
            alias = key
        else:
            alias = f"{prefix_alias}_{key}"

        if isinstance(d[key], dict) or (isinstance(d[key], str) and _is_json(d[key])):
            if isinstance(d[key], str):
                use_key = ast.literal_eval(d[key])
            else:
                use_key = d[key]

            return_dict = {
                **return_dict,
                **_extract_keys(use_key, alias),
            }
        else:
            return_dict[alias] = d[key]

    return return_dict


def _is_json(x: str) -> bool:
    """Check if a string is a valid JSON with ast.literal_eval."""
    try:
        if x[0] != "{":
            return False

        return isinstance(ast.literal_eval(x), dict)
    except (ValueError, SyntaxError, IndexError):
        return False


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


# --- Utilities END ---


# --- BigQuery Schemas START ---
yelp_academic_dataset_review_bq_schema = [
    "business_id:STRING",
    "funny:STRING",
    "cool:STRING",
    "user_id:STRING",
    "text:STRING",
    "date:STRING",
    "review_id:STRING",
    "useful:STRING",
    "stars:STRING",
]


yelp_academic_dataset_check_in_bq_schema = [
    "business_id:STRING",
    "date:STRING",
]


yelp_academic_dataset_business_bq_schema = [
    "city:STRING",
    "attributes_Music_video:STRING",
    "attributes_Music:STRING",
    "hours_Saturday:STRING",
    "attributes_DriveThru:STRING",
    "attributes_BusinessParking_lot:STRING",
    "attributes_Ambience_trendy:STRING",
    "attributes_BestNights_sunday:STRING",
    "hours_Tuesday:STRING",
    "attributes_BikeParking:STRING",
    "attributes_Ambience_casual:STRING",
    "attributes_HairSpecializesIn:STRING",
    "attributes_DietaryRestrictions_dairy-free:STRING",
    "attributes_Music_no_music:STRING",
    "attributes_HairSpecializesIn_kids:STRING",
    "attributes_BestNights:STRING",
    "attributes_BusinessAcceptsBitcoin:STRING",
    "attributes_Open24Hours:STRING",
    "attributes_BusinessParking_street:STRING",
    "attributes_Ambience_touristy:STRING",
    "attributes_BestNights_wednesday:STRING",
    "attributes_RestaurantsCounterService:STRING",
    "attributes_Smoking:STRING",
    "attributes_BestNights_friday:STRING",
    "attributes_RestaurantsAttire:STRING",
    "attributes_GoodForMeal_latenight:STRING",
    "address:STRING",
    "attributes_HasTV:STRING",
    "attributes_AcceptsInsurance:STRING," "attributes_BusinessParking:STRING",
    "attributes_Ambience_romantic:STRING",
    "attributes_Caters:STRING",
    "postal_code:STRING",
    "attributes_RestaurantsTakeOut:STRING",
    "name:STRING",
    "attributes:STRING",
    "attributes_DietaryRestrictions:STRING",
    "attributes_WiFi:STRING",
    "attributes_Music_live:STRING",
    "attributes_Ambience_upscale:STRING",
    "hours_Monday:STRING",
    "attributes_BusinessAcceptsCreditCards:STRING",
    "attributes_BestNights_monday:STRING",
    "attributes_HairSpecializesIn_extensions:STRING",
    "hours_Wednesday:STRING",
    "attributes_Ambience_classy:STRING",
    "attributes_BestNights_tuesday:STRING",
    "attributes_Music_jukebox:STRING",
    "attributes_DogsAllowed:STRING",
    "attributes_RestaurantsGoodForGroups:STRING",
    "attributes_HairSpecializesIn_coloring:STRING",
    "attributes_AgesAllowed:STRING",
    "attributes_HappyHour:STRING",
    "attributes_DietaryRestrictions_vegetarian:STRING",
    "attributes_DietaryRestrictions_soy-free:STRING",
    "attributes_HairSpecializesIn_curly:STRING",
    "attributes_GoodForMeal_breakfast:STRING",
    "attributes_Ambience:STRING",
    "attributes_BusinessParking_valet:STRING",
    "attributes_ByAppointmentOnly:STRING",
    "hours_Thursday:STRING",
    "attributes_Music_dj:STRING",
    "attributes_Ambience_intimate:STRING",
    "hours_Sunday:STRING",
    "state:STRING",
    "attributes_GoodForMeal_dessert:STRING",
    "attributes_HairSpecializesIn_perms:STRING",
    "attributes_OutdoorSeating:STRING",
    "attributes_Music_background_music:STRING",
    "hours:STRING",
    "attributes_BusinessParking_garage:STRING",
    "attributes_GoodForKids:STRING",
    "attributes_DietaryRestrictions_gluten-free:STRING",
    "latitude:STRING",
    "attributes_GoodForMeal_dinner:STRING",
    "review_count:STRING",
    "attributes_RestaurantsReservations:STRING",
    "attributes_NoiseLevel:STRING",
    "attributes_GoodForMeal_brunch:STRING",
    "attributes_BYOBCorkage:STRING",
    "attributes_DietaryRestrictions_halal:STRING",
    "longitude:STRING",
    "attributes_GoodForDancing:STRING",
    "attributes_Alcohol:STRING",
    "attributes_GoodForMeal_lunch:STRING",
    "attributes_RestaurantsTableService:STRING",
    "attributes_BestNights_saturday:STRING",
    "attributes_HairSpecializesIn_africanamerican:STRING",
    "business_id:STRING",
    "attributes_BestNights_thursday:STRING",
    "attributes_HairSpecializesIn_straightperms:STRING",
    "is_open:STRING",
    "attributes_Corkage:STRING",
    "stars:STRING",
    "attributes_RestaurantsDelivery:STRING",
    "attributes_HairSpecializesIn_asian:STRING",
    "attributes_BusinessParking_validated:STRING",
    "attributes_CoatCheck:STRING",
    "hours_Friday:STRING",
    "attributes_RestaurantsPriceRange2:STRING",
    "categories:STRING",
    "attributes_DietaryRestrictions_vegan:STRING",
    "attributes_WheelchairAccessible:STRING",
    "attributes_Ambience_divey:STRING",
    "attributes_DietaryRestrictions_kosher:STRING",
    "attributes_Ambience_hipster:STRING",
    "attributes_GoodForMeal:STRING",
    "attributes_Music_karaoke:STRING",
    "attributes_BYOB:STRING",
]
# --- BigQuery Schemas END ---


# --- Pipelines START ---
def run(argv=None):
    """INSERT DOCSTRING HERE"""
    BUCKET_NAME = "sky-beam-raw-data"
    GCP_PROJECT_ID = "sky-beam"

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--folder_name",
        dest="folder_name",
        type=str,
        required=True,
        help="Folder name for the data to be read from.",
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
    yelp_academic_review_table_spec = bigquery.TableReference(
        projectId=GCP_PROJECT_ID,
        datasetId="yelp",
        tableId="yelp_academic_dataset_review",
    )

    FILENAME_BASE = f"gs://{BUCKET_NAME}/{known_args.folder_name}/{{}}.json"

    with beam.Pipeline(options=pipeline_options) as p:
        # --- Yelp Academic Dataset Review (YADR) ---
        # noinspection PyTypeChecker
        (
            p
            | "YADR - Read from GCS"
            >> beam.io.ReadFromText(
                FILENAME_BASE.format(
                    "yelp_academic_dataset_review",
                )
            )
            | "YADR - Parse JSON" >> beam.ParDo(JsonLoads())
            | "YADR - Flatten Parse" >> beam.ParDo(FlattenParse())
            | "YADR - Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "YADR - Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                yelp_academic_review_table_spec,
                schema=", ".join(yelp_academic_dataset_review_bq_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        # --- Yelp Academic Check In (YACI) ---
        # noinspection PyTypeChecker
        (
            p
            | "YACI - Read from GCS"
            >> beam.io.ReadFromText(
                FILENAME_BASE.format(
                    "yelp_academic_dataset_checkin",
                )
            )
            | "YACI - Parse JSON" >> beam.ParDo(JsonLoads())
            | "YACI - Flatten Parse" >> beam.ParDo(FlattenParse())
            | "YACI - Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "YACI - Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                yelp_academic_dataset_check_in_table_spec,
                schema=", ".join(yelp_academic_dataset_check_in_bq_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        # --- Yelp Academic Dataset Business (YADB) ---
        # noinspection PyTypeChecker
        (
            p
            | "YADB - Read from GCS"
            >> beam.io.ReadFromText(
                FILENAME_BASE.format(
                    "yelp_academic_dataset_business",
                )
            )
            | "YADB - Parse JSON" >> beam.ParDo(JsonLoads())
            | "YADB - Flatten Parse" >> beam.ParDo(FlattenParse())
            | "YADB - Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "YADB - Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                yelp_academic_dataset_business_table_spec,
                schema=", ".join(yelp_academic_dataset_business_bq_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )


# --- Pipelines END ---


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s :: %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("---STARTING APACHE-BEAM---")
    start_time = perf_counter()

    logger.info("Turning on Pipeline")
    run()
    logger.info("Pipeline completed")

    end_time = perf_counter()
    total_time = end_time - start_time
    logger.info("---STOPPING APACHE-BEAM---")
    logger.info(f"---TOTAL TIME: {total_time:.2f} seconds---")
