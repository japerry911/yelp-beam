import argparse
import json
import logging
from time import perf_counter

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .utils import try_key_all


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

    table_spec = bigquery.TableReference(
        projectId=GCP_PROJECT_ID,
        datasetId="yelp",
        tableId="yelp_academic_dataset_business",
    )

    FILENAME = f"gs://{BUCKET_NAME}/{folder_name}/yelp_academic_dataset_business.json"

    SCHEMA_STR = """
        city:STRING, attributes_Music_video:STRING, attributes_Music:STRING, 
        hours_Saturday:STRING, attributes_DriveThru:STRING, 
        attributes_BusinessParking_lot:STRING, attributes_Ambience_trendy:STRING, 
        attributes_BestNights_sunday:STRING, hours_Tuesday:STRING, 
        attributes_BikeParking:STRING, attributes_Ambience_casual:STRING, 
        attributes_HairSpecializesIn:STRING, 
        attributes_DietaryRestrictions_dairy-free:STRING, 
        attributes_Music_no_music:STRING, attributes_HairSpecializesIn_kids:STRING, 
        attributes_BestNights:STRING, attributes_BusinessAcceptsBitcoin:STRING, 
        attributes_Open24Hours:STRING, attributes_BusinessParking_street:STRING, 
        attributes_Ambience_touristy:STRING, attributes_BestNights_wednesday:STRING, 
        attributes_RestaurantsCounterService:STRING, attributes_Smoking:STRING, 
        attributes_BestNights_friday:STRING, attributes_RestaurantsAttire:STRING, 
        attributes_GoodForMeal_latenight:STRING, address:STRING, 
        attributes_HasTV:STRING, attributes_AcceptsInsurance:STRING,
        attributes_BusinessParking:STRING, attributes_Ambience_romantic:STRING, 
        attributes_Caters:STRING, postal_code:STRING, 
        attributes_RestaurantsTakeOut:STRING, name:STRING, attributes:STRING, 
        attributes_DietaryRestrictions:STRING, attributes_WiFi:STRING, 
        attributes_Music_live:STRING, attributes_Ambience_upscale:STRING, 
        hours_Monday:STRING, attributes_BusinessAcceptsCreditCards:STRING, 
        attributes_BestNights_monday:STRING, 
        attributes_HairSpecializesIn_extensions:STRING, hours_Wednesday:STRING, 
        attributes_Ambience_classy:STRING, attributes_BestNights_tuesday:STRING, 
        attributes_Music_jukebox:STRING, attributes_DogsAllowed:STRING, 
        attributes_RestaurantsGoodForGroups:STRING, 
        attributes_HairSpecializesIn_coloring:STRING, attributes_AgesAllowed:STRING, 
        attributes_HappyHour:STRING, attributes_DietaryRestrictions_vegetarian:STRING, 
        attributes_DietaryRestrictions_soy-free:STRING, 
        attributes_HairSpecializesIn_curly:STRING, 
        attributes_GoodForMeal_breakfast:STRING, attributes_Ambience:STRING, 
        attributes_BusinessParking_valet:STRING, attributes_ByAppointmentOnly:STRING, 
        hours_Thursday:STRING, attributes_Music_dj:STRING, 
        attributes_Ambience_intimate:STRING, hours_Sunday:STRING, state:STRING, 
        attributes_GoodForMeal_dessert:STRING, 
        attributes_HairSpecializesIn_perms:STRING, attributes_OutdoorSeating:STRING, 
        attributes_Music_background_music:STRING, hours:STRING, 
        attributes_BusinessParking_garage:STRING, attributes_GoodForKids:STRING, 
        attributes_DietaryRestrictions_gluten-free:STRING, latitude:STRING, 
        attributes_GoodForMeal_dinner:STRING, review_count:STRING, 
        attributes_RestaurantsReservations:STRING, attributes_NoiseLevel:STRING, 
        attributes_GoodForMeal_brunch:STRING, attributes_BYOBCorkage:STRING, 
        attributes_DietaryRestrictions_halal:STRING, longitude:STRING, 
        attributes_GoodForDancing:STRING, attributes_Alcohol:STRING, 
        attributes_GoodForMeal_lunch:STRING, attributes_RestaurantsTableService:STRING, 
        attributes_BestNights_saturday:STRING, 
        attributes_HairSpecializesIn_africanamerican:STRING, business_id:STRING, 
        attributes_BestNights_thursday:STRING, 
        attributes_HairSpecializesIn_straightperms:STRING, is_open:STRING, 
        attributes_Corkage:STRING, stars:STRING, attributes_RestaurantsDelivery:STRING, 
        attributes_HairSpecializesIn_asian:STRING, 
        attributes_BusinessParking_validated:STRING, attributes_CoatCheck:STRING, 
        hours_Friday:STRING, attributes_RestaurantsPriceRange2:STRING, 
        categories:STRING, attributes_DietaryRestrictions_vegan:STRING, 
        attributes_WheelchairAccessible:STRING, attributes_Ambience_divey:STRING, 
        attributes_DietaryRestrictions_kosher:STRING, 
        attributes_Ambience_hipster:STRING, attributes_GoodForMeal:STRING, 
        attributes_Music_karaoke:STRING, attributes_BYOB:STRING
    """

    with beam.Pipeline(options=pipeline_options) as p:
        # noinspection PyTypeChecker
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(FILENAME)
            | "Parse JSON" >> beam.ParDo(JsonLoads())
            | "Flatten Parse" >> beam.ParDo(FlattenParse())
            | "Convert Values to Strings" >> beam.ParDo(ConvertValuesToStrings())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table_spec,
                schema=SCHEMA_STR,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
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
