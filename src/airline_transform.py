import os
from pyspark.sql.functions import when, col, unix_timestamp,to_timestamp,floor,format_string
# import goodreads_udf
import logging
import configparser
from pathlib import Path

config_path = Path("/home/hadoop/src/config.cfg")
config = configparser.ConfigParser()
config.read(config_path)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)

class AirlineTransform:
    
    def __init__(self, spark):
        self._spark = spark
        bucket_name = config.get('BUCKET', 'NAME')
        working_prefix = config.get('BUCKET', 'WORKING_PREFIX')
        processed_prefix = config.get('BUCKET', 'PROCESSED_PREFIX')
        self._load_path = f"s3://{bucket_name}/{working_prefix}"
        self._save_path = f"s3://{bucket_name}/{processed_prefix}"

    def transform_flights_dataset(self):

        flights_df = \
            self._spark.read.csv(self._load_path + '/flights.csv', header=True, mode='PERMISSIVE',inferSchema=True)

        flights_df = flights_df.withColumn(
            "flight_time_scheduled",
            when(
                col("dep_scheduled").isNotNull() & col("arr_scheduled").isNotNull(),
                format_string(
                    "%02d:%02d",
                    floor((unix_timestamp(to_timestamp("arr_scheduled")) - unix_timestamp(to_timestamp("dep_scheduled"))) / 3600),
                    ((unix_timestamp(to_timestamp("arr_scheduled")) - unix_timestamp(to_timestamp("dep_scheduled"))) % 3600 / 60).cast("int")
                )
            ).otherwise(None)
        )
        

        flights_df = flights_df.withColumn(
            "flight_time_actual",
            when(
                col("dep_actual").isNotNull() & col("arr_actual").isNotNull(),
                format_string(
                    "%02d:%02d",
                    floor((unix_timestamp(to_timestamp("arr_actual")) - unix_timestamp(to_timestamp("dep_actual"))) / 3600),
                    ((unix_timestamp(to_timestamp("arr_actual")) - unix_timestamp(to_timestamp("dep_actual"))) % 3600 / 60).cast("int")
                )
            ).otherwise(None)
        )

        flights_df = flights_df.withColumn(
            "dep_delay",
            when(
                col("dep_actual").isNotNull() & col("dep_scheduled").isNotNull(),
                (
                    (unix_timestamp(col("dep_actual")) - unix_timestamp(col("dep_scheduled"))) / 60
                ).cast("int")  # You can cast to Integer if you want clean minute values
            ).otherwise(None)
        )

        flights_df = flights_df.withColumn(
            "arr_delay",
            when(
                col("arr_actual").isNotNull() & col("arr_scheduled").isNotNull(),
                (
                    (unix_timestamp(col("arr_actual")) - unix_timestamp(col("arr_scheduled"))) / 60
                ).cast("int")  # You can cast to Integer if you want clean minute values
            ).otherwise(None)
        )

        logging.debug(f"Attempting to write data to {self._save_path + '/flights/'}")
        flights_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/flights/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')
