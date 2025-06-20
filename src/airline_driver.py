import sys
sys.path.insert(0, "/home/hadoop/src")
import os
# print("List /tmp:", os.listdir("tmp/"))
# print("List /tmp/src:", os.listdir("/tmp/src"))
from pyspark.sql import SparkSession
from s3_module import AirlineS3Module
from airline_transform import AirlineTransform
from pathlib import Path
import logging
import configparser
# from warehouse.goodreads_warehouse_driver import GoodReadsWarehouseDriver
import time

config_path = Path("/home/hadoop/src/config.cfg")
config = configparser.ConfigParser()
config.read(config_path)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)

def create_sparkSession():
    return (SparkSession.builder
            .appName("airline")
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")
            .enableHiveSupport()
            .getOrCreate())

def main():
    logger.debug("\n\nSetting up Spark Session...")
    spark = create_sparkSession()
    grt = AirlineTransform(spark)

    modules = {
        "flights.csv": grt.transform_flights_dataset
    }

    logger.debug("\n\nCopying data from s3 landing zone to...")
    airline_s3 = AirlineS3Module()

    airline_s3.s3_move_data()

    files_in_working_zone = airline_s3.get_files(config.get('BUCKET', 'NAME'),config.get('BUCKET', 'WORKING_PREFIX'))

    if len(set(modules.keys()) & set(files_in_working_zone)) > 0:
        logger.info("Cleaning up processed zone.")
        airline_s3.clean_bucket(config.get('BUCKET', 'NAME'),config.get('BUCKET', 'PROCESSED_PREFIX'))

    for file in files_in_working_zone:
        file_name = file.split("/")[-1]
        logger.info(f"Processing file: {file}")
        if file_name in modules:
            logger.debug(f"Calling transformation for: {file_name}")
            modules[file_name]()

    logger.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # airlineWarehouse = AirlineWarehouseDriver()

if __name__ == "__main__":
    main()