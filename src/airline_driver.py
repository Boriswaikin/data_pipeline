from pyspark.sql import SparkSession
from s3_module import AirlineS3Module

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parent[0]}/config.cfg"))

logging.config.fileConfig(f"{Path(__file__).parent[0]}/logging.ini")
logger = logging.getLogger(__name__)

def create_sparkSession():
    
    return SparkSession.builder.master('yarn').appName("airline")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")\
        .enableHiveSupport().getOrCreate()

def main():
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparkSession()
    
    logging.debug("\n\nCopying data from s3 landing zone to...")
    airline_s3 = AirlineS3Module()
    
    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    airlineWarehouse = AirlineWarehouseDriver()

    if __name__ = "__main__":
        main()
