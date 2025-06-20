import os
import boto3
import configparser
import logging
from pathlib import Path

config_path = Path("/home/hadoop/src/config.cfg")
config = configparser.ConfigParser()
config.read(config_path)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)

class AirlineS3Module():
    def __init__(self):
        self._s3 = boto3.resource(service_name='s3', region_name='us-west-2')
        self._files = []
        self._bucket_name = config.get('BUCKET', 'NAME')
        self._landing_prefix = config.get('BUCKET', 'LANDING_PREFIX')
        self._working_prefix = config.get('BUCKET', 'WORKING_PREFIX')
        self._processed_prefix = config.get('BUCKET', 'PROCESSED_PREFIX')

    def s3_move_data(self):
        bucket_name = self._bucket_name
        source_prefix = self._landing_prefix
        target_prefix = self._working_prefix

        logging.debug(f"Inside s3_move_data : Source bucket: {bucket_name}, prefix: {source_prefix}")
        logging.debug(f"Inside s3_move_data : Target bucket: {bucket_name}, prefix: {target_prefix}")

        self.clean_bucket(bucket_name, target_prefix)

        for key in self.get_files(bucket_name, source_prefix):
            # key 是完整 key, 應包含 prefix
            file_name = key.split("/")[-1]
            if file_name in config.get('FILES', 'NAME').split(","):
                logging.debug(f"Copying file {key} from {bucket_name}/{source_prefix} to {bucket_name}/{target_prefix}")
                copy_source = {'Bucket': bucket_name, 'Key': key}
                self._s3.meta.client.copy(copy_source, bucket_name, f"{target_prefix}/{file_name}")

    def get_files(self, bucket_name, prefix):
        logging.debug(f"Inspecting bucket: {bucket_name} with prefix: {prefix}")
        bucket = self._s3.Bucket(bucket_name)
        return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]

    def clean_bucket(self, bucket_name, prefix):
        logging.debug(f"Cleaning bucket: {bucket_name} with prefix: {prefix}")
        bucket = self._s3.Bucket(bucket_name)
        objs_to_delete = bucket.objects.filter(Prefix=prefix)
        objs_to_delete.delete()
    