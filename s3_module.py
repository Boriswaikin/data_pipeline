import boto3

class AirlineS3Module():
    def __init__(self):
        self._s3 = boto3\
                    .resource(service_name = 's3', region_name = 'us-east-1', , aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))
        self._files = []
        self._landing_zone = config.get('BUCKET','LANDING_ZONE')
        self._working_zone = config.get('BUCKET', 'WORKING_ZONE')
        self._processed_zone = config.get('BUCKET', 'PROCESSED_ZONE')
    
    def s3_move_data(self, source_bucket = None, target_bucket = None):
        if source_bucket is None:
            source_bucket = self._landing_zone
        if target_bucket is None:
            target_bucket = self._processed_zone

        logging.debug(f"Inside s3_move_data : Source bucket set is : {source_bucket}\n Target bucket set is : {target_bucket}")

        self.clean_bucket(target_bucket)

        for key in self.get_files(source_bucket):
            if key in config.get('FILES', 'NAME').split(","):
                logging.debug(f"Copying file {key} from {source_bucket} to {target_bucket}")
                self._s3.meta.client.copy({'Bucket': source_bucket, 'Key':key}, target_bucket, key)
        
        # cleanup source bucket,
        # slef.clean_bucket(source_bucket)

    def get_files(self,bucket_name):
        logging.debug(f"Inspecting bucket: {bucket_name} for file present")
        return [bucket_object.key for bucket_object in self._s3.Bucket(bucket_name).object.all()]
    
    def clean_bucket(self,bucke_name):
        logging.debug(f"Cleaning bucket: {bucket_name}")
        self._s3.Bucket(bucket_name).object.all().delete()
    