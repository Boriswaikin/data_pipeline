from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

config_path = Path("/home/hadoop/src/config.cfg")
config = configparser.ConfigParser()
config.read(config_path)

JOB_ROLE_ARN = config.get('AWS', 'JOB_ROLE_ARN')
S3_BUCKET = config.get('AWS', 'S3_BUCKET')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="airline_pipeline", 
    default_args=default_args,
    description="Run airline_etl_pipeline using EMR Serverless from zip package",
    schedule="*/10 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["airline", "emr-serverless"],
)

with dag:
    create_app = EmrServerlessCreateApplicationOperator(
        release_label="emr-6.15.0",
        task_id="create_emr_serverless_app",
        job_type="SPARK",
        config={"name": "airline-etl-serverless"},
        region_name="us-west-2"
    )
    application_id = create_app.output

    start_etl_job = EmrServerlessStartJobOperator(
        task_id="start_airline_etl_job",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{S3_BUCKET}/airline_etl_pipeline/airline_driver.py",
                "sparkSubmitParameters": (
                    f"--archives s3://{S3_BUCKET}/airline_etl_pipeline/src.zip#src "
                )
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{S3_BUCKET}/emr-serverless-logs/"
                }
            }
        },
        region_name="us-west-2"
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_emr_serverless_app",
        application_id=application_id,
        # trigger_rule="all_done",
        # region_name="us-west-2"
    )

    create_app>> start_etl_job >> delete_app