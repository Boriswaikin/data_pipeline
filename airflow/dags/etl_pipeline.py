from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {

}

dag_name = 'airline_pipeline'
dag = DAG(dag_name,
          default_args = default_args,
          description = 'Load and Transform data from landing zone to processed zone',
          schedule_interval = '*/10****',
          max_active_runs =  1
        )

start_operator = DummyOperator(task_id = 'Begin_execution', dag=dag)

jobOperator = SSHOperator(
        task_id = 'AirlinesETLJob',
        command = 
        ssh_hook = 
        dag=dag
)

warehouse_data_checks = DataQualityOperator(
    task_id = 'Warehouse_data_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    tables = []
)

end_operator = DummyOperator(task_id = 'Stop_execution', dag=dag)

start_operator >> jobOperator >> warehouse_data_checks >> end_operators