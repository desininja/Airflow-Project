from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
import logging

S3_bucket_name = 'food-delivery-project'
folder_path = 'landing-zone/food_delivery.csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 13),
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'S3_sensor_and_spark_transform_dag',
    default_args=default_args,
    description='For Airflow Project: This DAG detects new file and transforms it based on a PySpark script',
    schedule_interval='@daily',
    catchup=False
)

# S3 Task
S3_sensor_task_1 = S3KeySensor(
    task_id='New_S3_object_detection_dag',
    bucket_name=S3_bucket_name,
    bucket_key=folder_path,
    poke_interval=300,
    timeout=6000,
    mode='poke',
    dag=dag
)

# Logging the value from XCom
logging.info("XCom value for New_S3_object_detection_dag: %s", S3_sensor_task_1.xcom_push)

# EmrAddStepsOperator Task
def get_s3_file_path(ti):
    # Retrieve file path from XCom
    xcom_value = ti.xcom_pull(task_ids='New_S3_object_detection_dag', key='xcom_push')
    
    # Check if xcom_value is valid
    if not xcom_value:
        logging.warning("XCom value is None or empty, skipping task.")
        return None

    return xcom_value[0]

emr_spark_job_task_2 = EmrAddStepsOperator(
    task_id='run_spark_job_on_emr',
    job_flow_id='j-HBDIR87CSSQC',  # Replace with your EMR job flow ID
    aws_conn_id='aws_default',
    steps=[
        {
            'Name': 'Run PySpark Script',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode',
                    'cluster',
                    's3://pyspark-scripts-for-projects/food-delivery/pyspark_job.py',
                    '{{ task_instance.xcom_pull(task_ids="New_S3_object_detection_dag", key="return_value")[0] }}',
                ]
            }
        }
    ],
    dag=dag
)

# EmrStepSensor Task
step_checker_task3 = EmrStepSensor(
    task_id='check_step',
    job_flow_id='j-HBDIR87CSSQC',  # Replace with your EMR job flow ID
    step_id="{{ ti.xcom_pull(task_ids='run_spark_job_on_emr', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,
    timeout=86400,
    mode='poke',
    dag=dag
)

# Define task dependencies
S3_sensor_task_1 >> emr_spark_job_task_2 >> step_checker_task3
