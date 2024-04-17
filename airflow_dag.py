from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
import logging

S3_BUCKET_NAME = 'food-delivery-project'
FOLDER_PATH = 'landing-zone/food_delivery.csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 13),
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'S3_sensor_and_spark_transform_dag',
    default_args=default_args,
    description='For Airflow Project: Detects new file and transforms it based on a PySpark script',
    schedule_interval='@daily',
    catchup=False,
)

# S3KeySensor task
s3_sensor_task_1 = S3KeySensor(
    task_id='New_S3_object_detection_dag',
    bucket_name=S3_BUCKET_NAME,
    bucket_key=FOLDER_PATH,
    poke_interval=300,
    timeout=6000,
    mode='poke',
    dag=dag,
)

# PythonOperator task to log the XCom value from s3_sensor_task_1
def log_xcom_value(ti):
    xcom_value = ti.xcom_pull(task_ids='New_S3_object_detection_dag', key='return_value')
    if xcom_value:
        logging.info("XCom value for New_S3_object_detection_dag: %s", xcom_value)
    else:
        logging.info("Xcom value is Null!!!!!!!!!!!!!!!!!!!!!!!!")

log_xcom_value_task = PythonOperator(
    task_id='log_xcom_value_task',
    python_callable=log_xcom_value,
    dag=dag,
)

# EmrAddStepsOperator task
emr_spark_job_task_2 = EmrAddStepsOperator(
    task_id='run_spark_job_on_emr',
    job_flow_id='j-2HJM11AYZLN88',  # Replace with your EMR job flow ID
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
                    's3://pyspark-scripts-for-projects/food-delivery/pyspark_job.py'
                ]
            }
        }
    ],
    dag=dag,
)

# EmrStepSensor task
step_checker_task_3 = EmrStepSensor(
    task_id='check_step',
    job_flow_id='j-2HJM11AYZLN88',  # Replace with your EMR job flow ID
    step_id="{{ ti.xcom_pull(task_ids='run_spark_job_on_emr', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,
    timeout=86400,
    mode='poke',
    dag=dag,
)

# Define task dependencies
s3_sensor_task_1 >> log_xcom_value_task >> emr_spark_job_task_2 >> step_checker_task_3
