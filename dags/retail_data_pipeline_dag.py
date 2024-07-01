from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.etl_process import etl_task
from scripts.data_analysis import main as analyze_data
from scripts.create_snowflake_resources import create_snowflake_resources
from log_config import setup_logger

logger = setup_logger(__name__, 'logs/airflow_dag.log')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'retail_data_pipeline',
    default_args=default_args,
    description='A comprehensive retail data pipeline DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the tasks
start = DummyOperator(task_id='start', dag=dag)

create_resources = PythonOperator(
    task_id='create_snowflake_resources',
    python_callable=create_snowflake_resources,
    dag=dag,
)

etl = PythonOperator(
    task_id='etl_task',
    python_callable=etl_task,
    dag=dag,
)

analyze = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Define the task dependencies
start >> create_resources >> etl >> analyze >> end

# Add some logging
def on_success_callback(context):
    task_instance = context['task_instance']
    logger.info(f"Task {task_instance.task_id} completed successfully")

def on_failure_callback(context):
    task_instance = context['task_instance']
    logger.error(f"Task {task_instance.task_id} failed")

for task in dag.tasks:
    task.on_success_callback = on_success_callback
    task.on_failure_callback = on_failure_callback

logger.info("DAG 'retail_data_pipeline' has been defined")