from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_data(**kwargs):

    pass

def transform_data(**kwargs):
    pass

def load_data(**kwargs):
    pass

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
}

with DAG('etl_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        dag=dag,
    )

    extract_task >> transform_task >> load_task
