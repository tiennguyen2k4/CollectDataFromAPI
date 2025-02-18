from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import LoadDataToDB

default_args = {
    'owner': 'tiendinh',
    'start_date': datetime(2025,2,13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG (
    dag_id = 'my_dag',
    default_args = default_args,
    description = 'DAG transform data user',
    schedule_interval = '@daily',
    catchup = False
) as dag:
    load_data = PythonOperator(
        task_id='load_data_to_database',
        python_callable=LoadDataToDB.main,
        env={'NEWS_API_KEY': '38093f5839224d5ebea505260400ee22'}
    )