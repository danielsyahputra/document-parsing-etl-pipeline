from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_data": datetime(2025, 2, 13),
    "email": ["dnlshp@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "first_dag",
    start_date=datetime(2025, 2, 13),
    default_args=default_args,
    description="First DAG",
    schedule_interval=timedelta(days=1)
)

def hello_world():
    print("Hello, World!")

task = PythonOperator(
    task_id="print_hello_world",
    python_callable=hello_world,
    dag=dag
)