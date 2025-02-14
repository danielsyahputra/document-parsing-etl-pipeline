import os
import glob
import shutil
from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def move_to_processing_folder(**context):
    fs_hook = FSHook(fs_conn_id='test_file')
    base_path = fs_hook.get_path()
    
    filepath_pattern = os.path.join(base_path, "/opt/airflow/docs/new/*.pdf")
    
    files = glob.glob(filepath_pattern)
    print(f"Files found using pattern {filepath_pattern}: {files}")
    
    if not files:
        print("No PDF files found in the directory")
        return
    
    for file_path in files:
        process_single_file(file_path)

def process_single_file(file_path):
    if not os.path.exists(file_path):
        print(f"File does not exist: {file_path}")
        return
        
    print(f"Processing file path: {file_path}")
    filename = os.path.basename(file_path)
    
    destination_dir = '/opt/airflow/docs/cache'
    os.makedirs(destination_dir, exist_ok=True)
    
    destination = os.path.join(destination_dir, filename)
    print(f"Moving file from {file_path} to {destination}")
    
    try:
        shutil.copy2(file_path, destination)
        print(f"Successfully copied file to {destination}")
        os.remove(file_path)
        

    except Exception as e:
        print(f"Error copying file: {str(e)}")
        raise 

def test_connection(**context):
    fs_hook = FSHook(fs_conn_id='test_file')
    base_path = fs_hook.get_path()
    print(f"Base path from connection: {base_path}")
    
    pdf_pattern = os.path.join(base_path, "docs/new/*.pdf")
    files = glob.glob(pdf_pattern)
    print(f"PDF pattern being checked: {pdf_pattern}")
    print(f"Files found: {files}")

with DAG(
    "pdf_processor_dag",
    default_args=default_args,
    description='Monitor for PDFs and send to processor',
    schedule_interval=timedelta(seconds=30),
    catchup=False
    ) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/docs/new/*.pdf", 
        fs_conn_id="test_file",
        poke_interval=10,
        timeout=60*5
    )
    
    process_new_file = PythonOperator(
        task_id='process_new_file',
        python_callable=move_to_processing_folder,
        provide_context=True,
    )

    test_conn = PythonOperator(
        task_id='test_connection',
        python_callable=test_connection,
    )

    wait_for_file >> test_conn >> process_new_file