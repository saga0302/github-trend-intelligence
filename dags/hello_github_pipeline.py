from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'saga',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def task_one():
    print("=== TASK 1: Pipeline is alive ===")
    print(f"This is the GitHub Trend Intelligence Pipeline")
    print(f"Running at: {datetime.utcnow()}")

def task_two(**context):
    print("=== TASK 2: I know about the execution date ===")
    execution_date = context['execution_date']
    print(f"Airflow execution_date: {execution_date}")
    print(f"This means I would process GitHub data for hour: {execution_date.hour}")

def task_three(**context):
    msg = context['ti'].xcom_pull(task_ids='understand_execution_date')
    print("=== TASK 3: XCom demo ===")
    print("XComs let tasks pass data to each other")
    print(f"If task 2 pushed a value, I would see it here: {msg}")

with DAG(
    dag_id='hello_github_pipeline',
    default_args=default_args,
    description='My first DAG - understanding Airflow basics',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['learning', 'week1'],
) as dag:

    t1 = PythonOperator(
        task_id='pipeline_is_alive',
        python_callable=task_one,
    )

    t2 = PythonOperator(
        task_id='understand_execution_date',
        python_callable=task_two,
    )

    t3 = PythonOperator(
        task_id='xcom_demo',
        python_callable=task_three,
    )

    t1 >> t2 >> t3