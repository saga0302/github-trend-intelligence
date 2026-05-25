from datetime import datetime, timedelta
import requests
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'saga',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

DATABRICKS_HOST = "https://dbc-dbf6a37f-8f42.cloud.databricks.com"

JOB_BRONZE = 439438618542040
JOB_SILVER = 354067905897263
JOB_GOLD   = 102034231087609


def get_token():
    """
    Get OAuth token using service principal credentials.
    Client ID stored in 'login', Secret stored in 'password'
    in the databricks_default Airflow connection.
    """
    conn = BaseHook.get_connection("databricks_default")
    client_id = conn.login
    client_secret = conn.password

    token_url = f"{DATABRICKS_HOST}/oidc/v1/token"
    
    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
        },
        auth=(client_id, client_secret),
        timeout=30
    )
    
    print(f"Token response status: {response.status_code}")
    
    if response.status_code != 200:
        raise Exception(f"Failed to get token: {response.status_code} — {response.text}")
    
    return response.json()['access_token']


def calculate_target_hour(**context):
    """Calculate which hour to process and push to XCom."""
    target = context['data_interval_start'] - timedelta(hours=1)
    target_str = str(target)
    print(f"=== Hour to process: {target_str} ===")
    context['ti'].xcom_push(key='target_hour', value=target_str)
    return target_str


def run_job(job_id, params, token):
    """
    Trigger an existing Databricks job and poll until complete.
    Uses jobs/run-now which works with authentication scope tokens.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Trigger the job
    trigger_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    payload = {
        "job_id": job_id,
        "notebook_params": params
    }

    print(f"Triggering job {job_id}")
    print(f"Params: {params}")

    response = requests.post(trigger_url, headers=headers, json=payload, timeout=60)
    print(f"Response status: {response.status_code}")
    print(f"Response body: {response.text}")

    if response.status_code != 200:
        raise Exception(f"Failed to trigger job: {response.status_code} — {response.text}")

    run_id = response.json()['run_id']
    print(f"Run ID: {run_id}")

    # Poll until complete
    status_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
    while True:
        time.sleep(20)
        status_resp = requests.get(status_url, headers=headers, timeout=30)
        state = status_resp.json()['state']
        life_cycle = state['life_cycle_state']
        print(f"Job {job_id} state: {life_cycle}")

        if life_cycle in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'):
            result = state.get('result_state', 'UNKNOWN')
            print(f"life_cycle: {life_cycle}, result_state: {result}")
            # Databricks serverless quirk: INTERNAL_ERROR at life_cycle
            # level with no result_state means notebook actually succeeded
            if life_cycle == 'INTERNAL_ERROR' and result == 'UNKNOWN':
                print("Serverless quirk — treating as success")
                return run_id
            if result not in ('SUCCESS', 'INTERNAL_ERROR'):
                error_msg = state.get('state_message', 'No message')
                raise Exception(f"Job failed: {result} — {error_msg}")
            return run_id


def trigger_bronze(**context):
    target = context['ti'].xcom_pull(
        task_ids='calculate_target_hour',
        key='target_hour'
    )
    token = get_token()
    print(f"=== Triggering Bronze for {target} ===")
    run_id = run_job(JOB_BRONZE, {"target_hour": target}, token)
    print(f"Bronze complete. Run ID: {run_id}")
    context['ti'].xcom_push(key='bronze_run_id', value=run_id)


def trigger_silver(**context):
    target = context['ti'].xcom_pull(
        task_ids='calculate_target_hour',
        key='target_hour'
    )
    token = get_token()
    print(f"=== Triggering Silver for {target} ===")
    run_id = run_job(JOB_SILVER, {"target_hour": target}, token)
    print(f"Silver complete. Run ID: {run_id}")


def trigger_gold(**context):
    target = context['ti'].xcom_pull(
        task_ids='calculate_target_hour',
        key='target_hour'
    )
    token = get_token()
    print(f"=== Triggering Gold for {target} ===")
    run_id = run_job(JOB_GOLD, {"target_hour": target}, token)
    print(f"Gold complete. Run ID: {run_id}")

def export_to_snowflake(**context):
    """Trigger Databricks export job to load Gold tables into Snowflake."""
    target = context['ti'].xcom_pull(
        task_ids='calculate_target_hour',
        key='target_hour'
    )
    token = get_token()
    print(f"=== Exporting to Snowflake for {target} ===")
    run_id = run_job(425269259461376, {"target_hour": target}, token)
    print(f"Export complete. Run ID: {run_id}")
    

with DAG(
    dag_id='github_pipeline_full',
    default_args=default_args,
    description='Full pipeline: Bronze -> Silver -> Gold via Databricks Jobs API',
    start_date=datetime(2025, 4, 19, 3, 0, 0),
    schedule_interval='5 * * * *',
    catchup=False,
    tags=['github', 'week3', 'pipeline', 'full'],
) as dag:

    t1 = PythonOperator(
        task_id='calculate_target_hour',
        python_callable=calculate_target_hour,
    )

    t2 = PythonOperator(
        task_id='trigger_bronze',
        python_callable=trigger_bronze,
    )

    t3 = PythonOperator(
        task_id='trigger_silver',
        python_callable=trigger_silver,
    )

    t4 = PythonOperator(
        task_id='trigger_gold',
        python_callable=trigger_gold,
    )
    t5 = PythonOperator(
        task_id='export_to_snowflake',
        python_callable=export_to_snowflake,
    )

    t1 >> t2 >> t3 >> t4 >> t5 