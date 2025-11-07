from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.script_get_timeline import get_timeline
from scripts.script_get_token_airflow import get_token

default_args = {
    "owner": "teddy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def task_get_token(ti, **kwargs):
    token = get_token()
    # Stocke le token dans XCom pour la tâche suivante
    ti.xcom_push(key="access_token", value=token)

def task_get_timeline(ti, **kwargs):
    # Récupère le token depuis XCom
    token = ti.xcom_pull(key="access_token", task_ids="get_token_task")
    get_timeline(token)

with DAG(
    dag_id="bluesky_timeline_full",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    get_token_task = PythonOperator(
        task_id="get_token_task",
        python_callable=task_get_token
    )

    get_timeline_task = PythonOperator(
        task_id="get_timeline_task",
        python_callable=task_get_timeline
    )

    get_token_task >> get_timeline_task