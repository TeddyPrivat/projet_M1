from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
from datetime import datetime, timedelta

default_args = {
    "owner": "teddy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def get_token(**kwargs):
    res = subprocess.run(
        ["python3", "/opt/airflow/scripts/script_get_token_airflow.py"],
        capture_output=True,
        text=True,
        check=True
    )

    # Recherche de la ligne contenant "Access JWT:"
    for line in res.stdout.splitlines():
        if line.startswith("Access JWT:"):
            token = line.split(":", 1)[1].strip()
            print("Token récupéré :", token)  # Pour debug
            return token  # sera stocké dans XCom
    return None


with DAG(
    dag_id="bluesky_get_token",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bluesky"]
) as dag:

    t1 = PythonOperator(
        task_id="get_token",
        python_callable=get_token
    )