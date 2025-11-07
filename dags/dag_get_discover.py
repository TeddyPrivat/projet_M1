from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.script_get_discover import get_with_search
from scripts.script_get_token_airflow import get_token

default_args = {
    "owner": "teddy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def fetch_token(ti, **kwargs):
    """
    Appelle la fonction get_token et stocke le token dans XCom
    """
    token = get_token()  # ta fonction doit retourner le access_jwt
    if not token:
        raise ValueError("Token non récupéré !")
    # Stocke le token dans XCom
    ti.xcom_push(key='access_jwt', value=token)
    return token

def fetch_discover(ti, **kwargs):
    token = ti.xcom_pull(task_ids='get_token_task', key='access_jwt')
    if not token:
        raise ValueError("Token non trouvé dans XCom !")
    queries = ["news", "world", "science", "tech"]
    get_with_search(token=token, queries=queries)

with DAG(
    dag_id="bluesky_discover",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # toutes les heures à la minute 0
    catchup=False,
    tags=["bluesky", "discover"]
) as dag:

    # Tâche 1 : récupération du token
    get_token_task = PythonOperator(
        task_id="get_token_task",
        python_callable=fetch_token
    )

    # Tâche 2 : récupération de la timeline
    fetch_discover_task = PythonOperator(
        task_id="fetch_discover_task",
        python_callable=fetch_discover
    )

    get_token_task >> fetch_discover_task