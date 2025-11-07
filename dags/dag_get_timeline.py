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


def fetch_timeline(ti,**kwargs):
    """
    Récupère le token depuis XCom et appelle get_timeline
    """
    token = ti.xcom_pull(task_ids='get_token_task', key='access_jwt')
    if not token:
        raise ValueError("Token non trouvé dans XCom !")

    # Appelle ta fonction get_timeline en lui passant le token
    get_timeline(access_jwt=token)

with DAG(
    dag_id="bluesky_timeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bluesky"]
) as dag:

    # Tâche 1 : récupération du token
    get_token_task = PythonOperator(
        task_id="get_token_task",
        python_callable=fetch_token
    )

    # Tâche 2 : récupération de la timeline
    get_timeline_task = PythonOperator(
        task_id="get_timeline_task",
        python_callable=fetch_timeline
    )

    get_token_task >> get_timeline_task