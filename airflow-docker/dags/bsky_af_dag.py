from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Assure que Airflow peut importer les modules personnalisés
sys.path.append("/opt/airflow/src")

# Import des scripts personnalisés
from fetch_posts import fetch_and_import_to_mongo
from login_bsky import login
from mongo_connection import connect_to_mongo


# Arguments par défaut des tâches
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Définition du DAG
with DAG(
    dag_id="bluesky_to_mongodb",
    description="Récupère les posts publics de Bluesky et les enregistre dans MongoDB.",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule="@hourly",  # Exécution toutes les heures
    catchup=False,
    tags=["bluesky", "mongodb", "etl"],
) as dag:

    # 1️⃣ Connexion à Bluesky (test de login et récupération du token)
    login_bsky_task = PythonOperator(
        task_id="login_bluesky",
        python_callable=login,
    )

    # 2️⃣ Connexion à MongoDB (vérifie la connexion à la base)
    login_mongodb_task = PythonOperator(
        task_id="login_mongodb",
        python_callable=connect_to_mongo,
    )

    # 3️⃣ Récupération et sauvegarde directe des posts dans MongoDB
    fetch_and_save_task = PythonOperator(
        task_id="fetch_and_save_posts",
        python_callable=fetch_and_import_to_mongo,
        op_kwargs={"db_name": "projet_bluesky","collection_name": "news_posts", "max_posts": 500},
    )

    # Définition des dépendances
    login_bsky_task >> login_mongodb_task >> fetch_and_save_task
