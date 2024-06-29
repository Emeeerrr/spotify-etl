from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime

from spotify_etl_auto import run_spotify_etl, get_access_token

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 29), #Acá puedes poner una fecha de ayer hacia atras.
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'new_spotify_dag',
    default_args=default_args,
    description='DAG para ETL de Spotify con token refresh!',
    schedule_interval='30 * * * *',  # Ejecutar cada hora en el minuto 30
)

client_id = 'EL CLIENT ID DE TU APLICACION DE SPOTIFY'
client_secret = 'EL CLIENT SECRET DE TU APLICACION DE SPOTIFY'
refresh_token = 'El refresh token que obtuviste al correr el curl de que te access token (Leer documentación de Spotify)'

def update_and_run_etl():
    access_token = get_access_token(client_id, client_secret, refresh_token)
    run_spotify_etl(access_token)

run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=update_and_run_etl,
    dag=dag,
)

run_etl
