import sys
import os
# Agregar la ruta al paquete src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/extract')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/load')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/transform')))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Definir argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_data_task():
    from extract_grammys import extract_data
    return extract_data()

def load_spotify_data_task():
    from extract_spotify import load_spotify_data
    return load_spotify_data()

def get_artist_from_spotify_task():
    from transform_grammys import get_artist_from_spotify
    return get_artist_from_spotify()

def get_spotify_id_task():
    from transform_grammys import get_spotify_id
    return get_spotify_id()

def add_spotify_ids_task():
    from transform_grammys import add_spotify_ids
    return add_spotify_ids()

def transform_spotify_data_task():
    from transform_spotify import transform_spotify_data
    return transform_spotify_data()

def get_new_access_token_task():
    from merge import get_new_access_token
    return get_new_access_token()

def normalize_text_task():
    from merge import normalize_text
    return normalize_text()

def merge_datasets_task():
    from merge import merge_datasets
    return merge_datasets()

def get_spotify_info_task():
    from merge import get_spotify_info
    return get_spotify_info()

def get_audio_features_task():
    from merge import get_audio_features
    return get_audio_features()

def fill_missing_data_task():
    from merge import fill_missing_data
    return fill_missing_data()

def main_task():
    from merge import main
    return main()

def load_data_to_db_task():
    from load import load_data_to_db
    return load_data_to_db()

def authenticate_gdrive_task():
    from load_drive import authenticate_gdrive
    return authenticate_gdrive()

def upload_to_drive_task():
    from load_drive import upload_to_gdrive
    return upload_to_gdrive()

with DAG(
    'grammys_spotify_dag',  
    default_args=default_args,
    description='ETL Pipeline for grammys spotify',
    schedule_interval='@daily',
) as dag:

    # Crear las instancias de PythonOperator para cada tarea
    extract_data_operator = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_task,
    )

    load_spotify_data_operator = PythonOperator(
        task_id='load_spotify_data',
        python_callable=load_spotify_data_task,
    )

    get_artist_from_spotify_operator = PythonOperator(
        task_id='get_artist_from_spotify',
        python_callable=get_artist_from_spotify_task,
    )

    get_spotify_id_operator = PythonOperator(
        task_id='get_spotify_id',
        python_callable=get_spotify_id_task,
    )

    add_spotify_ids_operator = PythonOperator(
        task_id='add_spotify_ids',
        python_callable=add_spotify_ids_task,
    )

    transform_spotify_data_operator = PythonOperator(
        task_id='transform_spotify_data',
        python_callable=transform_spotify_data_task,
    )

    get_new_access_token_operator = PythonOperator(
        task_id='get_new_access_token',
        python_callable=get_new_access_token_task,
    )

    normalize_text_operator = PythonOperator(
        task_id='normalize_text',
        python_callable=normalize_text_task,
    )

    merge_datasets_operator = PythonOperator(
        task_id='merge_datasets',
        python_callable=merge_datasets_task,
    )

    get_spotify_info_operator = PythonOperator(
        task_id='get_spotify_info',
        python_callable=get_spotify_info_task,
    )

    get_audio_features_operator = PythonOperator(
        task_id='get_audio_features',
        python_callable=get_audio_features_task,
    )

    fill_missing_data_operator = PythonOperator(
        task_id='fill_missing_data',
        python_callable=fill_missing_data_task,
    )

    main_operator = PythonOperator(
        task_id='main',
        python_callable=main_task,
    )

    load_data_to_db_operator = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db_task,
    )

    authenticate_gdrive_operator = PythonOperator(
        task_id='authenticate_gdrive',
        python_callable=authenticate_gdrive_task,
    )

    upload_to_drive_operator = PythonOperator(
        task_id='upload_to_drive',
        python_callable=upload_to_drive_task,
    )

    # Definir las dependencias entre las tareas
    extract_data_operator >> get_artist_from_spotify_operator >>  get_spotify_id_operator >> add_spotify_ids_operator >> get_new_access_token_operator
    load_spotify_data_operator >> transform_spotify_data_operator >> get_new_access_token_operator
    
    get_new_access_token_operator >> normalize_text_operator >> merge_datasets_operator >> get_spotify_info_operator >> get_audio_features_operator
    get_audio_features_operator >> fill_missing_data_operator >> main_operator >> load_data_to_db_operator >> authenticate_gdrive_operator

    authenticate_gdrive_operator >> upload_to_drive_operator





