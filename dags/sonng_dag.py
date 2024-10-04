import sys
import os
# Agregar la ruta al paquete src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_grammys import read_db
from extract_spotify import read_csvs
from transform_grammys import transform_db
from transform_spotify import transform_csv
from merge import merge
from load import load
from load_drive import store

# Definir argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,  
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'grammys_spotify_dag',  
    default_args=default_args,
    description='ETL Pipeline for grammys spotify',
    schedule_interval='@daily',
) as dag:

    # Crear las instancias de PythonOperator para cada tarea
    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=read_db,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csvs,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    transform_db_task = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    transform_csv_task = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    merge_task = PythonOperator(
        task_id='merge',
        python_callable=merge,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    store_task = PythonOperator(
        task_id='store',
        python_callable=store,
        dag=dag,
        retries=3,  # Establecer el número de reintentos
        retry_delay=timedelta(minutes=1)
    )

    # Definir la secuencia de las tareas
    read_db_task >> transform_db_task >> merge_task
    read_csv_task >> transform_csv_task >> merge_task >> load_task >> store_task

