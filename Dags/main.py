import airflow
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task

from src.validation import validation
from src.etl import etl
from src.ml_model import ml_model


args = {'start_date': days_ago(1)}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag_countries = DAG(
    dag_id='dag_countries',
    default_args=args, 
    schedule_interval=None
    )

with dag_countries:

    op1= PythonOperator(
        task_id='validation',
        python_callable=validation,
        do_xcom_push=True
    )

    op2= PythonOperator(
        task_id='ETL',
        python_callable=etl,
    )

    op3= PythonOperator(
        task_id='ML_model',
        python_callable=ml_model,
    )


    op1 >> [op2,op3]
    
