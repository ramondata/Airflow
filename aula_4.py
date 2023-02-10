#!/usr/bin/env python3

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

with DAG(
    dag_id = 'atividade_aula_4',
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    def printar():
        print('hello world!')

    tarefa_1 = PythonOperator(
        task_id = 'funcao_print',
        python_callable = printar
        )