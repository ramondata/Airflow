#!/usr/bin/env python3

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from subprocess import Popen

with DAG(
    dag_id = 'teste_operator_python',
    start_date = days_ago(4),
    schedule_interval = '@daily'
) as dag:

    def subp():
        a = Popen('mkdir /Users/ramon/pasta_feita_em_python_operator/', shell=True).communicate()
        return a
    
    task_0 = EmptyOperator(
        task_id = 'inicio_teste')

    task_1 = PythonOperator(
        task_id = 'subprocess_test_python',
        python_callable = subp
    )

    task_0 >> task_1
