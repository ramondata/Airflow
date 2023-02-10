#!/usr/bin/env python3

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='my_first_dag',
    start_date=days_ago(2),
    schedule_interval="@daily"
) as dag:

    tarefa1 = EmptyOperator(task_id = 'tarefa_1')
    tarefa2 = EmptyOperator(task_id = 'tarefa_2')
    tarefa3 = EmptyOperator(task_id = 'tarefa_3')
    tarefa4 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/Users/ramon/nova_pasta_com_air_flow_nice{{data_interval_end}}/"'
    )

    tarefa1 >> [tarefa2, tarefa3]
    tarefa3 >> tarefa4