#!/usr/bin/env python3

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

with DAG(
    dag_id = 'view_jinja_variables',
    start_date = days_ago(1), 
    schedule_interval = '28 22 8 2 3' 
) as dag:

    task_0 = BashOperator(
        task_id = 'cria_diratorio_base',
        bash_command = 'mkdir -p "/Users/ramon/jinja/"'
    )

    task_1 = BashOperator(
        task_id = 'data_interval_start',
        bash_command = 'cd /Users/ramon/jinja/; echo > data_interval_start_{{data_interval_start}}'
    )

    task_2 = BashOperator(
        task_id = 'data_interval_end',
        bash_command = 'cd /Users/ramon/jinja/; echo > data_interval_end_{{data_interval_end}}'
    )

    task_3 = BashOperator(
        task_id = 'ds',
        bash_command = 'cd /Users/ramon/jinja/; echo > ds_{{ds}}'
    )

    task_4 = BashOperator(
        task_id = 'ds_nodash',
        bash_command = 'cd /Users/ramon/jinja/; echo > ds_nodash_{{ds_nodash}}'
    )

    task_0 >> task_1 >> task_2 >> task_3 >> task_4