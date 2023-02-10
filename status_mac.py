#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import psutil
from pync import Notifier

with DAG(
    dag_id = 'Verificador_desempenho',
    start_date = days_ago(1),
    schedule_interval = '0 16 * * *'
) as dag:

    def verificador():
        ram = psutil.virtual_memory().percent
        cpu = psutil.cpu_percent(interval=2)
        tempo_ocioso = psutil.cpu_times().idle // 2160
        batt = psutil.sensors_battery().percent

        print('\n\nSTATUS COMPUTER\n')
        print("".center(25, "*"))
        print("RAM USED", ram, "%", "\nCPU USED", cpu, "%", "\nTIME RELAX", tempo_ocioso, "hrs", "\nBATTERY", batt, "%")
        print("".center(25, "*"))
        print('\n\n')

        p = f"RAM USED:{ram}% CPU USED:{cpu}% TIME RELAX:{tempo_ocioso}:hrs BATTERY:{batt}%"
        Notifier.notify(p)


    task_1 = PythonOperator(
        task_id = 'performance',
        python_callable = verificador
    )

    task_2 = BashOperator(
        task_id = 'registro_log',
        bash_command = "mkdir -p '/Users/ramon/logs/airflow_log/{{ds_nodash}}'"
    )

    task_1 >> task_2