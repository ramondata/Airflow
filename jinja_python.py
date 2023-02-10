#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator 
from airflow.macros import ds_add
import pendulum

with DAG(
    dag_id = 'python_op_jinja_vars',
    start_date = pendulum.datetime(2023, 2, 8, tz = 'UTC'),
    schedule_interval = '15,30 16 * 2,3 *'
) as dag:

    def func_app(ds):
        date_ds = "esta data eh ds: %s" % ds
        date_ds_plus_10 = ds_add(ds, 10)
        print('ds', date_ds, 'ds + 10', date_ds_plus_10)

    task_0 = EmptyOperator(task_id = 'inicio')

    task_1 = PythonOperator(
        task_id = 'func_app',
        python_callable = func_app,
        op_kwargs = {'ds': "{{ds}}"}
    )

    task_0 >> task_1
