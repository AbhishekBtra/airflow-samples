import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from jinja2 import Environment,FileSystemLoader
from datetime import datetime


PARTITIONS = ['20220501', '20220502', '20220503']

START_DATE = datetime.strptime("2022-06-02","%Y-%m-%d")

file_dir = os.path.dirname(os.path.abspath(__file__))

with DAG(dag_id="dag2",start_date=START_DATE,schedule_interval="@once") as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for p in PARTITIONS:

        task = BashOperator(task_id="task"+p,bash_command=f"echo dir = {file_dir}")
        logging = BashOperator(task_id="logging"+p,bash_command='echo hello')


        start >> task >> logging >> end