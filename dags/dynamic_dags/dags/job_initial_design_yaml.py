from asyncio import tasks
import imp
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id="initial_design_yaml",schedule_interval="@once",start_date=datetime(2022,5,1)) as dag:

    task1 = BashOperator(task_id='task1',bash_command='sleep 5 ; echo hello')

    task1