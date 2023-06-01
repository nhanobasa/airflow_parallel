"""
Concurrency Task in Airflow Dag
"""
import json

import pendulum
from datetime import date, datetime
import time

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import yaml

default_args = {
    "owner": "nhantd",
    "email": ["tdnhan.it@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retry_delay": pendulum.duration(minutes=1),
    "retries": 2,
}

job_name = "parallel_sub_dag"


def read_yarm(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)


with DAG(
        dag_id=job_name,
        description="Concurrency Task in Airflow Dag",
        start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Ho_Chi_Minh"),
        schedule_interval="0 7 * * *",  # Runs every day at 07:00 AM (Timezone: Vietnam)
        catchup=False,
        params={
            'prd_id': Param(
                f"{date.today()}",
                type="string",
                format="date",
                title="Date Picker",
                description="Please select a date, use the button on the left for a pup-up calendar. "
                            "See that here are no times!",
            ),

        },
        tags={"parallel", "task"}
) as dag:
    start = BashOperator(task_id="Start", bash_command="date")
    end = BashOperator(task_id="End", bash_command="date")

    config_obj = read_yarm('/data/dag_config.yaml')
    parallel_num = config_obj['parallel_num']

    parallel_dag = []
    for idx in range(1, int(parallel_num) + 1):  # After 15s -> DAG update
        prd_id = dag.params['prd_id']
        parallel_dag_id = Param(f"parallel_dag_{idx}", type='string')
        # dag.params.__setitem__("parallel_dag_id", f"parallel_dag_{idx}")

        trigger_dag = TriggerDagRunOperator(
            task_id=f"get_data_api_{idx}",
            trigger_dag_id="parallel_task_in_dag",
            conf={
                'parallel_dag_id': f"parallel_dag_{idx}",
                'prd_id': '{{ params.prd_id }}'
            },
            wait_for_completion=True,
            poke_interval=10
        )
        parallel_dag.append(trigger_dag)

    start >> parallel_dag >> end
