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
from airflow.datasets import Dataset

default_args = {
    "owner": "nhantd",
    "email": ["tdnhan.it@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retry_delay": pendulum.duration(minutes=1),
    "retries": 2,
}

job_name = "parallel_task_in_dag"


def get(url: str, parallel_dag_id: str, prd_id: datetime) -> None:
    endpoint = url.split('/')[-1]
    now = datetime.now()
    now = f"{now.year}-{now.month}-{now.day}T{now.hour}-{now.minute}-{now.second}"
    if prd_id is None:
        prd_id = now
    res = requests.get(url)
    res = json.loads(res.text)

    with open(f"/data/{parallel_dag_id}-{endpoint}-{prd_id}.json", 'w') as f:
        json.dump(res, f)
    time.sleep(2)


with DAG(
        dag_id=job_name,
        description="Concurrency Task in Airflow Dag",
        start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Ho_Chi_Minh"),
        schedule_interval="0 7 * * *",  # Runs every day at 07:00 AM (Timezone: Vietnam)
        catchup=False,
        params={
            'parallel_dag_id': job_name,
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

    task_get_users = PythonOperator(
        task_id='get_users',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/users', 'parallel_dag_id': '{{ params.parallel_dag_id }}', 'prd_id': '{{ params.prd_id }}'}
    )

    task_get_posts = PythonOperator(
        task_id='get_posts',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/posts', 'parallel_dag_id': '{{ params.parallel_dag_id }}', 'prd_id': '{{ params.prd_id }}'}
    )

    task_get_comments = PythonOperator(
        task_id='get_comments',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/comments', 'parallel_dag_id': '{{ params.parallel_dag_id }}', 'prd_id': '{{ params.prd_id }}'}
    )

    task_get_todos = PythonOperator(
        task_id='get_todos',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/todos', 'parallel_dag_id': '{{ params.parallel_dag_id }}', 'prd_id': '{{ params.prd_id }}'}
    )

    # start >> task_get_users >> task_get_posts >> task_get_comments >> task_get_todos >> end
    start >> [task_get_users, task_get_posts, task_get_comments, task_get_todos] >> end
