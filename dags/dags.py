import time
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

base_hook = BaseHook()
dbw_iris_restapi_cluster_id = "parameters_git_internals_url"
git_url = "parameters_git_internals_url"
git_branch = "parameter_git_branch"

default_arguments = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=260),
}

dag = DAG(
    dag_id="type_ur_dag_id_name",
    default_args=default_arguments,
    start_date=datetime(2023, 7, 1),
    catchup=False,
    tags=[
        "tag1",
        "tag2",
        "tag3",
        "tag4",
    ],
    schedule_interval="0 9 * * 6", ## use cron to set intervals
)
# Lista de repos disponiveis!
repositorios = [
   "repo1",
   "repo2",
   "repo3"
]

tasks = []


def wait_function(wait_time, **kwargs):
    time.sleep(wait_time)


for repositorio in repositorios:
    task_id = f"get_pull_requests_by_repo_{repositorio}"

    wait_task_id = f"wait_{task_id}"

    wait_task = PythonOperator(
        task_id=wait_task_id,
        python_callable=wait_function,
        op_kwargs={"wait_time": 4 * 60 * 60},
        dag=dag,
    )

    task = DatabricksSubmitRunOperator(
        owner="Guilherme Oliveira",
        dag=dag,
        start_date=datetime(2023, 7, 1),
        databricks_conn_id="", ##use if you're going to use databricks as a main processor
        task_id=task_id,
        json={
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": "",
                "source": "GIT",
                "base_parameters": {"repositorio": repositorio},
            },
            "git_source": {
                "git_url": git_url,
                "git_provider": "gitHub",
                "git_branch": git_branch,
            },
        },
    )

    wait_task >> task

    if tasks:
        tasks[-1] >> wait_task ## this is a way to cooldown the token

    tasks.append(task)