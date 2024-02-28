from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_world():
    print("World")

with DAG(
    dag_id = "Hello_world",
    description = "My First DAG",
    start_date = days_ago(2),
    schedule_interval="0 6 * * *", # (UTC)
    tags = ["my_dags"]

) as dag:

    t1 = BashOperator(
        task_id="print_hello",
        bash_command = "echo Hello",
        owner = "heumsi"
    )

    t2 = PythonOperator(
        task_id="print_world",
        python_callable = print_world
    )

    t1 >> t2