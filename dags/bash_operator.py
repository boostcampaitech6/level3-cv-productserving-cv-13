from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "lukehanjun",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id = "bash_dag",
    default_args=default_args,
    schedule_interval="@once",
    tags=["my_dags"]
) as dag:
    
    task1= BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task2 = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
        retries=2
    )

    task3 = BashOperator(
        task_id="pwd",
        bash_command="pwd"
    )

    task1 >> [task2, task3]