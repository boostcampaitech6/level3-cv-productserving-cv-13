from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "lukehanjun",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 1, 4)
}

def print_current_date_with_context(*args, **kwargs):
    execution_date = kwargs["ds"]
    print(f"execution_date: {execution_date}")

    execution_date_datetime = datetime.strptime(execution_date, "%Y-%m-%d").date()
    date_kor = ["월", "화", "수", "목", "금", "토", "일"]
    datetime_weeknum = execution_date_datetime.weekday()
    print(f"{execution_date}는 {date_kor[datetime_weeknum]}요일입니다")

with DAG(
    dag_id = "python_dag",
    default_args=default_args,
    schedule_interval="30 0 * * *",  # 매일 UTC 0시 30분
    tags = ["my_dags"],
    catchup=True
) as dag:

    t1 = PythonOperator(
        task_id = "print_current_date",
        python_callable=print_current_date_with_context
    )

    t1