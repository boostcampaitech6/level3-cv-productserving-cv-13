import os
import os.path as osp
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from utils.slack_notifier import task_fail_slack_alert

import pickle

# Optional advanced task
# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

OUTPUT_DIR = os.path.join(os.curdir, "output")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
    'end_date': datetime(2024, 2, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_dataset() -> pd.DataFrame:
    iris = load_iris()

    data = iris.data
    target = iris.target
    feature_names = iris.feature_names

    dataset = pd.DataFrame(data, columns=feature_names)
    dataset['target'] = target

    return dataset


# TODO 1. train_model 함수를 완성합니다. train_model 을 통해 학습한 모델을 로컬 경로에 저장하게 됩니다.
def train_model(start_date, **kwargs) -> str:
    # TODO: get_dataset 함수를 통해 다운받은 dataset 를 가져온 뒤, 모델을 학습합니다.
    dataset = get_dataset()
    X = dataset.drop('target', axis=1).values
    y = dataset['target'].values

    # Train
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    print(f"model score: {score}")

    print(f"output dir : {OUTPUT_DIR}")
    print(f"current dir : {os.curdir}")

    # TODO: 주어진 경로에 모델의 각 실행 버전을 나누어 저장합니다.
    if not osp.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    model_path = osp.join(OUTPUT_DIR, start_date)

    pickle.dump(model, open(model_path, "wb"))

    # TODO: 저장된 모델의 경로를 반환합니다.
    
    return model_path


# TODO 2. 모델을 학습하는 DAG를 완성합니다. 주어진 함수 두 개를 활용합니다.
# TODO: 슬랙을 통해 DAG 실패 알람을 받습니다.
with DAG(
        dag_id='01-simple-train',
        default_args=default_args,
        schedule_interval="30 0 * * * ",
        catchup=True,
        tags=['assignment'],
        on_failure_callback=task_fail_slack_alert
) as dag:
    execution_date = "{{ ds_nodash }}"

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=get_dataset,
    )

    train_model_task = PythonOperator(
        task_id="train_model_task",
        python_callable=train_model,
        op_kwargs={
            'start_date': execution_date,
        }
    )

    # TODO: 심화/선택과제
    #  LocalFilesystemToGCSOperator 를 이용하여 로컬이 아닌 GCS 버킷의 특정 경로에 파일을 업로드 해 봅니다.
    # upload_to_gcs_task = LocalFilesystemToGCSOperator(
    #     task_id=f'upload_to_gcs',
    #     src={},
    #     dst={},
    #     bucket={},
    #     gcp_conn_id={},
    # )

    get_data_task >> train_model_task
    # >> upload_to_gcs_task
