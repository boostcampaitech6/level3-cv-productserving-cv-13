import os
import os.path as osp
import shutil
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from utils.slack_notifier import task_succ_slack_alert

import pickle

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


# TODO 1. train_model 함수를 완성합니다
#  model score 가 기존 score 보다 성능이 좋은 경우엔 모델을 저장하고, 그러지 않는 경우엔 패스하도록 합니다
def train_model(start_date, **kwargs) -> str:
    # TODO: get_dataset 함수를 통해 다운받은 dataset 를 가져온 뒤, 모델을 학습합니다. (과제 1과 동일)
    dataset = get_dataset()
    X = dataset.drop('target', axis=1).values
    y = dataset['target'].values

    # Train
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    print(f"- current model score: {score}")

    # TODO: 주어진 경로에 모델의 각 실행 버전을 나누어 저장합니다. (과제 1과 동일)
    if not osp.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not osp.exists(osp.join(OUTPUT_DIR, "versions")):
        os.makedirs(osp.join(OUTPUT_DIR, "versions"), exist_ok=True)

    original_model_path = osp.join(OUTPUT_DIR, start_date)
    new_model_path = osp.join(OUTPUT_DIR, "versions", start_date)
    pickle.dump(model, open(new_model_path, "wb"))
    # TODO:
    #  model score 가 기존 score 보다 성능이 좋은 경우엔 모델을 저장하고, 그렇지 않은 경우에는 저장하지 않고 end task 를 수행합니다.
    #  update_model_task 와 pass_task 를 활용합니다.
    
    original_model = pickle.load(open(original_model_path, "rb"))
    original_score = original_model.score(X_test, y_test)
    print(f"- original model score: {original_score}")

    if score > original_score:
        return 'update_model_task'
    
    else:
        return 'end'


def update_model(start_date):
    shutil.copy(
        os.path.join(OUTPUT_DIR, "versions", f"{start_date}.pkl"),
        os.path.join(OUTPUT_DIR, f"{start_date}.pkl")
    )


# TODO 2. 모델을 학습하고 성능에 따라 저장하는 DAG를 완성합니다. 주어진 함수 세 개를 활용합니다.
with DAG(
        # TODO: 모델 저장에 성공한 경우 슬랙 알림을 전송하도록 코드를 완성합니다
        dag_id='02-model-selection',
        default_args=default_args,
        schedule_interval="30 0 * * * ",
        catchup=True,
        tags=['assignment'],
        on_success_callback=task_succ_slack_alert,
) as dag:
    execution_date = "{{ ds_nodash }}"  # Template 정의

    end = EmptyOperator(task_id="end")

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=get_dataset,
    )

    # TODO: train_model 함수와 BranchPythonOperator를 활용하여 task 를 완성합니다
    #  model score 가 기존 score 보다 성능이 좋은 경우엔 모델을 저장하고, 그러지 않는 경우엔 패스하도록 합니다
    train_model_task = BranchPythonOperator(
        task_id="train_model_task",
        # TODO: task 를 완성하시오.
        python_callable=train_model,
        op_kwargs={
            'start_date': execution_date,
        }
    )

    update_model_task = PythonOperator(
        task_id="update_model_task",
        python_callable=update_model,
        op_kwargs={
            'start_date': execution_date,
        }
    )

    # TODO 3. 모델 저장에 성공한 경우 슬랙 알림을 전송하도록 코드를 완성합니다
    get_data_task >> train_model_task >> [update_model_task, end]
