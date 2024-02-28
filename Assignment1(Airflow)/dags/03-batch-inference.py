import os
import csv
import pandas as pd
from sklearn.datasets import load_iris

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

import random
import pickle

OUTPUT_DIR = os.path.join(os.curdir, "output")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
    'end_date': datetime(2024, 2, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# TODO 1. get_samples 함수를 완성합니다
def get_samples() -> pd.DataFrame:
    iris = load_iris()

    data = iris.data
    # target = iris.target
    feature_names = iris.feature_names

    dataset = pd.DataFrame(data, columns=feature_names)
    # dataset['target'] = target

    # TODO: 한번 학습 시 랜덤한 5개 데이터 세트에 대한 학습을 수행하도록 구현합니다.
    #  실제 회사에서는 클라우드의 데이터 저장소나 데이터베이스에 있는 데이터를 가지고 와서 처리하지만,
    #  본 과제에서는 로컬에 있는 파일에서 랜덤으로 실험 세트를 추출해 예측하는 방식으로 진행합니다.
    print(f'- len dataset : {len(dataset)}')
    random_samples = dataset.iloc[random.sample(range(0, len(dataset)), 5)]
    return random_samples



# TODO 2. inference 함수를 완성합니다
def inference(start_date, **kwargs):
    model_path = os.path.join(OUTPUT_DIR, start_date)

    # TODO:
    #  get_samples 함수를 통해 다운받은 dataset 를 가져옵니다.
    #  주어진 model_path 에서 학습된 모델을 불러옵니다.
    dataset = get_samples()
    model = pickle.load(open(model_path, "rb"))

    # Save file as csv format
    output_dir = os.path.join(os.curdir, "data")
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d%H%M")
    output_file = os.path.join(output_dir, f"predictions_{timestamp}.csv")

    # TODO: 불러온 모델로부터 예측을 수행하고, 그 결과를 주어진 경로와 파일명 형식을 따르는 csv 파일 형태로 저장합니다.

    prediction = model.predict(dataset)
    prediction_df = pd.DataFrame(prediction)
    prediction_df.to_csv(output_file)


# TODO 1. 5분에 1번씩 예측을 수행하는 DAG를 완성합니다. 주어진 두 함수를 활용합니다.
with DAG(
        dag_id='03-batch-inference',
        default_args=default_args,
        schedule_interval="*/5 * * * *",  # Run every 5 minutes
        catchup=True,
        tags=['assignment'],
) as dag:
    execution_date = "{{ ds_nodash }}"

    get_data_task = PythonOperator(
        task_id = 'get_data',
        python_callable=get_samples,
    )

    inference_task = PythonOperator(
        task_id= 'inference',
        python_callable=inference,
        op_kwargs={
            'start_date': execution_date
        }
    )

    get_data_task >> inference_task