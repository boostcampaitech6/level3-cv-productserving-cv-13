import os
import requests
import pandas as pd
import json

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


OUTPUT_DIR = os.path.join(os.curdir, "data")
DOC_PATH = os.path.join(OUTPUT_DIR, "forecasts.csv")

FCST_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"
SERVICE_KEY = "XSrI8zPcKMES%2BXPnmMd6ORillvuLHIsVYDAaD3ZetMerw8dJsKyPXy9EUq%2FazX8uuxMDXImEAq1x%2BYC3SoMoXQ%3D%3D" # TODO: use your secret key


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 26),
    'end_date': datetime(2024, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# TODO 1. get_forecast 함수를 완성합니다
def get_forecast(start_date, lat, lng) -> pd.DataFrame:
    # TODO:
    #  requests, FCST_URL, SERVICE_KEY 를 활용하여 서울의 초단기 날씨 예보를 수집합니다
    #  lat, lng 는 좌표 정보이며, Pandas DataFrame 형태로 결과를 반환합니다
    base_time = '0700'
    url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst?serviceKey={SERVICE_KEY}&numOfRows=60&pageNo=1&dataType=json&base_date={start_date}&base_time={base_time}&nx={lat}&ny={lng}"
    response = requests.get(url, verify=False)
    res = json.loads(response.text)

    informations = dict()
    for items in res['response']['body']['items']['item'] :
        cate = items['category']
        fcstTime = items['fcstTime']
        fcstValue = items['fcstValue']
        temp = dict()
        temp[cate] = fcstValue
        
        if fcstTime not in informations.keys() :
            informations[fcstTime] = dict()
    #     print(items['category'], items['fcstTime'], items['fcstValue'])
    #     print(informations[fcstTime])
        informations[fcstTime][cate] = fcstValue

    deg_code = {0 : 'N', 360 : 'N', 180 : 'S', 270 : 'W', 90 : 'E', 22.5 :'NNE',
           45 : 'NE', 67.5 : 'ENE', 112.5 : 'ESE', 135 : 'SE', 157.5 : 'SSE',
           202.5 : 'SSW', 225 : 'SW', 247.5 : 'WSW', 292.5 : 'WNW', 315 : 'NW',
           337.5 : 'NNW'}

    def deg_to_dir(deg) :
        close_dir = ''
        min_abs = 360
        if deg not in deg_code.keys() :
            for key in deg_code.keys() :
                if abs(key - deg) < min_abs :
                    min_abs = abs(key - deg)
                    close_dir = deg_code[key]
        else : 
            close_dir = deg_code[deg]
        return close_dir
    
    pyt_code = {0 : '강수 없음', 1 : '비', 2 : '비/눈', 3 : '눈', 5 : '빗방울', 6 : '진눈깨비', 7 : '눈날림'}
    sky_code = {1 : '맑음', 3 : '구름많음', 4 : '흐림'}


    for key, val in zip(informations.keys(), informations.values()) :
    #     print(key, val)
        # val['LGT'] -- 낙뢰 
        template = f"""{start_date[:4]}년 {start_date[4:6]}월 {start_date[-2:]}일 {key[:2]}시 {key[2:]}분 {(int(lat), int(lng))} 지역의 날씨는 """ 
        
        
        # 맑음(1), 구름많음(3), 흐림(4)
        if val['SKY'] :
            sky_temp = sky_code[int(val['SKY'])]
    #         print("하늘 :", sky_temp)
            template += sky_temp + " "
        
        # (초단기) 없음(0), 비(1), 비/눈(2), 눈(3), 빗방울(5), 빗방울눈날림(6), 눈날림(7)
        if val['PTY'] :
            pty_temp = pyt_code[int(val['PTY'])]
    #         print("강수 여부 :",pty_temp)
            template += pty_temp
            # 강수 있는 경우
            if val['RN1'] != '강수없음' :
                # RN1 1시간 강수량 
                rn1_temp = val['RN1']
    #             print("강수량(1시간당) :",rn1_temp)
                template += f"시간당 {rn1_temp}mm "
        
        # 기온
        if val['T1H'] :
            t1h_temp = float(val['T1H'])
    #         print(f"기온 : {t1h_temp}℃")
            template += f" 기온 {t1h_temp}℃ "
        # 습도
        if val['REH'] :
            reh_temp = float(val['REH'])
    #         print(f"습도 : {reh_temp}%")
            template += f"습도 {reh_temp}% "
        # val['UUU'] -- 바람
        
        # val['VVV'] -- 바람
        
        # 풍향/ 풍속
        if val['VEC'] and val['WSD']:
            vec_temp = deg_to_dir(float(val['VEC']))
            wsd_temp = val['WSD']
    #         print(f"풍속 :{vec_temp} 방향 {wsd_temp}m/s")
            
        template += f"풍속 {vec_temp} 방향 {wsd_temp}m/s"

    print('- current template : ', template)
    return template


# TODO 2. processing 함수를 완성합니다
def processing(**kwargs) -> pd.DataFrame:
    # TODO:
    #  get_forecast 함수를 통해 수집한 예보를 가져옵니다.
    #  같은 지역에 대한 다른 시간대의 예보 데이터가 쌓일 경우, 가장 최근의 데이터를 제외하고 중복 제거합니다.
    #  예보 데이터는 수집 시점을 기준으로 2~4시간 사이의 예보를 반환합니다.
    #  중복된 데이터가 있을 시 제거해야 합니다.
    df = kwargs['task_instance'].xcom_pull(task_ids='get_forecast_task')
    
    return df

# TODO 3. save_file 함수를 완성합니다
def save_file(**kwargs):
    # TODO: get_forecast_task 를 통해 다운 받은 예보 결과를 가져온 뒤 csv 파일 형태로 저장합니다.
    #   마찬가지로 중복된 행을 제거해야 합니다.
    df = kwargs['task_instance'].xcom_pull(task_ids='processing_task')
    df = pd.DataFrame([df])
    df.to_csv('output.csv')
    

# TODO 4. 한 시간에 한번씩 서울 지역의 날씨 데이터를 수집하는 DAG를 완성합니다. 주어진 두 함수를 활용합니다.
with DAG(
        dag_id='04-crawling_weather',
        default_args=default_args,
        schedule_interval="@once",  #"* 12 * * *",  # hourly
        catchup=True,
        tags=['assignment'],
) as dag:
    execution_date = "{{ ds_nodash }}"


    # TODO: get_forecast 함수를 활용해 forecast_task 를 완성합니다.
    get_forecast_task = PythonOperator(
        task_id="get_forecast_task",
        python_callable=get_forecast,
        op_kwargs={
            'start_date': execution_date,
            'lat': 62,
            'lng': 123,
        }
    )

    processing_task = PythonOperator(
        task_id="processing_task",
        python_callable=processing,
    )

    save_task = PythonOperator(
        task_id="save_forecast_task",
        python_callable=save_file,
    )

    get_forecast_task >> processing_task >> save_task
