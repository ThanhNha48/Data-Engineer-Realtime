from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator  
import json
import requests
from kafka import KafkaProducer
import logging  
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def get_and_format_data(**context):
    res = requests.get("https://randomuser.me/api/").json()
    res = res['results'][0]
    
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{res['location']['street']['number']} {res['location']['street']['name']}, " \
                      f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['postal_code'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    logging.info(f"Formatted data: {json.dumps(data, indent=2)}")
    
    # Đẩy data vào XCom để task sau dùng
    context['ti'].xcom_push(key='formatted_data', value=data)
    return data

def stream_to_kafka(**context):
    # Lấy data từ task trước
    ti = context['ti']
    data = ti.xcom_pull(key='formatted_data', task_ids='get_and_format_data')
    
    if not data:
        logging.error("No data to stream!")
        raise ValueError("No data received from previous task")
    
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],  # Tên service Kafka trong Docker network
        max_block_ms=5000
    )
    curr_time = time.time()
    
    try:
        while True:
            if time.time() > curr_time + 60:  # 1 minute
                break
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        
        logging.info(f"Đã thành công gửi data topic đến users_created': {json.dumps(data)}")
    except Exception as e:
        logging.exception("Lỗi trong khi Streaming kafka: %s", e)
        raise
    finally:
        producer.flush()  # Đảm bảo gửi ngay

with DAG(
    dag_id='user_automation',  # Tên DAG không dấu cách
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['kafka', 'api']
) as dag:

    task_get_format = PythonOperator(
        task_id='get_and_format_data',
        python_callable=get_and_format_data,
    )

    task_stream = PythonOperator(
        task_id='stream_to_kafka',
        python_callable=stream_to_kafka,
    )

    task_get_format >> task_stream  # Thứ tự chạy