# this is where the DAG for pipeline is
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args ={
    'owner':'kelvin',
    'start_date':datetime(2023,9,11,18,00)
}

# GET request to api
def get_data():
    import json
    import requests
    res = requests.get('https://randomuser.me/api/')
    res_json = res.json()
    res_json = res_json['results'][0]
    return res_json


# extract information from api data
def format_data(res):
    data={}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']} {location['state']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_data'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    # print(json.dumps(res, indent=3))
    # straight away publish the publish the data using kafaProducer
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    # change to ['broker:29092'] once the docker container is up due to the yaml file setting
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time()> curr_time +60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue



with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data #name of the python function
    )

# stream_data()