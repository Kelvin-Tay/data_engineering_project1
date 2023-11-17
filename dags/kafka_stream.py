# this is where the DAG for pipeline is
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

default_args ={
    'owner':'kelvin',
    'start_date':datetime(2023,9,11,18,00)
}

# GET request to api
def get_data():
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
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))


with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

stream_data()