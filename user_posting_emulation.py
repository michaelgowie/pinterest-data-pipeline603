from db_connector_class import AWSDBConnector

import requests
import yaml
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import datetime


random.seed(100)




new_connector = AWSDBConnector()
user_id = '121704c19363'
invoke_url_pin = f'https://tuxowzwx5k.execute-api.us-east-1.amazonaws.com/first/topics/{user_id}.pin'
invoke_url_geo = f'https://tuxowzwx5k.execute-api.us-east-1.amazonaws.com/first/topics/{user_id}.geo'
invoke_url_user = f'https://tuxowzwx5k.execute-api.us-east-1.amazonaws.com/first/topics/{user_id}.user'
headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

def send_to_api(invoke_url, payload):
    response = requests.request("POST", invoke_url, headers=headers, data=payload)

def serialize_datetime(timestamp):
    return timestamp.isoformat()

def retrieve_row(table_name: str, row_num: int, database_connection):
    query_string = text(f"SELECT * FROM {table_name} LIMIT {row_num}, 1")
    selected_row = database_connection.execute(query_string)
            
    for row in selected_row:
        result = dict(row._mapping)

    return result

def post_data_to_api(data_dict: dict, invoke_url: str):
    payload = json.dumps({"records": [{"value": data_dict}]}, default=serialize_datetime)
    response = requests.request("POST", invoke_url_geo, headers=headers, data=payload)


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_result = retrieve_row('pinterest_data', random_row, connection)
            geo_result = retrieve_row('geolocation_data', random_row, connection)
            user_result = retrieve_row('user_data', random_row, connection)
            
            post_data_to_api(pin_result, invoke_url_pin)
            post_data_to_api(geo_result, invoke_url_geo)
            post_data_to_api(user_result, invoke_url_user)
            



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


