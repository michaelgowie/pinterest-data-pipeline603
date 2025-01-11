from multiprocessing import Process
from time import sleep
from sqlalchemy import text
from db_connector_class import AWSDBConnector

import requests
import yaml
import random
import boto3
import json
import sqlalchemy
import datetime


random.seed(100)



new_connector = AWSDBConnector()
user_id = '121704c19363'
invoke_url = f'https://tuxowzwx5k.execute-api.us-east-1.amazonaws.com/stage-2/streams/Kinesis-Prod-Stream/record'

headers = {'Content-Type': 'application/json'}

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

def put_result_to_stream(data_dict: dict, partition_key: str):
    payload = json.dumps({
                                    "StreamName":"Kinesis-Prod-Stream",
                                    "Data": data_dict,
                                    "PartitionKey": partition_key
                                    }, default=serialize_datetime)
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_result = retrieve_row('pinterest_data', random_row, connection)
            geo_result = retrieve_row('geolocation_data', random_row, connection)
            user_result = retrieve_row('user_data', random_row, connection)
            
            put_result_to_stream(pin_result, "121704c19363.pin")
            put_result_to_stream(geo_result, "121704c19363.geo")
            put_result_to_stream(user_result, "121704c19363.user")
        
            connection.close()
        
            



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')