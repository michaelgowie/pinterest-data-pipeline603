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


class AWSDBConnector:

    def __init__(self):
        with open('db_creds.yaml', 'r') as f:
            db_creds_dict = yaml.safe_load(f)

        self.HOST = db_creds_dict['HOST']
        self.USER = db_creds_dict['USER']
        self.PASSWORD = db_creds_dict['PASSWORD']
        self.DATABASE = db_creds_dict['DATABASE']
        self.PORT = db_creds_dict['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


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




def run_infinite_post_data_loop():
    k = 0
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            
            print(user_result)
            print(k)
            k += 1
            pin_payload = json.dumps({"records": [{"value": pin_result}]})
            geo_payload = json.dumps({"records": [{"value": geo_result}]}, default=serialize_datetime)
            user_payload = json.dumps({"records": [{"value": user_result}]}, default=serialize_datetime)
            response_pin = requests.request("POST", invoke_url_pin, headers=headers, data=pin_payload)
            response_geo = requests.request("POST", invoke_url_geo, headers=headers, data=geo_payload)
            response_user = requests.request("POST", invoke_url_user, headers=headers, data=user_payload)
            #print(type(pin_payload), type(geo_result), type(user_result))
            print(response_geo.status_code, response_pin.status_code, response_user.status_code)
            



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


