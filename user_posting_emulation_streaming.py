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
invoke_url = f'https://tuxowzwx5k.execute-api.us-east-1.amazonaws.com/stage-2/streams/Kinesis-Prod-Stream/record'

headers = {'Content-Type': 'application/json'}

def send_to_api(invoke_url, payload):
    response = requests.request("POST", invoke_url, headers=headers, data=payload)

def serialize_datetime(timestamp):
    return timestamp.isoformat()


def run_infinite_post_data_loop():
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
            
            
            pin_payload = json.dumps({
                                    "StreamName":"Kinesis-Prod-Stream",
                                    "Data": {'index': pin_result['index'], 'unique_id': pin_result['unique_id'],
                                              'title': pin_result['title'], 'description': pin_result['description'], 'poster_name': pin_result['poster_name'],
                                                'follower_count': pin_result['follower_count'], 'tag_list': pin_result['tag_list'],
                                                'is_image_or_video': pin_result['is_image_or_video'], 'image_src': pin_result['image_src'],
                                                'downloaded': pin_result['downloaded'], 'save_location': pin_result['save_location'], 'category': pin_result['category']},
                                    "PartitionKey": "121704c19363.pin"
                                    })
            geo_payload = json.dumps({
                                    "StreamName":"Kinesis-Prod-Stream",
                                    "Data": {'ind': geo_result['ind'], 'timestamp': geo_result['timestamp'], 'latitude': geo_result['latitude'], 
                                        'longitude': geo_result['longitude'], 'country': geo_result['country']},
                                    "PartitionKey": "121704c19363.geo"
                                    }, default=serialize_datetime)
            user_payload = json.dumps({
                                    "StreamName":"Kinesis-Prod-Stream",
                                    "Data": {'ind': user_result['ind'], 'first_name': user_result['first_name'], 
                                             'last_name': user_result['last_name'], 'age': user_result['age'], 
                                             'date_joined': user_result['date_joined']},
                                    "PartitionKey": "121704c19363.user"
                                    }, default=serialize_datetime)
            response_pin = requests.request("PUT", invoke_url, headers=headers, data=pin_payload)
            response_geo = requests.request("PUT", invoke_url, headers=headers, data=geo_payload)
            response_user = requests.request("PUT", invoke_url, headers=headers, data=user_payload)
            connection.close()
        
            



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')