import yaml
import sqlalchemy

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