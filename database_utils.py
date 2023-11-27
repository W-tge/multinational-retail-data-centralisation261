from sqlalchemy import create_engine, inspect, MetaData
import yaml
import pandas as pd

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
        self.creds_file = creds_file
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()  # Store engine as an instance variable

    def read_db_creds(self):
        with open(self.creds_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self):
        creds = self.db_creds
        try:
            connection_string = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            return create_engine(connection_string)
        except Exception as e:
            print(f"An error occurred while creating the engine: {e.__class__.__name__}: {e}")
            return None




