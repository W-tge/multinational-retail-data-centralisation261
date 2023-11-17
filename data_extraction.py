from sqlalchemy import create_engine, inspect, MetaData
import yaml
import pandas as pd

class DatabaseConnector:
    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self):
        creds = self.db_creds
        # Format the connection string for PostgreSQL
        connection_string = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(connection_string)
        return engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def read_rds_table(self, table_name):
        table_df = pd.read_sql_table(table_name, self.engine)
        return table_df