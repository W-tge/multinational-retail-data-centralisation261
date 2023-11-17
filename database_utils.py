from sqlalchemy import create_engine, inspect, MetaData
import yaml

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


# Usage
db_connector = DatabaseConnector()
db_engine = db_connector.init_db_engine()
