
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# main.py
# ...

db_connector = DatabaseConnector()

if db_connector.engine is None:
    print("Failed to create database engine.")
else:
    data_extractor = DataExtractor(db_connector)
    tables = data_extractor.list_db_tables()
    print("Tables in the database:", tables)
