
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
import tabula_py
# main.py
# ...

db_connector = DatabaseConnector()

if db_connector.engine is None:
    print("Failed to create database engine.")
else:
    data_extractor = DataExtractor(db_connector)
    tables = data_extractor.list_db_tables()
    print("Tables in the database:", tables)


for table in data_extractor.list_db_tables():
    try:
        t_data = data_extractor.read_rds_table(table)
        clean_data = DataCleaning.clean_user_data(t_data)
        data_extractor.upload_to_db(clean_data, f"{table}_cleaned")
        print(f"The Table: '{table}' was successfully cleaned and reuploaded")
    except Exception as e:
         print(f"An error occurred while cleaning and reuploading the data: {e.__class__.__name__}: {e}")
