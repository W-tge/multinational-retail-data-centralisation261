from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
import re

# Initialize DatabaseConnector
db_connector = DatabaseConnector()

if db_connector.engine is None:
    print("Failed to create database engine.")
else:
    # Initialize DataExtractor with db_connector
    data_extractor = DataExtractor(db_connector)
    # List tables in the database
    tables = data_extractor.list_db_tables()
    print("Tables in the database:", tables)

    # Iterate over tables, clean the data, and re-upload
    for table in tables:
        try:
            # Read data from the table
            t_data = data_extractor.read_rds_table(table)
            # Clean the data
            clean_data = DataCleaning.clean_user_data(t_data)
            # Upload the cleaned data to a new table named "<table_name>_cleaned"
            data_extractor.upload_to_db(clean_data, f"{table}_cleaned")
            print(f"The Table: '{table}' was successfully cleaned and reuploaded")
        except Exception as e:
            print(f"An error occurred while cleaning and reuploading the data for table '{table}': {e.__class__.__name__}: {e}")

    # Extract, clean, and upload card details
    try:
        # Extract card details from PDF
        cards = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        # Clean the card details data
        clean_cards = DataCleaning.clean_card_data(cards)
        # Upload cleaned card details to the database
        data_extractor.upload_to_db(clean_cards, "dim_card_details_cleaned")
        print("Card details data was successfully cleaned and uploaded.")
    except Exception as e:
        print(f"An error occurred while processing the card details data: {e.__class__.__name__}: {e}")
