from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
import re
import boto3

# Initialize DatabaseConnector and DataExtractor
db_connector = DatabaseConnector()
local_db_connector = DatabaseConnector(creds_file='local_db.yaml')
data_extractor = DataExtractor(db_connector)
local_extractor = DataExtractor(local_db_connector)

# List all tables in the database
tables = data_extractor.list_db_tables()
print("Tables in the database:", tables)

# Special handling for 'legacy_users_cleaned' table
if 'legacy_users_cleaned' in tables:
    users_df = data_extractor.read_rds_table('legacy_users_cleaned')
    users_df = DataCleaning.clean_user_data(users_df)
    # Additional specific cleaning for 'user_uuid', 'date_of_birth', and 'join_date' columns
    users_df = users_df[users_df['user_uuid'].apply(lambda x: DataCleaning.is_valid_uuid(x))]
    users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], errors='coerce')
    users_df['join_date'] = pd.to_datetime(users_df['join_date'], errors='coerce')
    users_df.dropna(subset=['user_uuid', 'date_of_birth', 'join_date'], inplace=True)
    # Upload the cleaned DataFrame and rename it
    local_extractor.upload_to_db(users_df, 'dim_users')

# Iterate over remaining tables, clean the data, and re-upload
for table in tables:
    if table != 'legacy_users_cleaned':  # Skip 'legacy_users_cleaned' as it's already processed
        try:
            # Read data from the table
            t_data = data_extractor.read_rds_table(table)
            # Clean the data
            clean_data = DataCleaning.clean_user_data(t_data)
            # If the table has 'user_uuid' or 'date_uuid', clean those columns
            if 'user_uuid' in clean_data.columns:
                clean_data = DataCleaning.clean_uuid_column(clean_data, 'user_uuid')
            if 'date_uuid' in clean_data.columns:
                clean_data = DataCleaning.clean_uuid_column(clean_data, 'date_uuid')
            # Upload the cleaned data to a new table named "<table_name>_cleaned"
            local_extractor.upload_to_db(clean_data, f"{table}_cleaned")
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
        local_extractor.upload_to_db(clean_cards, "dim_card_details_cleaned")
        print("Card details data was successfully cleaned and uploaded.")
    except Exception as e:
        print(f"An error occurred while processing the card details data: {e.__class__.__name__}: {e}")


# Extract data from 'legacy_users_cleaned' into a DataFrame
users_df = local_extractor.read_rds_table('legacy_users_cleaned')

# Clean the 'date_of_birth' column in the users_df DataFrame
users_df = DataCleaning.clean_user_data(users_df)

# Assuming that clean_user_data now modifies the DataFrame in place, upload this cleaned DataFrame
local_extractor.upload_to_db(users_df, 'legacy_users_cleaned')


#Getting the store data
api_headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
stores_df = data_extractor.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", headers= api_headers)

#Cleaning the Store Data:
clean_store_data_df = DataCleaning.clean_store_data(stores_df)
local_extractor.upload_to_db(clean_store_data_df, 'dim_store_details')

#Getting AWS Data: (commented out because stopped working out of the blue and not fixed yet)
#AWS_store_data = data_extractor.extract_from_s3(s3_url='s3://data-handling-public/products.csv')
#AWS_store_data = DataCleaning.convert_product_weights(AWS_store_data)
#local_extractor.upload_to_db(AWS_store_data, 'dim_products')

#Listing tables on local postgre database
local_tables = local_extractor.list_db_tables()
print("Tables in the Postgre database:", local_tables)

#Extracting the orders info using read_rds_table

orders_table = local_extractor.read_rds_table('orders_table_cleaned')

#Removing extraneous columns from orders table:
orders_table = DataCleaning.clean_orders_data(orders_table)

print(type(orders_table))
local_extractor.upload_to_db(orders_table, 'orders_table')

#Getting 2nd Set of AWS Data:

date_events_data = data_extractor.extract_from_s3(s3_url='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
cleaned_events_data = DataCleaning.clean_date_events(df= date_events_data)
local_extractor.upload_to_db(cleaned_events_data, 'dim_date_times')


#Dropping level_0 and index cols from orders_table:

local_db_connector.drop_columns('orders_table', ['level_0', 'index'])

# Define the mappings for column data types
orders_table_column_type_mappings = {
    'date_uuid': {'data_type': 'UUID'},
    'user_uuid': {'data_type': 'UUID'},
    'card_number': {'data_type': 'VARCHAR(19)'},
    'store_code': {'data_type': 'VARCHAR(12)'},
    'product_code': {'data_type': 'VARCHAR(11)'},
    'product_quantity': {'data_type': 'SMALLINT'}
}

local_db_connector.alter_column_types('orders_table', orders_table_column_type_mappings)
#Renaming to the proper name
#local_db_connector.rename_table('legacy_users_cleaned', 'dim_users')


dim_users_column_type_mappings = {
    'first_name': {'data_type': 'VARCHAR(255)'},
    'last_name': {'data_type': 'VARCHAR(255)'},
    'date_of_birth': {'data_type': 'DATE'},
    'country_code': {'data_type': 'VARCHAR(10)'},  
    'user_uuid': {'data_type': 'UUID'},
    'join_date': {'data_type': 'DATE'}
}

local_db_connector.alter_column_types('dim_users', dim_users_column_type_mappings)
