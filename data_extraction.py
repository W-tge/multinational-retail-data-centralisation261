from sqlalchemy import create_engine, inspect, MetaData
import yaml
import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from io import StringIO, BytesIO
import uuid


class DataExtractor:
    def __init__(self, db_connector):
        self.engine = db_connector.engine  # Use the engine from the DatabaseConnector instance

    def read_rds_table(self, table_name):
        table_df = pd.read_sql_table(table_name, self.engine)
        return table_df

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        df.to_sql(name=table_name, con=self.engine, if_exists='replace', index=False)

    def retrieve_pdf_data(self, link):
        pdf_df = tabula.read_pdf(link, pages='all', multiple_tables=True )
        headers = pdf_df[0].columns
        try:
            combined_table = pd.DataFrame()
            for df in pdf_df:
                if df.iloc[0].tolist() == headers.tolist():
                    combined_table = pd.concat([combined_table, df.iloc[1:]], ignore_index=True)
                else:
                    combined_table = pd.concat([combined_table, df], ignore_index=True)
            return combined_table
        except Exception as e:
            raise ValueError("Failed to obtain data from provided link")
    

    api_headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
   
    def list_number_of_stores(store_num_endpoint, headers):
        response = requests.get(store_num_endpoint, headers=api_headers)
        if response.status_code == 200:
            data = response
            return data
        else:
            print(f"Failed to Retrieve Number of Stores. Error Code:", response.status_code)
            print(f"Response content: {response.content}")  # This will print the response body which might contain clues

            return None

    def retrieve_stores_data(self, store_data_endpoint, headers):
        full_table = pd.DataFrame()
        api_headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }
        for store_number in range(1,201):
            formatted_endpoint = store_data_endpoint.replace("{store_number}", str(store_number))
            response = requests.get(formatted_endpoint, headers= api_headers)
            #Check to make sure successful
            if response.status_code == 200:
                df = pd.json_normalize(response.json())
                full_table = pd.concat([full_table, df], ignore_index = True)
            else:
                print(f"There was an issue retrieving the data from store number: {store_number}")
                print(f"Response content: {response.content}") 
                break
        print("Store data retrieved successfully ")
        return full_table


    def extract_from_s3(self, s3_url):
        # Parse the S3 URI
        bucket_name = s3_url.split('/')[2].split('.')[0] 
        s3_file_key = '/'.join(s3_url.split('/')[3:])

        # Initialize a boto3 client
        s3_client = boto3.client('s3')

        # Retrieve the object from S3
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        s3_data = s3_object['Body'].read()

        # Check the file extension and read the data into a DataFrame accordingly
        if s3_url.endswith('.csv'):
            data_df = pd.read_csv(StringIO(s3_data.decode('utf-8')))
        elif s3_url.endswith('.json'):
            data_df = pd.read_json(BytesIO(s3_data))
        else:
            raise ValueError("File format not supported.")

        return data_df


    
    def clean_date_events(self, df):
        # Convert timestamp to datetime time object or separate columns
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce').dt.time

        # Check for unique date_uuid

        duplicates = df[df.duplicated(subset='date_uuid', keep=False)]

        # Generate new UUIDs for duplicates
        for index in duplicates.index:
            df.at[index, 'date_uuid'] = str(uuid.uuid4())

        # Validate time_period
        # Handle missing values
        print(df.isnull().sum())
    
        # Reset index after cleaning if rows are dropped
        df = df.reset_index(drop=True)

        return df
                
        
        
