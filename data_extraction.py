from sqlalchemy import create_engine, inspect, MetaData
import yaml
import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests

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
        
