from sqlalchemy import create_engine, inspect, MetaData
import yaml
import pandas as pd
from database_utils import DatabaseConnector
import tabula_py

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
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def retrieve_pdf_data(self):
        pdf_df = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", pages='All', multiple_tables=True )
        headers = pdf_df.columns[0]
        if pdf_df:
            for df in pdf_df:
                if df.iloc[0].tolist() == headers.tolist():
                    combined_table = pd.concat([combined_table, df.iloc[1:]], ignore_index=True)
                else:
                    combined_table = pd.concat([combined_table, df], ignore_index=True)
            return combined_table
        else:
            raise ValueError("Failed to obtain data from provided link")

        
