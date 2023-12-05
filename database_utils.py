from sqlalchemy import create_engine, inspect, MetaData, text
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

    def drop_columns(self, table_name, columns):
        """
        Drop specified columns from a table.

        :param table_name: Name of the table.
        :param columns: List of columns to drop.
        """
        with self.engine.connect() as connection:
            for column in columns:
                try:
                    # Prepare the SQL statement using 'text' for raw SQL
                    drop_query = text(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column};")
                    # Execute the SQL statement
                    connection.execute(drop_query)
                    print(f"Column {column} dropped successfully from {table_name}.")
                except Exception as e:
                    print(f"Failed to drop column {column} from {table_name}: {e}")

    def alter_column_types(self, table_name, column_type_mappings):
        with self.engine.connect() as connection:
            for column, new_type in column_type_mappings.items():
                try:
                    # Prepare the SQL statement
                    sql = f"ALTER TABLE {table_name} ALTER COLUMN {column} TYPE {new_type['data_type']} USING {column}::{new_type['data_type']};"
                    # Execute the SQL statement
                    connection.execute(sql)
                    print(f"Column {column} type changed to {new_type['data_type']} successfully.")
                except Exception as e:
                    print(f"An error occurred: {e}")


    def rename_table(self, old_name, new_name):
        with self.engine.begin() as connection:
            rename_query = f"ALTER TABLE {old_name} RENAME TO {new_name};"
            connection.execute(rename_query)
            print(f"Table {old_name} renamed to {new_name} successfully.")




