import pandas as pd
import re
import numpy as np
import uuid 

class DataCleaning: 
    
    @staticmethod
    def clean_user_data(dataframe):
        print(f"Cleaning data for DataFrame with columns: {dataframe.columns.tolist()}")

        # Convert 'date_of_birth' and 'join_date' columns to datetime, invalid dates become NaT
        for col in ['date_of_birth', 'join_date']:
            if col in dataframe.columns:
                print(f"Cleaning column: {col}")
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')

        # Validate and convert 'user_uuid' column, invalid UUIDs become None
        if 'user_uuid' in dataframe.columns:
            print("Cleaning 'user_uuid' column")
            dataframe['user_uuid'] = dataframe['user_uuid'].apply(DataCleaning.validate_uuid)

        # Drop rows with NaT or NaN in critical columns
        critical_columns = ['date_of_birth', 'join_date', 'user_uuid']
        for col in critical_columns:
            if col in dataframe.columns:
                dataframe = dataframe.dropna(subset=[col])
        
        return dataframe

    @staticmethod
    def clean_uuid_column(dataframe, column_name):
        def is_valid_uuid(uuid_to_test, version=4):
            try:
                uuid.UUID(uuid_to_test, version=version)
                return True
            except ValueError:
                return False

        # Apply the is_valid_uuid function to filter the dataframe
        valid_uuid_mask = dataframe[column_name].apply(lambda x: is_valid_uuid(x) if pd.notna(x) else False)
        dataframe = dataframe[valid_uuid_mask]
        return dataframe


    @staticmethod
    def validate_uuid(uuid_to_test):
        try:
            val = uuid.UUID(uuid_to_test, version=4)  # Use uuid.UUID because of your import statement
            return uuid_to_test
        except ValueError:
            return None

    
    def clean_card_data(df):
        # Internal function to check the date format
        def check_date_format(date_str):
            # This regex matches dates in YYYY-MM-DD format
            if re.match(r"^\d{4}-\d{2}-\d{2}$", str(date_str)):
                return date_str
            else:
                return None  # or pd.NaT if you want to use pandas NaT for missing dates

        # Apply the check_date_format function to each element in the 'expiry_date' column
        df['expiry_date'] = df['expiry_date'].apply(check_date_format)
        # Apply the check_date_format function to each element in the 'date_payment_confirmed' column
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(check_date_format)

        # Convert to datetime, coercing errors to NaT (missing values)
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        return df

    @staticmethod
    def clean_store_data(df):
        # Replace newline characters with a space
        df['address'] = df['address'].str.replace(r'\n', ' ', regex=True)
        
        # Convert 'None' strings to actual NaN values
        df.replace('None', np.nan, inplace=True)
        
        # Convert data types
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        
        # Drop the 'index' column
        df.drop('index', axis=1, inplace=True)

        #Drop the Lat column
        df.drop('lat', axis = 1, inplace = True)

        #Reording the columns to a more logical order
        new_order = ['continent', 'country_code', 'locality', 'address', 'longitude', 'latitude',  'store_code', 'staff_numbers', 'opening_date', 'store_type', ]
        df = df.reindex(columns = new_order)


        return df
        
    
    def convert_product_weights(df):
        
        # Check if the DataFrame has the 'weight' column
        if 'weight' not in df.columns:
            raise ValueError('DataFrame must have a weight column.')

        def clean_weight(value):
            if pd.isna(value) or not isinstance(value, str):
                return np.nan
            unit = re.findall(r'([a-zA-Z]+)$', value)
            if unit and len(unit[0]) > 2:
                return np.nan
            number = float(re.findall(r'^\d*\.?\d*', value)[0])
            if 'g' in unit or 'ml' in unit:
                return number / 1000
            elif 'kg' in unit:
                return number
            elif 'oz' in unit:
                return number / 35.274
            return np.nan  # Return NaN for any other cases

        # Apply the clean_weight function to the 'weight' column
        df['weight'] = df['weight'].apply(clean_weight)
        df.dropna(subset=['weight'], inplace=True)
        df.rename(columns={'weight': 'weight (Kg)'}, inplace=True)

        return 


    @staticmethod  
    def clean_orders_data(df):
        cols_to_drop = ['first_name', 'last_name', '1']
        df = df.drop(cols_to_drop, axis=1)
        return df

    @staticmethod
    def clean_date_events(df):
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
                
        
    def clean_date_column(df, column_name):
        # Define a regex pattern to match valid dates (adjust pattern as needed)
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')

        # Remove invalid dates
        df[column_name] = df[column_name].apply(lambda x: x if date_pattern.match(str(x)) else pd.NaT)
