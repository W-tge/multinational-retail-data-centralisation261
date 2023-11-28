import pandas as pd
import re
import numpy as np

class DataCleaning: 
    
    @staticmethod
    def clean_user_data(dataframe):
        # Drop rows containing a NULL anywhere and resetting the index
        dataframe = dataframe.dropna().reset_index(drop=True)

        # Convert columns containing 'date' to datetime
        for col in dataframe.columns:
            if 'date' in col:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')

        return dataframe
    
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

        return df