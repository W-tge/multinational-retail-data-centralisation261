import pandas as pd
import re

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