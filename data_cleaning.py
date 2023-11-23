import pandas as pd


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
