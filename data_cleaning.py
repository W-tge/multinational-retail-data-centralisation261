class DataCleaning: 
    
    @staticmethod
    def clean_user_data(dataframe):
        # Modify the DataFrame in place
        dataframe = dataframe.dropna().reset_index(drop=True)

        # Convert columns containing 'date' to datetime
        for col in dataframe.columns:
            if 'date' in col:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')

        return dataframe
