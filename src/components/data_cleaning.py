import configparser
config = configparser.RawConfigParser()
import os.path as path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.constants import *
import seaborn as sns
import re
import os
import glob
import hopsworks
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging

# Set logging level to ERROR globally to suppress unnecessary warnings/info
logging.basicConfig(level=logging.ERROR)




class DataClean:
    
    def __init__(self):
        self.config = config.read(CONFIG_FILE_PATH)
        
        # Initialize the Hopsworks connection as None
        self.connection = None
         
         # Database connection details
        db_config = {
            'user': config.get('POSTGRESQL', 'user'),
            'password': config.get('POSTGRESQL', 'password'),
            'host': config.get('POSTGRESQL', 'host'),
            'port': config.get('POSTGRESQL', 'port'),
            'database': config.get('POSTGRESQL', 'dbname2')
        }

        # Create the database connection string
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Create a SQLAlchemy engine and store it as an instance variable
        self.engine = create_engine(connection_string)
        print("Database connection initialized.")
        
    def connect_to_hopsworks(self):
        if self.connection is None:
            project_name = config.get('HOPSWORKS', 'project_name')
            api_key = config.get('HOPSWORKS', 'api_key')

            self.connection = hopsworks.login(
                project=project_name,
                api_key_value=api_key
            )
            print("Connected to Hopsworks")
        return self.connection

    
    
    def read_data(self):

        # Get the list of all tables in the database
        with self.engine.connect() as connection:
            result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            tables = [row[0] for row in result]

        self.dataframes = {}

        # Read each table into a DataFrame
        for table in tables:
            df = pd.read_sql_table(table, self.engine)

            # Create a primary key column
            df['primary_key'] = [f"{table}_{i}" for i in range(len(df))]

            # Add the 'event_date' column
            current_date = datetime.now().date()
            df['event_date'] = current_date

            # Store the DataFrame in the dictionary
            self.dataframes[table] = df

        print("Loaded tables:", list(self.dataframes.keys()))

        return self.dataframes

    
    def missingValues(self, df):
        return df.dropna(inplace=True)
    
    def remove_columns(self, df, columns_to_remove):
        df_cleaned = df.drop(columns=columns_to_remove)
        return df_cleaned
    
    def convert_to_datetime(self, df, column):
        try:
            # Convert the column to datetime
            df[column] = pd.to_datetime(df[column])
            print(f"Successfully converted '{column}' to datetime.")
        except Exception as e:
            print(f"Error converting column '{column}' to datetime: {e}")
    
        return df
    
    def convert_hour_column(self, hour_column):
        def convert_to_24_hour_format(hour):
            # Convert to string and pad with zeros to ensure 4 digits (e.g., '100' -> '0100')
            hour_str = str(hour).zfill(4)
            # Insert colon between the first two and last two digits
            return f"{hour_str[:2]}:{hour_str[2:]}"
        
        # Apply the conversion function to the entire column
        return hour_column.apply(convert_to_24_hour_format)

    def combine_date_and_hour_to_datetime(self, date_column, hour_column):
        # Combine the date and hour columns into one string
        combined = date_column + ' ' + hour_column
        
        # Convert the combined string into a pandas datetime column
        return pd.to_datetime(combined, format='%Y-%m-%d %H:%M')
    
    def update_to_hopsworks(self, dataframes_dict):
        # Assuming dataframes_dict is a dictionary where keys are table names and values are DataFrames
        inspector = inspect(self.engine)
        # Connect to Hopsworks only once, outside the loop
        if self.connection is None:
            connection = self.connect_to_hopsworks()
            fs = connection.get_feature_store() 
        else:
            fs = self.connection.get_feature_store()
        
        # Avoid redundant processing by checking if DataFrame is already processed
        processed_tables = set()
        
        for df_name, df in dataframes_dict.items():
            if df_name in processed_tables:
                continue  # Skip already processed table
            
            # Generate the cleaned table name
            df_name_cleaned = f"{df_name}_cleaned"
            
            # Check if the table exists in the database
            if inspector.has_table(df_name_cleaned):
                print(f"Table {df_name_cleaned} already exists in the database.")
            else:
                # Save the DataFrame to the database
                df.to_sql(df_name_cleaned, self.engine, if_exists='fail', index=False)
                print(f"Created and saved data to the table '{df_name_cleaned}' in the database.")

            
            
            # Check if the feature group already exists in Hopsworks
            try:
                existing_fg = fs.get_feature_group(df_name, version=1)
                print(f"Feature group '{df_name}' already exists in Hopsworks.")
                continue  # Move to the next DataFrame in the loop
            except:
                pass

            # Create a new feature group in the Feature Store
            feature_group = fs.create_feature_group(
                name=df_name,  # Use df_name as the feature group name
                version=1,
                description=f"Feature group for {df_name}",
                primary_key=['primary_key'],  # You can adjust the primary key if needed
            )

            # Insert the DataFrame into the feature group in Hopsworks
            feature_group.insert(df)
            print(f"Inserted {df_name} into the Hopsworks Feature Store as feature group '{df_name}'")

            processed_tables.add(df_name)

    
    # def update_to_hopsworks(self, df, df_name):
        
    #     inspector = inspect(self.engine)
        
    #     df_name_cleaned = f"{df_name}_cleaned"
    #     if inspector.has_table(df_name_cleaned):
    #         print(f"Table {df_name_cleaned} already exists in the database.")
    #     else:
    #         df.to_sql(df_name_cleaned, self.engine, if_exists='fail', index=False)
    #         print(f"Created and saved data to the table '{df_name}' in the database.")


    #     connection = self.connect_to_hopsworks()
    #     fs = connection.get_feature_store()  
              
    #     # Define a feature group name
    #     feature_group_name = df_name
        
    #     # Check if the feature group already exists
    #     try:
    #         existing_fg = fs.get_feature_group(df_name, version=1)
    #         print(f"Feature group '{df_name}' already exists in Hopsworks.")
    #         return  
    #     except:
    #         pass

    #     # Create a feature group in the Feature Store
    #     feature_group = fs.create_feature_group(
    #         name=feature_group_name,
    #         version=1,
    #         description=f"Feature group for {feature_group_name}",
    #         primary_key=['primary_key'],  
    #     )

    #     # Insert the DataFrame into the feature group in Hopsworks
    #     feature_group.insert(df)
    #     print(f"Inserted {df_name} into the Hopsworks Feature Store as feature group '{feature_group_name}'")