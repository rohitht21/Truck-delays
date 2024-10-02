import psycopg2
import pandas as pd
import configparser
import os.path as path
from src.constants import *
from sqlalchemy import create_engine
import os
# from src.constants import *
# from src.utils import *

STAGE_NAME = "Data Ingestion"

class DataIngestion:
    def __init__(self):
        self.config = configparser.RawConfigParser()  # Properly initialize config parser
        self.config.read(CONFIG_FILE_PATH)  # Correct usage of read()

    def connect_to_db(self):
        try:
            # Fetching PostgreSQL connection details from config file
            dbname = self.config.get('POSTGRESQL', 'dbname2')
            user = self.config.get('POSTGRESQL', 'user')
            password = self.config.get('POSTGRESQL', 'password')
            host = self.config.get('POSTGRESQL', 'host')
            port = self.config.get('POSTGRESQL', 'port')
            
            # Establish the connection
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            return conn
        except Exception as e:
            print(f"Error connecting to the PostgreSQL database: {e}")
            raise

    def download_data_from_db(self, tables):
        try:
            conn = self.connect_to_db()
            raw_data_dir = self.config.get('DATA', 'raw_data_file')

            for table in tables:
                csv_path = os.path.join(raw_data_dir, f"{table}.csv")
                
                # Check if the file already exists
                if os.path.exists(csv_path):
                    print(f"File '{csv_path}' already exists. Skipping download.")
                else:                      
                    query = f"SELECT * FROM {table};"
                    print(f"Downloading data from {table}...")
                    
                    # Fetch the data using pandas
                    data = pd.read_sql(query, conn)
                    
                    # Save the table to a CSV file in the raw data directory
                    data.to_csv(csv_path, index=False)
                    
                    print(f"Data from {table} saved to {csv_path}")
                
            conn.close()
            print("Data downloaded successfully from PostgreSQL database.")
            
        except Exception as e:
            print(f"Error during data retrieval: {e}")
            raise e