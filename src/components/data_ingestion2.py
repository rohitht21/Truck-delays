import pandas as pd
from sqlalchemy import create_engine, inspect
import psycopg2
import requests
from bs4 import BeautifulSoup
import os
import time
import configparser
import os.path as path
from src.constants import *
import os
STAGE_NAME = "Data Ingestion"

class DataIngestion:
    def _init_(self):
        self.config = configparser.RawConfigParser() 
        self.config.read(CONFIG_FILE_PATH)  
        
        # Connect to the database
        self.conn = self.connect_to_db()
        self.engine = create_engine(f'postgresql+psycopg2://{self.config.get("POSTGRESQL", "user")}:'
                                f'{self.config.get("POSTGRESQL", "password")}@'
                                f'{self.config.get("POSTGRESQL", "host")}:'
                                f'{self.config.get("POSTGRESQL", "port")}/'
                                f'{self.config.get("POSTGRESQL", "dbname2")}')

    def connect_to_db(self):
        try:
            # PostgreSQL connection details from config file
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
        
    def list_csv_files_from_git(self, git_repo_url):
        try:
            # Scrape the GitHub repository webpage to find CSV files
            response = requests.get(git_repo_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            csv_files = []

            # Find all links to CSV files
            for link in soup.find_all('a', href=True):
                file_url = link['href']
                if file_url.endswith('.csv'):
                    full_url = f"https://raw.githubusercontent.com/{file_url.replace('/blob/', '/')}"
                    csv_files.append(full_url)
                    

            return csv_files

        except Exception as e:
            print(f"Error listing CSV files from GitHub: {e}")
            raise
    
    def download_and_save_data(self, git_url, table_name):
        try:
            # Download the data from the Git URL
            response = requests.get(git_url)
            response.raise_for_status()

            # Save the downloaded content to a temporary CSV file
            temp_csv_path = 'temp_data.csv'
            with open(temp_csv_path, 'wb') as file:
                file.write(response.content)

            # Read the data into a DataFrame
            df = pd.read_csv(temp_csv_path)
            
            # Save the DataFrame to a PostgreSQL table
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data from Git Repo has been successfully saved to the table '{table_name}' in the database.")

        except Exception as e:
            print(f"Error downloading or saving data: {e}")
            raise
        finally:
            # Clean up temporary files if they exist
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)