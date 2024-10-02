import configparser
config = configparser.RawConfigParser()
import os.path as path
import pandas as pd
import sys
import os

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
sys.path.append( parent_directory)

from src.components.data_ingestion2 import DataIngestion
ingestion_obj = DataIngestion()
from src.constants import *



STAGE_NAME = "Data Ingestion"

class DataIngestionPipeline:
    def __init__(self):
        self.ingestion_obj = DataIngestion()

    def main(self):
        try:
            config.read(CONFIG_FILE_PATH)
            git_url = config.get('DATA', 'git_dir')

            # Links to CSV files in the repository
            csv_files = set(self.ingestion_obj.list_csv_files_from_git(git_url))
            
            # save the data into the database
            for git_url in csv_files:
                # Extract table name from the URL (filename without .csv)
                table_name = os.path.basename(git_url).replace('.csv', '')
                print(f"Downloading and saving data for {table_name} from Git Repo")
                self.ingestion_obj.download_and_save_data(git_url, table_name)
                print('')

        except Exception as e:
            print(f"An error occurred in the data ingestion pipeline: {e}")
            raise e

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        obj = DataIngestionPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e