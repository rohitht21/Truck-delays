import configparser
config = configparser.RawConfigParser()
import os.path as path
import pandas as pd
import sys
import os

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
# print('parent directory',parent_directory)
sys.path.append( parent_directory)

from src.components.data_ingestion import DataIngestion

STAGE_NAME = "Data Ingestion"
ingestion_obj = DataIngestion()

class DataIngestionPipeline:
    def __init__(self):
        self.ingestion_obj = DataIngestion()

    def main(self):
        try:
            # List of tables you want to download from the PostgreSQL database
            tables_to_download = ["city_weather", "drivers_table", "routes_table", "routes_weather",  "traffic_table", "truck_schedule_table", "trucks_table"]
            
            # Call the method to download data and store it as CSVs
            self.ingestion_obj.download_data_from_db(tables_to_download)
        except Exception as e:
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