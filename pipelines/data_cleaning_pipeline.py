import configparser
config = configparser.RawConfigParser()
import os.path as path
import pandas as pd
import sys
import os

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
sys.path.append(parent_directory)

from src.components.data_cleaning import DataClean

STAGE_NAME = "Data Cleaning"

cleaning_obj=DataClean()

class DataCleaningPipeline:
    def __init__(self):
        pass

    def main(self):
        try:
            dfr = cleaning_obj.read_data()
            
            list_of_dataframes = []


            for file_name, df in dfr.items():
                print(f"{file_name} has been loaded")
                
                # removing .csv part
                df_name = file_name.replace('.csv', '')
                list_of_dataframes.append(df_name)
                
            
                # Use setattr to create an attribute with the extracted name
                setattr(self, df_name, df)
                
                print(f"Assigned DataFrame from {file_name} to {df_name}")
                        
            ['city_weather', 'drivers_table', 'routes_weather', 'routes_table', 'traffic_table', 'trucks_table', 'truck_schedule_table']
            
            # print('list_of_dataframes', list_of_dataframes)
                    
            self.routes_weather = cleaning_obj.remove_columns(self.routes_weather,['chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder'])                    
            
            self.city_weather['hour'] = cleaning_obj.convert_hour_column(self.city_weather['hour'])
            self.city_weather['date_time'] = cleaning_obj.combine_date_and_hour_to_datetime(self.city_weather['date'], self.city_weather['hour'])
            
            cleaning_obj.missingValues(self.trucks_table)
            
            cleaning_obj.missingValues(self.traffic_table)
            self.traffic_table['hour'] = cleaning_obj.convert_hour_column(self.traffic_table['hour'])
            self.traffic_table['date_time'] = cleaning_obj.combine_date_and_hour_to_datetime(self.traffic_table['date'], self.traffic_table['hour'])
            
            cleaning_obj.missingValues(self.drivers_table)
            
            
            cleaning_obj.update_to_hopsworks({'city_weather':self.city_weather,  'routes_weather':self.routes_weather, 'routes_table':self.routes_table, 'truck_schedule_table':self.truck_schedule_table, 'drivers_table': self.drivers_table, 'traffic_table': self.traffic_table})
            cleaning_obj.update_to_hopsworks({'trucks_table': self.trucks_table})
        except Exception as e:
            raise e
        
if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :",STAGE_NAME)
        obj = DataCleaningPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e