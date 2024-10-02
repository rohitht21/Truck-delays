from pathlib import Path
import os.path as path

CONFIG_FILE_PATH =  path.abspath(path.join(__file__ ,"../../config/config.ini"))
# print('aaa',CONFIG_FILE_PATH)
PARAMS_FILE_PATH = Path("params.yaml")
SCHEMA_FILE_PATH = Path("schema.yaml")