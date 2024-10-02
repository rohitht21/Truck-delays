import os
import joblib
import pandas as pd
import sys
import os.path as path

parent_directory = os.path.abspath(path.join(__file__, "../../"))
sys.path.append(parent_directory)

from src.components.data_transformation import DataTransformation  # Import DataTransformation class

STAGE_NAME = "Data Transformation"

class DataTransformationPipeline:
    def __init__(self):
        self.transformation = DataTransformation()

    def load_data_from_joblib(self, load_directory="data/joblib_files"):
        """
        Load DataFrames from joblib files.
        """
        dataframes = {}
        for file_name in os.listdir(load_directory):
            if file_name.endswith(".joblib"):
                df_name = file_name.split(".")[0]
                file_path = os.path.join(load_directory, file_name)
                dataframes[df_name] = joblib.load(file_path)
                print(f"Loaded {df_name} from {file_path}")
        return dataframes

    def save_transformed_data(self, df, save_directory="data/joblib_files"):
        """
        Save the transformed DataFrame to a joblib file.
        """
        # Create the directory if it doesn't exist
        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, "transformed_data.joblib")
        joblib.dump(df, file_path)
        print(f"Saved transformed DataFrame to {file_path}")

    # Ensure that the main method is correctly defined within the class
    def main(self):
        try:
            # Step 1: Load DataFrames from joblib files
            dataframes = self.load_data_from_joblib()

            # Inspect columns of each DataFrame to find 'remainder__delay'
            for df_name, df in dataframes.items():
                print(f"{df_name} columns: {df.columns.tolist()}")

            # Step 2: Apply Transformation (Choose the correct DataFrame containing 'remainder__delay')
            # Adjust this after finding the correct DataFrame
            df_to_transform = dataframes['schedule_df']  # Example, replace with the correct DataFrame
            transformed_df = self.transformation.fit_transform(df_to_transform)

            # Step 3: Save Transformed Data
            self.save_transformed_data(transformed_df)

            # Step 4: Save the transformer and scaler for future use
            self.transformation.save_transformers('models/scaler.joblib', 'models/transformer.joblib')

        except Exception as e:
            raise e


if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        obj = DataTransformationPipeline()
        obj.main()  # Ensure the method name here matches exactly with the defined method
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e
