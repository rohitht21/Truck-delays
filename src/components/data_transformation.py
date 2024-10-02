# src/components/data_transformation.py

import pandas as pd
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
import joblib

class DataTransformation:
    def __init__(self):
        self.scaler = None
        self.transformer = None

    def fit_transform(self, df):
        # Extract the target variable
        y = df['remainder__delay']
        
        # Drop the target variable from the DataFrame
        df_features = df.drop(columns=['remainder__delay'])

        # Identify categorical and numerical columns
        categorical_cols = df_features.select_dtypes(include=['object']).columns.tolist()
        numerical_cols = ['remainder__distance', 'remainder__average_hours']

        # One-Hot Encoding for categorical columns
        self.transformer = ColumnTransformer(transformers=[
            ('cat', OneHotEncoder(drop='first', sparse_output=False), categorical_cols)
        ], remainder='passthrough')  # Leave numerical columns unchanged
        
        # Fit and transform the features
        transformed_data = self.transformer.fit_transform(df_features)
        transformed_df = pd.DataFrame(transformed_data, columns=self.transformer.get_feature_names_out())

        # Scaling numerical features
        self.scaler = StandardScaler()
        transformed_df[numerical_cols] = self.scaler.fit_transform(transformed_df[numerical_cols])

        # Add the target variable back to the DataFrame
        transformed_df['remainder__delay'] = y.values

        return transformed_df

    def save_transformers(self, scaler_path, transformer_path):
        joblib.dump(self.scaler, scaler_path)
        joblib.dump(self.transformer, transformer_path)

    def load_transformers(self, scaler_path, transformer_path):
        self.scaler = joblib.load(scaler_path)
        self.transformer = joblib.load(transformer_path)

    def transform(self, df):
        # Transform features using the saved transformers
        transformed_data = self.transformer.transform(df)
        transformed_df = pd.DataFrame(transformed_data, columns=self.transformer.get_feature_names_out())
        
        # Scale numerical columns
        numerical_cols = ['remainder__distance', 'remainder__average_hours']
        transformed_df[numerical_cols] = self.scaler.transform(transformed_df[numerical_cols])

        return transformed_df
