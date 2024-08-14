import os
import sys
import re
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from src.utils import save_object

@dataclass
class DataTransformConfig:
    data_path: str = os.path.join(os.getcwd(), "data/movies.csv")
    save_path: str = os.path.join(os.getcwd(), "data/movies_updates.csv")


class DataTransform:
    def __init__(self):
        self.transform_config = DataTransformConfig()

    def data_transform(self):
        logging.info("Entered the data transformation component")

        try:
            movies_df = pd.read_csv(self.transform_config.data_path)

            # Drop the unnecessary "Unnamed: 0" column
            movies_df = movies_df.drop(columns=["Unnamed: 0"])

            # Convert 'release-date' to datetime format
            movies_df['release-date'] = pd.to_datetime(movies_df['release-date'], format='%Y-%m-%d')

            # Strip any leading/trailing whitespace in text columns
            movies_df['Description'] = movies_df['Description'].str.strip()
            movies_df['Title'] = movies_df['Title'].str.strip()
            movies_df['Genres'] = movies_df['Genres'].str.strip()
            movies_df['Directors'] = movies_df['Directors'].str.strip()

            # Extract the numerical part from the 'Description' column
            movies_df['Gross_Millions'] = movies_df['Description'].apply(lambda x: int(re.sub(r'[^\d]', '', x)))

            # Drop the original 'Description' column
            movies_df = movies_df.drop(columns=['Description'])

            # Convert the gross to millions and round
            movies_df['Gross_Millions'] = movies_df['Gross_Millions'].apply(lambda x: f"{round(x / 1e6):,}M")

            # Remove the 'M' suffix and convert the values to integers
            movies_df['Gross_Millions'] = movies_df['Gross_Millions'].str.replace('M', '').str.replace(',', '').astype(int)

            # Display the first few rows to verify the changes
            print(movies_df.head())

            logging.info("Data Transformation successfully completed")

            save_object(self.transform_config.save_path, movies_df)

            logging.info("Pre processed data as CSV file")


        except Exception as e:
            logging.info("Caught exception")
            raise CustomException(e, sys)


if __name__ == "__main__":
    obj = DataTransform()
    obj.data_transform()



