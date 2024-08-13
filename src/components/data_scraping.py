import os
import sys
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json

@dataclass
class DataScrapingConfig:
    data_path: str = os.path.join(os.getcwd(), "data/movies.csv")


class DataScraping:
    def __init__(self):
        self.ingestion_config = DataScrapingConfig()
        self.headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
                    }

    def initiate_data_scraping(self, url):
        logging.info("Entered the data scraping component")

        try:
            response = requests.get(url, headers=self.headers)

            number_list =[]
            name_list = []
            rating_list = []
            gross_list = []
            year_list = []
            runtime_list = []
            certification_list = []
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                content = soup.find_all("h3", class_='ipc-title__text')
                for i in content:
                    full_text = i.text.strip()
                    number, name = full_text.split(' ', 1)
                    number_list.append(number)
                    name_list.append(name)

                content = soup.find_all("span", class_='ipc-rating-star--rating')
                for i in content:
                    rating = i.text.strip()
                    rating_list.append(rating)

                content = soup.find_all("span", class_='ipc-md-header ipc-md-header--h2')
                for i in content:
                    gross = i.text.strip()
                    gross_list.append(gross)

                content = soup.find_all("div", class_="sc-b189961a-7 btCcOY dli-title-metadata")
                for div in content:
                    spans = div.find_all("span", class_="sc-b189961a-8 hCbzGp dli-title-metadata-item")
                    year, runtime, certification = spans[0].text.strip(), spans[1].text.strip(), spans[2].text.strip()
                    year_list.append(year)
                    runtime_list.append(runtime)
                    certification_list.append(certification)

                movies = [
                    {
                        'id': id,
                        'name': name,
                        'rating': rating,
                        'gross': gross,
                        'year': year,
                        'runtime': runtime,
                        'certification': certification
                    }
                    for id, name, rating, gross, year, runtime, certification in zip(
                        number_list, name_list, rating_list, gross_list, year_list, runtime_list, certification_list
                    )
                ]
                logging.info("Data scraping successfully completed")
                movies = json.dumps(movies)
                return movies

        except Exception as e:
            raise CustomException(e, sys)

    def save_csv_file(self, movies):
        try:
            os.makedirs(os.path.dirname(self.ingestion_config.data_path), exist_ok=True)
            df = pd.read_json(movies)
            df.to_csv(self.ingestion_config.data_path)
            logging.info("Data saved as CSV file")

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    obj = DataScraping()
    movies = obj.initiate_data_scraping("http://www.imdb.com/list/ls098063263/")
    obj.save_csv_file(movies)



