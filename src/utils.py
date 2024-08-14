import os
import sys
from src.exception import CustomException
from src.logger import logging

def save_object(file_path, obj):
    try:
        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)
        obj.to_csv(file_path)
        logging.info("Utils function executed")

    except Exception as e:
        raise CustomException(e, sys)