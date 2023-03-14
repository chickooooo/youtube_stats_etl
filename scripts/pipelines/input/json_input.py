import pandas as pd
from datetime import datetime


class JSONInput:

    def __init__(self, file_path: str, input_validation=None):
        data = pd.read_json(file_path)

        if input_validation:
            input_validation.validate(data)

        self.__data = data

    def __format_date(self, x: int) -> datetime:
        return datetime.fromtimestamp(x)

    def process_data(self) -> pd.DataFrame:
        self.__data.trending_date = self.__data.trending_date.apply(
            self.__format_date)

        return self.__data
