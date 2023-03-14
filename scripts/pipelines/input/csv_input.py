import pandas as pd
from datetime import datetime


class CSVInput:

    def __init__(self, file_path: str, input_validation=None):
        data = pd.read_csv(file_path)

        if input_validation:
            input_validation.validate(data)

        self.__data = data

    def __format_date(self, x: str) -> datetime:
        return datetime.strptime(x, r"%y.%d.%m")

    def __format_datetime(self, x: str) -> datetime:
        return datetime.strptime(x, r"%Y-%m-%dT%H:%M:%S.000Z")

    def __format_tags(self, x: str) -> list[str]:
        return x.split("...")

    def process_data(self) -> pd.DataFrame:
        self.__data.trending_date = self.__data.trending_date.apply(
            self.__format_date)

        self.__data.publish_time = self.__data.publish_time.apply(
            self.__format_datetime)

        self.__data.tags = self.__data.tags.apply(self.__format_tags)

        return self.__data
