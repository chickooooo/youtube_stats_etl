import sqlite3
import pandas as pd
from datetime import datetime


class SQLInput:

    def __init__(self, db_path: str, table_name: str, input_validation=None):
        # connect to database
        connection = sqlite3.connect(db_path)
        # query all records
        data = pd.read_sql_query(f"SELECT * FROM {table_name};", connection)
        # drop id column
        data.drop(['id'], axis=1, inplace=True)
        # close the connection
        connection.close()

        if input_validation:
            input_validation.validate(data)

        self.__data = data

    def __format_date(self, x: str) -> datetime:
        return datetime.strptime(x, r"%Y-%m-%d")

    def __format_datetime(self, x: str) -> datetime:
        return datetime.strptime(x, r"%Y-%m-%d %H:%M:%S.000")

    def __format_tags(self, x: str) -> list[str]:
        return x.split("---")

    def __format_boolean(self, x: str) -> bool:
        return True if x == 'TRUE' else False

    def process_data(self) -> pd.DataFrame:
        self.__data.trending_date = self.__data.trending_date.apply(
            self.__format_date)

        self.__data.publish_time = self.__data.publish_time.apply(
            self.__format_datetime)

        self.__data.tags = self.__data.tags.apply(self.__format_tags)

        self.__data.comments_disabled = self.__data.comments_disabled.apply(
            self.__format_boolean)
        self.__data.ratings_disabled = self.__data.ratings_disabled.apply(
            self.__format_boolean)
        self.__data.video_error_or_removed = self.__data.video_error_or_removed.apply(
            self.__format_boolean)

        return self.__data
