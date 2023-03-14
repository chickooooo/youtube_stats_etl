import pandas as pd
from enum import Enum

from scripts.pipelines.input.csv_input import CSVInput
from scripts.pipelines.input.json_input import JSONInput
from scripts.pipelines.input.sql_input import SQLInput
from scripts.pipelines.output.mongodb_output import MongoDBOutput
from scripts.pipelines.validation.input_validation import InputValidation
from scripts.pipelines.validation.output_validation import OutputValidation
from scripts.pipelines.processing.data_cleaning import DataCleaning


class FileType(Enum):
    """Enum for type of file"""
    CSV = 1
    JSON = 2
    SQL = 3
    MONGODB = 4


class FullPipeline:

    def __init__(self):
        self.__input_validation = InputValidation()
        self.__output_validation = OutputValidation()
        self.__data_cleaning = DataCleaning()

    def __process_csv(self, file_path: str) -> pd.DataFrame:
        input = CSVInput(file_path, self.__input_validation)
        return input.process_data()

    def __process_json(self, file_path: str) -> pd.DataFrame:
        input = JSONInput(file_path, self.__input_validation)
        return input.process_data()

    def __process_sql(self, db_path: str, table_name: str) -> pd.DataFrame:
        input = SQLInput(db_path, table_name, self.__input_validation)
        return input.process_data()

    def __save_to_mongodb(
            self,
            data: pd.DataFrame,
            connection_url: str,
            db_name: str,
            collection_name: str):
        output = MongoDBOutput(connection_url, db_name)
        output.save_data(data, collection_name)

    def process(self,
                input_file_path: str,
                input_file_type: FileType,
                output_file_path: str,
                output_file_type: FileType,
                input_db_name=None,
                input_table_name=None,
                output_db_name=None,
                output_table_name=None,
                ):
        # processed data
        data = pd.DataFrame()

        print("Loading data")

        # load, validate, transform
        if input_file_type is FileType.CSV:
            data = self.__process_csv(input_file_path)

        elif input_file_type is FileType.JSON:
            data = self.__process_json(input_file_path)

        elif input_file_type is FileType.SQL:
            assert input_table_name, "table name required"
            data = self.__process_sql(input_file_path, input_table_name)

        print("Cleaning data")

        # clean
        data = self.__data_cleaning.clean_data(data)

        print("Validating data")

        # validate
        self.__output_validation.validate(data)

        print("Saving data")

        # save
        if output_file_type is FileType.MONGODB:
            assert output_db_name, "database name required"
            assert output_table_name, "collection name required"
            self.__save_to_mongodb(
                data, output_file_path,
                output_db_name,
                output_table_name
            )
