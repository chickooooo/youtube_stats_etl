from pymongo import MongoClient
import pandas as pd


class MongoDBOutput:

    def __init__(self, connection_url: str, db_name: str):
        self.__connection_url = connection_url
        self.__db_name = db_name

    def __get_data_dict(self, data: pd.DataFrame) -> list:
        data_dict = []

        for i in range(len(data)):
            data_dict.append(data.iloc[i].to_dict())

        return data_dict

    def save_data(self, data: pd.DataFrame, collection_name: str):
        # connect to mongodb
        client = MongoClient(self.__connection_url)
        # get database
        database = client[self.__db_name]

        # create new collection if not already present
        if not collection_name in database.list_collection_names():
            database.create_collection(collection_name)

        # select collection
        collection = database[collection_name]

        # get data in dict format
        data_dict = self.__get_data_dict(data)

        # save data
        collection.insert_many(data_dict)
        # close the connection
        client.close()
