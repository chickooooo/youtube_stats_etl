import pandas as pd


class DataCleaning:

    def __init__(self) -> None:
        pass

    def __remove_empty_tags(self, x: list[str]) -> list[str]:
        x_cleaned = []
        empty_tags = ["", "[none]"]

        for item in x:
            if item.strip() not in empty_tags:
                x_cleaned.append(item.strip())

        return x_cleaned

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        cleaned_data = data.copy()

        cleaned_data.tags = cleaned_data.tags.apply(self.__remove_empty_tags)

        # add more cleaning steps here

        return cleaned_data
