import pandas as pd


class InputValidation:

    __required_columns = {
        'category_id',
        'channel_title',
        'comment_count',
        'comments_disabled',
        'dislikes',
        'likes',
        'publish_time',
        'ratings_disabled',
        'tags',
        'thumbnail_link',
        'title',
        'trending_date',
        'video_error_or_removed',
        'video_id',
        'views'
    }

    def __init__(self) -> None:
        pass

    def validate(self, data: pd.DataFrame):
        assert set(data.columns) == self.__required_columns, "incorrect columns"

        # add more validation checks here

        print("Input validated")
