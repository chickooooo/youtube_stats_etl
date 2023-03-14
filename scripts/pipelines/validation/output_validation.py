import pandas as pd
from numpy import dtype


class OutputValidation:

    __dtype_values = [
        dtype('O'),
        dtype('<M8[ns]'),
        dtype('O'),
        dtype('O'),
        dtype('int64'),
        dtype('<M8[ns]'),
        dtype('O'),
        dtype('int64'),
        dtype('int64'),
        dtype('int64'),
        dtype('int64'),
        dtype('O'),
        dtype('bool'),
        dtype('bool'),
        dtype('bool')
    ]

    __dtype_index = [
        'video_id',
        'trending_date',
        'title',
        'channel_title',
        'category_id',
        'publish_time',
        'tags',
        'views',
        'likes',
        'dislikes',
        'comment_count',
        'thumbnail_link',
        'comments_disabled',
        'ratings_disabled',
        'video_error_or_removed'
    ]

    __final_dtypes = pd.Series(data=__dtype_values, index=__dtype_index)

    def __init__(self) -> None:
        pass

    def validate(self, data: pd.DataFrame):
        assert (self.__final_dtypes == data.dtypes).all(), "incorrect datatypes"

        # add more validation checks here

        print("Output validated")
