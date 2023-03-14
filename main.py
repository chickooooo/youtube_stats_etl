from scripts.full_pipeline import FullPipeline, FileType


# full pipeline
pipeline = FullPipeline()

# data_files,
# input_format,
# output_format
data = [

    # sql
    {
        "input_file_path": "./data/north_america/usa.db",
        "input_file_type": FileType.SQL,
        "input_table_name": "USA",
        "output_table_name": "usa",
    },

    # csv
    {
        "input_file_path": "./data/north_america/canada.csv",
        "input_file_type": FileType.CSV,
        "output_table_name": "canada",
    },
    {
        "input_file_path": "./data/north_america/mexico.csv",
        "input_file_type": FileType.CSV,
        "output_table_name": "mexico",
    },

    # json
    {
        "input_file_path": "./data/europe/france.json",
        "input_file_type": FileType.JSON,
        "output_table_name": "france",
    },
    {
        "input_file_path": "./data/europe/germany.json",
        "input_file_type": FileType.JSON,
        "output_table_name": "germany",
    },
    {
        "input_file_path": "./data/europe/great_britain.json",
        "input_file_type": FileType.JSON,
        "output_table_name": "great_britain",
    },
]

# data warehouse location
output_file_path = "mongodb://{add-url-here}"
# file type
output_file_type = FileType.MONGODB
# database name
output_db_name = "youtube_stats"


# process all data_files
for item in data:
    print(f"---------- {item['output_table_name']} ----------")
    pipeline.process(
        **item,
        output_file_path=output_file_path,
        output_file_type=output_file_type,
        output_db_name=output_db_name
    )
    print("\n")
