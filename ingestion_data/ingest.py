import pandas as pd

class Extraction():
    def __init__(self) -> None:
        self.path: str
        self.url: str
        self.data = pd.DataFrame()

    def static_file(self, path: str):
        self.path = path
        self.extension = self.__ext_checker()
        if self.extension == "csv":
            self.__read_csv()
        elif self.extension == "json":
            self.__read_json()
        elif self.extension == "parquet":
            self.__read_parquet()
        else:
            pass
        
        self.display_data()
        self.cast_data()

        return self.data
    
    def display_data(self) -> None:
        # pandas truncates information
        pd.set_option('display.max_columns', None)
        print(self.data.head(10))

    def __ext_checker(self) -> str:
        return self.path.split(".")[2]
    
    def __read_json(self):
        self.data = pd.read_json("./dataset/2017-10-02-1.json", lines=True)

        """
        lines = True 

        you attempt to import a JSON file into a pandas DataFrame, 
        yet the data is written in lines separated by endlines like '\n'.
        """

    def __read_parquet(self):
        self.data = pd.read_parquet("./dataset/yellow_tripdata_2023-01.parquet", engine="pyarrow")

        """
        Parquet library to use. If 'auto', then the option io.parquet.engine is used. 
        The default io.parquet.engine behavior is to try 'pyarrow’, falling back to 'fastparquet’ if 'pyarrow' is unavailable.

        When using the 'pyarrow' engine and no storage options are provided and a filesystem is implemented by both pyarrow.fs and fsspec (e.g. “s3://”), 
        then the pyarrow.fs filesystem is attempted first. 

        Use the filesystem keyword with an instantiated fsspec filesystem if you wish to use its implementation.

        https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html

        """


    def __read_csv(self) -> pd.DataFrame:
        """
        problem: DtypeWarning: Columns (6) have mixed types.Specify dtype option on import or set low_memory=False.
        to solve:

        

        df = pd.read_csv(
            'employees.csv',
            sep=',',
            encoding='utf-8',
            dtype=dtype

        )

        """

        dtype = {
            'first_name': str,
            'last_name': str,
            'date': str,
            'salary': str
        }

        self.data = pd.read_csv(self.path, dtype=dtype)

    def request_api(self, url):
        self.url = url

        self.__read_json_chunked()

    def __read_json_chunked(self) -> None:
        """Read github data from web with read_json to pandas DataFrame"""
        header = {'User-Agent': 'pandas'}
        chunk_size = 50000
        with pd.read_json(self.url, lines=True, storage_options=header, chunksize=chunk_size, compression="gzip") as reader: 
            for chunk in reader:
                self.data = self.data.append(chunk, ignore_index=True)
        print(self.data.head())

    def investigate_schema(self):
        print("INVESTIGATE SCHEMA ", self.data.info())

        # https://datatofish.com/data-type-pandas-dataframe/

        # checking is there any NaN value from each object-type column
        org_nan_value = self.data["org"].isnull().sum()
        print("org_nan_value ", org_nan_value)

        # checking is there any non-string value from from each object-type column
        type_nan_value = self.data["type"].isnull().sum()
        print("type_nan_value ", type_nan_value)



    def cast_data(self):
        # from display_data and investigate_schema, we will cast the appropriate data type to each column
        import json

        # how to store json: https://www.freecodecamp.org/news/postgresql-and-json-use-json-data-in-postgresql

        self.data["id"] = self.data["id"].astype("Int64")
        self.data["type"] = self.data["type"].astype("string")
        self.data["created_at"] = pd.to_datetime(self.data["created_at"])

    
class Load():
    # https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
    def __init__(self) -> None:
        self.df = pd.DataFrame
        self.engine = None
        self.connection = None
    
    def __create_connection(self):
        from sqlalchemy import create_engine 

        user = "postgres"
        password = "admin"
        host = "localhost"
        database = "mydb"
        port = 5432
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.engine = create_engine(conn_string) 

    def to_postgres(self, data: pd.DataFrame):
        from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean
        from sqlalchemy.exc import SQLAlchemyError

        self.__create_connection()

        try:
            df_schema = {
                "id": BigInteger,
                "type": String(100),
                "actor": JSON,
                "repo": JSON,
                "payload": JSON,
                "public": Boolean,
                "created_at": DateTime,
                "org": JSON
            }

            # jsonb cheatcheet -> https://medium.com/hackernoon/how-to-query-jsonb-beginner-sheet-cheat-4da3aa5082a3

            data.to_sql(name="github_data", con=self.engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)
        except SQLAlchemyError as err:
            print("error", err.__cause__)

def main():
    extract = Extraction()

    # read data from static file to dataframe
    # file_path = "./dataset/yellow_tripdata_2020-07.csv"
    file_path = "./dataset/2017-10-02-1.json"
    # file_path = "./dataset/yellow_tripdata_2023-01.parquet"
    df_result = extract.static_file(file_path)
    extract.investigate_schema()


    # read data from github dataset to dataframe (2 ways: with read_json and requests)
    # year, month, day, hour = 2023, 10, 1, 1
    # url = f"http://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"
    # print(">>>> url ", url)
    # extract.request_api(url)

    load = Load()
    load.to_postgres(df_result)


if __name__ == "__main__":
    main()

# Loading data into a data frame 
"""
there are 5 ways to load data to a dataframe: 
- from arrays
- from dict
- from lists
- from series 
- from file (static, web), example: csv/json/parquet data

"""

# Investigating data in a data frame 
"""
- investigate schema 
- convert appropriate data type
"""

# Casting data based on appropriate data types (timestamp, int, float/double, boolean, string/varchar, json object)

# Cleaning data, handle missing value, deal with NaN value 

# load data to a data warehouse (postgres) 

# Check data in data warehouse with DBeaver

# [TASK] Ingest data new york taxi to PostgreSQL

# TODO: sunday 

# working on ppt for week 1