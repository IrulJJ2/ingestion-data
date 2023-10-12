# Ingestion Data with Python

Outline: 
1. Install virtualenv
2. Install libraries

## 
## 1. Install virtualenv

- check global installed python version 

```
    python --version
```

- Install virtualenv

```
    pip install virtualenv
```

- install python3.10 virtual environment locally

```
    virtualenv -p python3.10 env
```

- activate virtual environment and check python version

```
    source ./env/bin/activate
    python --version
```

- deactivate your virtual environment

```
    deactivate
```

## 2. Install Python libraries 

- Activate virtualenv
- We have a large number of libraries need to be installed, we can install all the packages at once by using the requirements.txt file. The syntax would be:

```
    pip install -r requirements.txt
```

## 3. Run postgresql service with docker-compose

https://geshan.com.np/blog/2021/12/docker-postgres/

To run Postgres with docker-compose we will create a [docker-compose-pg-only.yml](./ingestion_data/docker-compose-pg-only.yml).

To start the containers, run this command: 

```
    docker-compose -f ingestion_data/docker-compose-pg-only.yml up postgresql
```

## 4. Manage postgresql with Dbeaver

- Open DBeaver, then create `New Database Connection`
- Choose postgresql ![postgresql-driver](./img/ingestion__dbeaver-postgresql.png)
- Fill in the connection attributes as our settings on postgresql docker ![connection-info](./img/ingestion__dbeaver-info.png)

## Pandas

Pandas is a Python library used for data manipulation and analysis. It provides data structures for efficiently storing and manipulating large datasets, as well as tools for data cleaning, filtering, and transformation.

## DataFrame

A DataFrame is a 2-dimensional data structure with columns of potentially different types, like a 2 dimensional array, or a table with rows and columns. It is generally the most commonly used Pandas object.

### Create a DataFrame

- Read Data from an Array ([code](./ingestion_data/dataframe_from_arrays.py))

```

    arr_data = [ [10, 100, 1000], [40, 400, 4000]]
    df = pd.DataFrame(arr_data)

```

- Read Data from a Dictionary ([code](./ingestion_data/dataframe_from_dict.py))

We can specify how your data is laid out with `orient`. `orient` is short for orientation. By default , `orient` value is `columns`, means that the keys of your dictionary to be the DataFrame column names. 

```
    dict_data = {"a": [10, 20, 30, 40], "b": [50, 60, 70, 80]}
    df_by_columns = pd.DataFrame.from_dict(dict_data, orient="columns")
    print("dataframe created from from_dict")
    print(df_by_columns)

```

The output is: 

![default-orient](./img/ingestion__df-dict-orient-default.png)


When `orient` value is `index`, the keys of your dictionary should be the index values. We need to be explicit about column names.

```

    dict_data = {"a": [10, 20, 30, 40], "b": [50, 60, 70, 80]}
    cols = ['number_1', 'number_2', 'number_3', 'number_4']
    df_by_index = pd.DataFrame.from_dict(dict_data, orient="index", columns=cols)
    print("dataframe created from from_dict and set the orient")
    print(df_by_index)

```

The output is: 

![column-orient](./img/ingestion__df-dict-orient-column.png)


- Read Data from Pandas `Series` ([code](./ingestion_data/dataframe_from_series.py))

Pandas Series is a one-dimensional labeled array capable of holding data of any type (integer, string, float, python objects, etc.).

By default, `Series` is assigned an integer index.

```

    s = {
        "a": pd.Series(range(1, 3)), 
        "b": pd.Series(range(2, 4))
    }

    df = pd.DataFrame(s)

```

The output is: 


![series-without-index](./img/ingestion__df-series-no-index.png)



but it can be changed using the index parameter.

```

    s = {
        "a": pd.Series(range(1, 3), index=["index1", "index2"]),
        "b": pd.Series(range(2, 4), index=["index3", "index4"])
    }
    df = pd.DataFrame(s)


```

The output is: 

![series-with-index](./img/ingestion__df-series-index.png)


- Read Data from a local csv file ([code](./ingestion_data/dataframe_from_file.py))

In most cases, we read data from a file with formats, such as: `json`, `csv`, `excel`, and `parquet`. In this part, we will read a local csv file with `read_csv` function. This function creates a DataFrame from a csv file.

```

    df = pd.read_csv("dataset/sample.csv", sep=",")
    print("Print the first row")
    print(df.head(1))

```

There are other parameters in `read_csv` function. We have the option to read only some of the columns from the csv file with `usecols`. The code below reads data on column `tpep_dropoff_datetime` only, from `dataset/sample.csv`.

```

    df = pd.read_csv("dataset/sample.csv", sep=",", header=0, usecols=["tpep_dropoff_datetime"])
    print("only rad the tpep_dropoff_datetime column")
    print(df.head())

```

- Read `json` data from API ([code](./ingestion_data/ingest.py))

The `read_json` function can read a `json` file from a local file or a URL and convert them into a DataFrame. By default, `read_json` assumes that the `json` file contains a list of objects, where each object represents a row in the DataFrame.

```

    def __read_json_chunked(self) -> None:
        storage_options = {'User-Agent': None}
        chunk_size = 50000
        with pd.read_json(self.url, lines=True, storage_options=storage_options, chunksize=chunk_size, compression="gzip") as reader: 
            for chunk in reader:
                self.dataframe = pd.concat([self.dataframe, chunk], ignore_index=True)
        print(self.dataframe.head())

```

If the `json` data are quite large, we can utilize `chunksize` parameter to partially read the data.

### Indexing and Selecting data in a DataFrame

Indexing means selecting particular rows and columns from a DataFrame. There are several ways to do indexing with Pandas, some indexing methods are: 

#### DataFrame[]

this function also known as indexing operator

#### DataFrame.loc[]

this function is used for labels

- selecting a single column
- selecting multiple columns

#### DataFrame.iloc[] 

this function is used for positions or integer based

- selecting a single row
- selecting multiple rows
- selecting two rows and three columns

#### DataFrame.ix[] 

this function is used for both label and integer based

- selecting a single row
- selecting multiple rows
- selecting two rows and three columns


https://www.geeksforgeeks.org/indexing-and-selecting-data-with-pandas/ 

### Simple Ingestion Data to Postgresql

#### Identify data

#### Extract data 

To extract `csv` and `json` data has already explained above.

Here we are going to extract data from a `parquet` file. 
To extract `parquet` data: 

```
    self.dataframe = pd.read_parquet(self.path, engine="pyarrow")
```

#### Investigate data in a DataFrame

```
    def investigate_schema(self):
        # looking at DataFrame schema 
        print("df schema ", self.dataframe.info())

        # checking is there any NaN value from `org` column
        org_nan_value = self.dataframe["org"].isnull().sum()
        print("org_nan_value ", org_nan_value)

        # checking is there any non-string value from from `type` column
        type_nan_value = self.dataframe["type"].isnull().sum()
        print("type_nan_value ", type_nan_value)

```


#### Casting data based on appropriate data types

https://pbpython.com/Pandas_dtypes.html

Timestamp
Int
float/double
Boolean
string/varchar
json object



```
    def cast_data(self):
        self.dataframe["id"] = self.dataframe["id"].astype("Int64")
        self.dataframe["type"] = self.dataframe["type"].astype("string")
        self.dataframe["created_at"] = pd.to_datetime(self.dataframe["created_at"])
```

#### Cleaning data, handle missing value, deal with NaN value 

#### Load DataFrame to Postgresql

```
    def __create_connection(self):
        from sqlalchemy import create_engine 

        user = "postgres"
        password = "admin"
        host = "localhost"
        database = "mydb"
        port = 5432
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.engine = create_engine(conn_string) 

    def to_postgres(self, db_name: str, data: pd.DataFrame):
        from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean
        from sqlalchemy.exc import SQLAlchemyError

        self.db_name = db_name
        self.__create_connection()

        try:
            # TODO: manage schema for each dataset
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

            data.to_sql(name=self.db_name, con=self.engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)
        except SQLAlchemyError as err:
            print("error >> ", err.__cause__)

```

#### Check data in Postgresql with DBeaver

- Open DBeaver
- Refresh connection to your local postgresql
- Open new script to write SQL syntax

```

select count(1) from github_data

select actor, actor->>'id' as actor_id from github_data limit 100

```

#### Try it yourself

1. Ingest and set an appropriate data type for [Yellow Trip dataset](./dataset/yellow_tripdata_2020-07.csv) to PostgreSQL. Then count how many rows are ingested.
