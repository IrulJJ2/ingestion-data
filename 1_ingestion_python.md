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

- from [Array](./ingestion_data/dataframe_from_arrays.py)

- from [Dictionary](./ingestion_data/dataframe_from_dict.py)

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


- from Pandas `Series`

Pandas Series is a one-dimensional labeled array capable of holding data of any type (integer, string, float, python objects, etc.).

By default, `Series` is assigned an integer index, 

```
s = {
    "a": pd.Series(range(1, 3)), 
    "b": pd.Series(range(2, 4))
}

df = pd.DataFrame(s)
```

The output 

but it can be changed using the index parameter.

```

```

- from [csv static file](./ingestion_data/dataframe_from_file.py)

``````

- from API
    - Open [ingestion file](./ingestion_data/ingest.py)


### Indexing and Selecting data in a DataFrame

https://www.geeksforgeeks.org/indexing-and-selecting-data-with-pandas/ 

### Simple Ingestion Data to Postgresql

#### Identify data

#### Extract 

`.csv`, `.json`, `.parquet`

#### Investigate data in a DataFrame

#### Casting data based on appropriate data types

https://pbpython.com/Pandas_dtypes.html

Timestamp
Int
float/double
Boolean
string/varchar
json object

#### Casting data based on appropriate data types

#### Cleaning data, handle missing value, deal with NaN value 

#### Load DataFrame to Postgresql

#### Check data in Postgresql with DBeaver

#### Try it yourself

1. Ingest and set an appropriate data type for [Yellow Trip dataset](./dataset/yellow_tripdata_2020-07.csv) to PostgreSQL. Then count how many rows are ingested.

2. 