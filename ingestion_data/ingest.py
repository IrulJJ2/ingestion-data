import pandas as pd

"""
TODO: 
- bikin func untuk masing2 read, sehingga bisa easy switch
- pake class
"""

"""
problem: DtypeWarning: Columns (6) have mixed types.Specify dtype option on import or set low_memory=False.
to solve:

dtype = {
    'first_name': str,
    'last_name': str,
    'date': str,
    'salary': str
}

df = pd.read_csv(
    'employees.csv',
    sep=',',
    encoding='utf-8',
    dtype=dtype

)

"""

df_csv = pd.read_csv("./dataset/yellow_tripdata_2020-07.csv")
print(df_csv.head())

# df_json = pd.read_json("./dataset/2017-10-02-1.json", lines=True)

"""
lines = True 

you attempt to import a JSON file into a pandas DataFrame, 
yet the data is written in lines separated by endlines like '\n'.
"""

# print(df_json.head())


# df_parquet = pd.read_parquet("./dataset/yellow_tripdata_2023-01.parquet", engine="pyarrow")

"""
Parquet library to use. If 'auto', then the option io.parquet.engine is used. 
The default io.parquet.engine behavior is to try 'pyarrow’, falling back to 'fastparquet’ if 'pyarrow' is unavailable.

When using the 'pyarrow' engine and no storage options are provided and a filesystem is implemented by both pyarrow.fs and fsspec (e.g. “s3://”), 
then the pyarrow.fs filesystem is attempted first. 

Use the filesystem keyword with an instantiated fsspec filesystem if you wish to use its implementation.

https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html

"""
# print(df_parquet.head())

