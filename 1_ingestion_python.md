# Ingestion Data with Python

Outline: 
1. Install virtualenv
2. Install pandas

## 1. Install virtualenv

- check global installed python version 

```
python --version
```

- Install virtualenv

```
pip install virtualenv
```

- install python3.7 virtual environment locally

```
virtualenv -p python3.7 env
```

- check python version and activate virtual environment based on selected python version

```
source ./env/bin/activate

python --version
```

- deactivate python version locally

```
deactivate
```

- recheck python version

```
python --version
```

## 2. Install pandas


- Activate virtualenv
- Install pandas library

```
pip install pandas
pip install pyarrow
pip install fastparquet
```

or in case we have a large number of libraries installed, we can install all the packages at once by using the requirements.txt file. The syntax would be:

```
pip install -r requirements.txt
```

## 3. Run postgresql service with docker-compose
```
docker-compose -f ingestion_data/docker-compose.yml up postgresql -d
```

## 4. Manage postgresql with Dbeaver

- Open DBeaver, then create `New Database Connection`
- Choose postgresql <<IMAGE_BELOW>>
- Fill in the connection attributes as our settings on postgresql docker <<IMAGE_BELOW>>


## 5. Read data from source: static file (`.csv`, `.json`, `.parquet`)

Download data 

parquet: 
- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- to download:
```

wget -P dataset/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

```

csv: 
- to download:
```
wget -P dataset/ https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-07.csv.gz
gzip -d dataset/yellow_tripdata_2020-07.csv.gz
``` 

json: 
- to download:
```
wget -P dataset/ https://data.gharchive.org/2017-10-02-1.json.gz
gzip -d dataset/2017-10-02-1.json.gz
```

Read data

csv

json

parquet

## 6. Read data from Request API  

## 7. Investigating data in dataframe

## 8. Casting data based on appropriate data types

Timestamp
Int
float/double
Boolean
string/varchar
json object



