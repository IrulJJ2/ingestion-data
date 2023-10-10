import pandas as pd

pd.set_option('display.max_columns', None)

# example: read the first row, use original column names
df = pd.read_csv("dataset/sample.csv", sep=",")
print("Print the first row")
print(df.head(1))
print("--------------------")

# example: read only 10 rows
df = pd.read_csv("dataset/sample.csv", sep=",", nrows=10)
print("only read 10 rows")
print("The number of row in this dataframe is {}".format(df.shape[0]))
print("--------------------")

# example: read only first row and replace column names
df = pd.read_csv("dataset/sample.csv", sep=",", header=0, names=["new_column_aa", "new_column_bb", "new_column_cc"])
print("use the new column names")
print(df.head(1))
print("--------------------")

# example: read from selected column only
df = pd.read_csv("dataset/sample.csv", sep=",", header=0, usecols=["tpep_dropoff_datetime"])
print("only rad the tpep_dropoff_datetime column")
print(df.head())