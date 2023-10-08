import pandas as pd

# example: read all the files, use original column names
df = pd.read_csv("sample.csv", sep=",")
print("Print the first row")
print(df.head(1))
print("--------------------")

df = pd.read_csv("sample.csv", sep=",", nrows=10)
print("only read 10 rows")
print("The number of row in this dataframe is {}".format(df.shape[0]))
print("--------------------")

df = pd.read_csv("sample.csv", sep=",", header=0, names=["a", "b", "c"])
print("use the new column names")
print(df.head(1))
print("--------------------")

df = pd.read_csv("sample.csv", sep=",", header=0, usecols=["animal"])
print("only rad the animal column")
print(df.head(1))