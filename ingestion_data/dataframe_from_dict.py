import pandas as pd 

d = {"a": [1, 2, 3, 4], "b": [2, 4, 6, 8]}
df = pd.DataFrame.from_dict(d)
print("dataframe created from from_dict")
print(df)
print("--------------------")

df = pd.DataFrame.from_dict(d, orient="index")
print("dataframe created from from_dict and set the orient")
print(df)