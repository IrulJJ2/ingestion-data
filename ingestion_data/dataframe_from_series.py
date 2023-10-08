import pandas as pd

# example 1: series without index
s = {"a": pd.Series(range(1, 3)), "b": pd.Series(range(2, 4))}

df = pd.DataFrame(s)
print("dataframe created from dict of series")
print(df)
print("--------------------")

# example 2: series with index
s = {
    "a": pd.Series(range(1, 3), index=["a", "b"]),
    "b": pd.Series(range(2, 4), index=["b", "c"])
}
df = pd.DataFrame(s)
print("dataframe created from dict of series with index")
print(df)