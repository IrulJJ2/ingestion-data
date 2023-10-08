import pandas as pd 

# example 1: init a dataframe by dict without index
d = {"a": [1, 2, 3, 4], "b": [2, 4, 6, 8]}
df = pd.DataFrame(d)
print("The DataFrame ")
print(df)
print("---------------------")
print("The values of column a are {}".format(df["a"].values))

# example 2: init a dataframe by dict with different index
d = {"a": {"a1":1, "a2":2, "c":3}, "b":{"b1":2, "b2":4, "c":9}}
df = pd.DataFrame(d)
print("The DataFrame ")
print(df)