import pandas as pd 

dict_data = {"a": [10, 20, 30, 40], "b": [50, 60, 70, 80]}
df = pd.DataFrame.from_dict(dict_data)
print("dataframe created from from_dict")
print(df)
print("--------------------")

df_by_columns = pd.DataFrame.from_dict(dict_data, orient="columns")
print("dataframe created from from_dict")
print(df_by_columns)

cols = ['number_1', 'number_2', 'number_3', 'number_4']
df_by_index = pd.DataFrame.from_dict(dict_data, orient="index", columns=cols)
print("dataframe created from from_dict and set the orient")
print(df_by_index)

"""
https://dataindependent.com/pandas/pandas-dataframe-from-dict-pd-df-from_dict/

Orient is short for orientation, or, a way to specify how your data is laid out.

Method 1 
– Orient (default): columns = If you want the keys of your dictionary to be the DataFrame column names

Method 2 
– Orient: index = If the keys of your dictionary should be the index values. You’ll need to be explicit about column names.

"""