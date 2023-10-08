import pandas as pd
import numpy as np

d = np.random.normal(size=(2,3))
print("The original Numpy array")
print(d)
print("---------------------")

s = pd.DataFrame(d)
print("The DataFrame ")
print(s)