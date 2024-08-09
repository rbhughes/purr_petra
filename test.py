import pandas as pd
import numpy as np

# Create a sample DataFrame
df = pd.DataFrame({"A": range(15)})

# Your test data
test_data = [
    [1.0, 2.0, 3.0],
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
]

# Try to assign the data
df["B"] = test_data

print(df["B"].dtype)
