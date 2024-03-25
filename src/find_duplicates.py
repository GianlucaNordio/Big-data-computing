import pandas as pd

# Load the data from the CSV file
df = pd.read_csv('../input/uber-10k.csv')

# Find duplicate rows
print(df)
duplicates = df[df.duplicated()]

print("Duplicate Rows:")
print(duplicates)