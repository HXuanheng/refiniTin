
import pandas as pd


results="results/"

# Read the files into two dataframes.
df1 = pd.read_csv(results+'data_appended.csv')
df2 = pd.read_csv(results+'ric_search.csv')

# Rename column
df1.rename(columns={'Instrument': 'RIC'}, inplace=True)

# Merge the two dataframes, using _ID column as key
df3 = pd.merge(df1, df2, on='RIC')

# Write it to a new CSV file
df3.to_csv(results+'merged.csv', index=False)