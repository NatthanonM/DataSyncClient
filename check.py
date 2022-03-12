import pandas as pd
import sqlite3

conn = sqlite3.connect('pd.db')
TABLE = 'data_record'
df = pd.read_sql(f"SELECT * from {TABLE}", conn)
print('Created:')
print(df[(df['author'].str.contains('james')) | (
    df['author'].str.contains('inez')) | (df['author'].str.contains('drew'))].shape[0])
print('Updated:')
print(df[(df['author'].str.contains('betty')) | (
    df['author'].str.contains('taylor')) | (df['message'].str.contains('Forever winter if you go'))].shape[0])
'Forever winter if you go'
print('Deleted:')
print(33333 + 524288 - df.shape[0])
