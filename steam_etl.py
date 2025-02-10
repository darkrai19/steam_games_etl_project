import pandas as pd
import sqlite3

df_csv = pd.read_csv(r'C:\Users\raimo\Documents\Airflow\data\steam\games.csv', index_col=False)

fact_table = df_csv[['AppID', 'Name', 'Release date', 'Estimated owners', 'Price', 'Publishers', 'Categories', 'Genres', 'Tags']]

fact_table = fact_table.rename(columns={'Publishers': 'Developers', 'Categories': 'Publishers', 'Genres': 'Categories', 'Tags': 'Genres'})

fact_table = fact_table.apply(lambda x: x.strip() if isinstance(x, str) else x)
fact_table.columns = fact_table.columns.str.lower()
fact_table.columns = fact_table.columns.str.replace(" ", "_")

fact_table['developers'] = fact_table['developers'].str.extract(r'^([^,]+)')
fact_table['publishers'] = fact_table['publishers'].str.extract(r'^([^,]+)')
fact_table['categories'] = fact_table['categories'].str.extract(r'^([^,]+)')
fact_table['genres'] = fact_table['genres'].str.extract(r'^([^,]+)')

for column in ['developers', 'publishers', 'categories', 'genres']:
    fact_table[column + '_ID'] = fact_table[column].astype('category').cat.codes + 1

dim_developers = fact_table[['developers', 'developers_ID']].drop_duplicates().dropna().reset_index(drop=True)
dim_publishers = fact_table[['publishers', 'publishers_ID']].drop_duplicates().dropna().reset_index(drop=True)
dim_categories = fact_table[['categories', 'categories_ID']].drop_duplicates().dropna().reset_index(drop=True)
dim_genres = fact_table[['genres', 'genres_ID']].drop_duplicates().dropna().reset_index(drop=True)

fact_table = fact_table.drop(columns=['developers', 'publishers', 'categories', 'genres'], axis=1)

fact_table['estimated_owners']= fact_table['estimated_owners'].str.extract(r'[-\s]*\d+\s*-\s*(\d+)|(\d+)').iloc[:, 0].astype(int)

from sqlalchemy import create_engine
import psycopg2

try:
    engine = create_engine('postgresql+psycopg2://postgres:Killerrem159!!@localhost:5432/steam')
    fact_table.to_sql('fact_table', con=engine, schema='steam_schema', if_exists='replace', index=False)
    dim_developers.to_sql('dim_developers', con=engine, schema='steam_schema', if_exists='replace', index=False)
    dim_publishers.to_sql('dim_publishers', con=engine, schema='steam_schema', if_exists='replace', index=False)
    dim_categories.to_sql('dim_categories', con=engine, schema='steam_schema', if_exists='replace', index=False)
    dim_genres.to_sql('dim_genres', con=engine, schema='steam_schema', if_exists='replace', index=False)
    print('Data loaded successfully')
except Exception as e:
    print(f"Failed to load data. Error: {e}")

