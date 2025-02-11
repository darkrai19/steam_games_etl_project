import pandas as pd

# extract part
columns = ['AppID', 'Name', 'Release date', 'Estimated owners', 'Price', 'Publishers', 'Categories', 'Genres', 'Tags']
df_csv = pd.read_csv(r'C:\Users\raimo\Documents\Airflow\data\steam\games.csv', usecols = columns, index_col=False)

fact_table = df_csv.rename(columns={'Publishers': 'Developers', 'Categories': 'Publishers', 'Genres': 'Categories', 'Tags': 'Genres'})

# transform part
fact_table = fact_table.apply(lambda x: x.strip() if isinstance(x, str) else x)
fact_table.columns = fact_table.columns.str.lower()
fact_table.columns = fact_table.columns.str.replace(" ", "_")

for column in ['developers', 'publishers', 'categories', 'genres']:
    fact_table[column] = fact_table[column].str.extract(r'^([^,]+)')
    fact_table[column + '_ID'] = fact_table[column].astype('category').cat.codes + 1

dim_developers = fact_table[['developers', 'developers_ID']].drop_duplicates().dropna().reset_index(drop=True).sort_values(by='developers_ID')
dim_publishers = fact_table[['publishers', 'publishers_ID']].drop_duplicates().dropna().reset_index(drop=True).sort_values(by='publishers_ID')
dim_categories = fact_table[['categories', 'categories_ID']].drop_duplicates().dropna().reset_index(drop=True).sort_values(by='categories_ID')
dim_genres = fact_table[['genres', 'genres_ID']].drop_duplicates().dropna().reset_index(drop=True).sort_values(by='genres_ID')

fact_table = fact_table.drop(columns=['developers', 'publishers', 'categories', 'genres'], axis=1)

fact_table['estimated_owners']= fact_table['estimated_owners'].str.extract(r'[-\s]*\d+\s*-\s*(\d+)|(\d+)').iloc[:, 0].astype(int)

# loading part
from sqlalchemy import create_engine
import psycopg2

tables = [fact_table, dim_developers, dim_publishers, dim_categories, dim_genres]
table_names = ['fact_table', 'dim_developers', 'dim_publishers', 'dim_categories', 'dim_genres']

def select_output():
    file_type = input("Create a csv file or load to postgres? (csv/sql): ")
    return file_type

response = select_output()

if response.lower() == 'csv':
    for table, name in zip(tables, table_names):
        file_name = f"{name}.csv"
        table.to_csv(file_name, index=False)
        print(f"{file_name} has been created.")

elif  response.lower() == 'sql':
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

else:
    print("Please choose between csv or sql option. Thank you!")