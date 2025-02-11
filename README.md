This project is an end-to-end ETL project using the kaggle dataset "Steam Games Dataset": https://www.kaggle.com/datasets/fronkongames/steam-games-dataset

The ETL only processes the .csv file included when you download the dataset from the link. The json file does not add additional information as per this project's purpose.

The steam_etl.ipynb file includes comments, data inspection, and logic in the ETL pipeline. While the steam_etl.py is a streamlined version. Both files work the same way.
Below are the simple explanations on what the files do:
  -Extract data from the csv file (games.csv), and load it into a dataframe using pandas.
  -Transform and clean the data: renaming columns, and creating fact and dimension tables
  -Loading the tables into a postgresql or as a csv file.

For the sql option: it is required that you change the values in the engine variable (postgresql+psycopg2://username:password@host:port/database)
  username: your username in pgadmin4
  password: your password in pgadmin4
  host: specify if you are using a specific host(for this example, it is configured to localhost by default)
  database: name of the database you want to load the data to

There is also an included ERD file that can be loaded in pgadmin4 to show entity relationships; primary and foreign keys.
