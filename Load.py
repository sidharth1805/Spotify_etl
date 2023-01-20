import Extract
import Transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

if __name__ == "__main__":

#Importing the songs_df from the Extract.py
    load_df=Extract.return_dataframe()
    if(Transform.Data_Quality(load_df) == False):
        raise ("Failed at Data Validation")
    Transformed_df=Transform.Transform_df(load_df)
    #The Two Data Frame that need to be Loaded in to the DataBase
    print(load_df)
    print(Transformed_df)

#Loading into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    #SQL Query to Create Played Songs
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS fav_artist(
        artist_name VARCHAR(200),
        count VARCHAR(200)   
    )
    """
    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)
    print("Opened database successfully")

    load_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    Transformed_df.to_sql("fav_artist", engine, index=False, if_exists='append')

    conn.close()
    print("Close database successfully")
    
    