import pandas as pd 
import requests
from datetime import datetime
import datetime


USER_ID = "lxi1d1lsodtuyosc12xhufdpq" 
TOKEN = "BQAX-XM1ZBONHRotRMsvqbIeQHB4Cjc2UKnZn-WXY8Zbx2yHbTvvWUQl6E9f0EK1PqeUTlgeOw5aYl5qMd9YIWttsLKn0TiAYWMYOILdryhi1sh1nSRwCfByonOa54DFlt2iYvhD0xT0zq0PQaMaNfJGJkWfnAqjYt-xDZCWMmkIy6Ji_i-qFFWxaVlTPoE_X4DS"


# Creating an function to be used in other pyrhon files
def return_dataframe(): 
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
     
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = input_variables)

    data = r.json()
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    return song_df