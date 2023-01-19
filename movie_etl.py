import requests
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_movie_etl():
    
    page = 1

    movie_list = []

    while page<10:

        # Request to the api
        # Remove {Enter your API key here} and write your generated API Key
        movie_request = requests.get("https://api.themoviedb.org/3/trending/all/day?api_key="+"{Enter your API key here}"+"&page="+str(page))

        # Return response in json
        movie_json = movie_request.json()

        # Getting movie result section from json
        movie_result = movie_json['results']

        for item in movie_result:
            movie_list.append(item)

        page = page + 1

    movie_df = pd.DataFrame(movie_list)

    movie_df['Original Title'] = movie_df['title'].combine_first(movie_df['name'])

    movie_df = movie_df[['Original Title','media_type','original_language','origin_country','vote_count','vote_average','popularity']]

    movie_df.to_csv("s3://ss-airflow-movie-bucket/movie_trending.csv")