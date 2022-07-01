import pandas as pd
from spotify_client import *
from bq_load import *

def get_tables():
    '''
    ETL function to obtain final tables and load them into BigQuery
    '''
    # Initialize client to interact with Spotify API
    client_id = "bfd320eaa2cf4397bf9f4a23159fe35b"
    client_secret = "5a7bc7833a9341149a73849fd5a953ca"
    spotify = SpotifyAPI(client_id, client_secret)

    # Album ids
    album_ids = ["5SwNGsGw1I8H361DKiYnnn", "0SHpIbyBLUugMXsl3yNkUz", "2NKxb7pk04CuZab5udkGUl", 
                "61ulfFSmmxMhc2wCdmdMkN", "1ibYM4abQtSVQFQWvDSo4J", "7aDBFWp72Pz4NZEtVBANi9", 
                "1iSTXHBhLc9ImaqyvVZGft", "2hHFZLYnwsYOOxTCrlNvg0", "16rTsMjlDt6DEbLRtxvcWu", 
                "7MMpjM0wynysTbhpvKjHrm", "31MluXLYC0ZnCSfUZ5T4GX", "3RPImDZ7Ihh5YR5iJh1gH1", 
                "4q88opkbXkvvL0iIvbs0pv", "47voGlgRLcSe9VVO4K4IQa", "6GhWU8SIWavH4IQAoeJazI", 
                "0v1DRRYBXYg1uVN1CIsyy0"]

    # Generate tracks table holding all tracks from album_ids
    tracks = []
    for album_id in album_ids:
        album = spotify.get_resource(query=f"albums/{album_id}")
        lst = []
        for item in album["tracks"]["items"]:
            lst.append([item["id"], item["name"], item["external_urls"]["spotify"], album_id])
        album_tracks = pd.DataFrame(lst, columns = ["track_id", "track_name", "track_url", "album_id"])
        tracks.append(album_tracks)
    tracks = pd.concat(tracks, ignore_index = True)

    # Generate albums table holding albums information
    albums = []
    for album_id in album_ids:
        album = spotify.get_resource(query=f"albums/{album_id}")
        albums.append([album_id, album["name"], album["artists"][0]["id"], album["total_tracks"], 
                    album["release_date"], album["external_urls"]["spotify"]])
    albums = pd.DataFrame(albums, columns = ["album_id", "album_name", "artist_id", "total_tracks", 
                                            "release_date", "album_url"])

    # Generate artist table
    response = spotify.get_resource(query=f"albums/{album_ids[0]}")
    dic = {
        "artist_id": [response["artists"][0]["id"]],
        "artist_name": [response["artists"][0]["name"]],
        "artist_url": [response["artists"][0]["external_urls"]["spotify"]]
    }
    artists = pd.DataFrame(dic)     

    # Load 3 tables into BigQuery
    project_id = "graphic-segment-355021"
    dataset_name = "spotify_data"

    bigquery_load(tracks, "tracks", dataset_name, project_id)
    bigquery_load(albums, "albums", dataset_name, project_id) 
    bigquery_load(artists, "artists", dataset_name, project_id)                                      