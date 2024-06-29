import sqlite3
import pandas as pd 
import requests
from datetime import datetime, timedelta
import json

def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No se descargaron canciones. Finalizando ejecución")
        return False 

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check fue violada")

    if df.isnull().values.any():
        raise Exception("Valores NULL encontrados")

    return True

def run_spotify_etl():
    DATABASE_LOCATION = "/ruta_de_tu_proyecto_airflow/my_played_tracks.sqlite"
    USER_ID = 'Tu usuario de Spotify'
    TOKEN = 'El token generado por el refresh token de Spotify o por el curl (Leer documentación de Spotify)'

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    
    print(song_df)
    if check_if_valid_data(song_df):
        print("Datos válidos, proceda a la etapa de carga")

        conn = sqlite3.connect(DATABASE_LOCATION)
        cursor = conn.cursor()
        
        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """
        
        cursor.execute(sql_query)
        print("Base de datos abierta con éxito")

        try:
            song_df.to_sql("my_played_tracks", conn, index=False, if_exists='append')
            print("Datos insertados exitosamente.")
        except Exception as e:
            print(f"Error al insertar los datos: {e}")

        conn.close()
        print("Base de datos cerrada exitosamente.")
