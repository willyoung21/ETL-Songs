import pandas as pd
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import sys
import numpy as np
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '../extract'))
from db_connection import establecer_conexion, cerrar_conexion

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Conectar a la base de datos y cargar los datos
engine, session = establecer_conexion()  # Cambiado para usar la nueva función de conexión
query = "SELECT * FROM grammy_awards;"
df = pd.read_sql(query, engine)

# Cerrar la conexión a la base de datos
cerrar_conexion(session)

# Eliminar columnas innecesarias
columns_to_drop = ['img', 'updated_at', 'published_at']
df_clean = df.drop(columns=columns_to_drop)

# Eliminar filas donde 'nominee' es nulo
df_clean = df_clean.dropna(subset=['nominee'])

# Rellenar valores nulos en 'workers' con 'unknown worker'
df_clean['workers'] = df_clean['workers'].fillna('unknown worker')

# Configurar credenciales de Spotify
client_id = os.getenv('SPOTIFY_CLIENT_ID')
client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

# Verificar si las credenciales están configuradas
if not client_id or not client_secret:
    raise ValueError("Credenciales de Spotify no encontradas. Verifica tu archivo .env.")

# Autenticación con Spotify
auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)


def get_artist_from_spotify(song_title):
    try:
        result = sp.search(q=song_title, type='track', limit=1)
        if result['tracks']['items']:
            return result['tracks']['items'][0]['artists'][0]['name']
        return "Artist Not Found"
    except Exception as e:
        print(f"Error retrieving artist for {song_title}: {e}")
        return None


# Imputar los valores nulos en 'artist' usando la API de Spotify
df_nulos_artist = df_clean[df_clean['artist'].isnull()]

for idx, row in df_nulos_artist.iterrows():
    song_title = row['nominee']
    artist_name = get_artist_from_spotify(song_title)
    df_clean.at[idx, 'artist'] = artist_name
    print(f"Rellenando fila {idx}: Artista encontrado -> {artist_name}")


# Función para obtener el track_id, album_id o artist_id de Spotify basado en el nominee o artist
def get_spotify_id(nominee, artist):
    try:
        # Primero buscamos como track
        result = sp.search(q=nominee, type='track', limit=1)
        if result['tracks']['items']:
            print(f"Track encontrado: {nominee} - {result['tracks']['items'][0]['id']}")
            return result['tracks']['items'][0]['id']  # Si encuentra un track, devolvemos el track_id

        # Si no se encuentra el track, buscamos el álbum
        result = sp.search(q=nominee, type='album', limit=1)
        if result['albums']['items']:
            print(f"Álbum encontrado: {nominee} - {result['albums']['items'][0]['id']}")
            return result['albums']['items'][0]['id']  # Si encuentra un álbum, devolvemos el album_id

        # Si no se encuentra el álbum, buscamos el artista
        result = sp.search(q=artist, type='artist', limit=1)
        if result['artists']['items']:
            print(f"Artista encontrado: {artist} - {result['artists']['items'][0]['id']}")
            return result['artists']['items'][0]['id']  # Si encuentra un artista, devolvemos el artist_id

        # Si no se encuentra nada, devolvemos None
        print(f"No se encontró información para: {nominee} / {artist}")
        return np.nan  # Usar NaN en lugar de None para datos faltantes

    except Exception as e:
        print(f"Error retrieving data for nominee: {nominee}, artist: {artist}: {e}")
        return np.nan


# Función para añadir los Spotify IDs al DataFrame con seguimiento del progreso
def add_spotify_ids(df):
    total_rows = len(df)
    for i, (index, row) in enumerate(df.iterrows()):
        # Obtener el Spotify ID basado en el nominee o el artista
        spotify_id = get_spotify_id(row['nominee'], row['artist'])
        
        # Añadir el Spotify ID a la nueva columna 'spotify_id'
        df.at[index, 'spotify_id'] = spotify_id
        
        # Imprimir el progreso cada 100 filas
        if (i + 1) % 100 == 0:
            print(f"Progreso de obtención de Spotify IDs: {i + 1}/{total_rows} filas completadas")
        
        # Pausa corta para evitar posibles problemas de límites de la API
        time.sleep(5)
    
    return df


# Añadir columna 'spotify_id' vacía
df_clean['spotify_id'] = None

# Llamada para añadir los Spotify IDs con seguimiento del progreso
df_clean = add_spotify_ids(df_clean)


# Mostrar el número total de filas y columnas
num_filas, num_columnas = df_clean.shape
print(f"\nNúmero total de filas: {num_filas}")
print(f"Número total de columnas: {num_columnas}")

# Revisar la cantidad de valores nulos por columna
print("\nValores nulos por columna:")
print(df_clean.isnull().sum())

# Guardar el DataFrame limpio con Spotify IDs
try:
    df_clean.to_csv('data/processed/grammys_supercleaned.csv', index=False)
    print("Transformación completada y archivo guardado exitosamente.")
except Exception as e:
    print(f"Error durante la transformación: {e}")
