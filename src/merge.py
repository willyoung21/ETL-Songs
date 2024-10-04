def merge():
    import os
    from dotenv import load_dotenv
    import pandas as pd
    import re
    from rapidfuzz import process, fuzz
    import requests
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    import time

    # Cargar las variables de entorno desde el archivo .env
    load_dotenv()

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                                                    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
                                                    scope="user-library-read"))

    access_token = sp.auth_manager.get_access_token(as_dict=False)

    # Ruta del archivo de progreso
    progress_file = 'data/processed/progress.csv'

    def save_to_csv(df, filename):
        df.to_csv(filename, index=False)

    def normalize_text(text):
        if isinstance(text, str):
            text = text.lower()
            text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
            text = re.sub(r'\s+', ' ', text).strip()
            text = re.sub(r'\b(feat|ft|featuring|remix|version)\b', '', text)
        return text

    def merge_datasets(grammy_merge, spotify_merge):
        # Normalizar los nombres de los artistas, canciones y álbumes en ambos datasets
        grammy_merge['artist_clean'] = grammy_merge['artist'].apply(normalize_text)
        grammy_merge['nominee_clean'] = grammy_merge['nominee'].apply(normalize_text)
        spotify_merge['artists_clean'] = spotify_merge['artists'].apply(lambda x: normalize_text(', '.join(x)) if isinstance(x, list) else normalize_text(x))
        spotify_merge['track_name_clean'] = spotify_merge['track_name'].apply(normalize_text)
        spotify_merge['album_name_clean'] = spotify_merge['album_name'].apply(normalize_text)

        # Merge directo basado en 'spotify_id' y 'track_id'
        merged_df = pd.merge(grammy_merge, spotify_merge, left_on='spotify_id', right_on='track_id', how='left')

        # Filas sin match en el merge basado en spotify_id
        no_match_df = merged_df[merged_df['track_id'].isnull()]

        # Si hay filas sin match, aplicar coincidencia difusa
        song_threshold = 70
        artist_threshold = 70
        album_threshold = 60

        matches = []
        for i, row in no_match_df.iterrows():
            song_match = process.extractOne(row['nominee_clean'], spotify_merge['track_name_clean'], scorer=fuzz.ratio)

            if song_match and song_match[1] >= song_threshold:
                matched_rows = spotify_merge[spotify_merge['track_name_clean'] == song_match[0]]
                artist_matches = []

                for _, matched_row in matched_rows.iterrows():
                    artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                    if artist_match and artist_match[1] >= artist_threshold:
                        artist_matches.append(matched_row)

                if artist_matches:
                    matches.extend(artist_matches)
                else:
                    matches.append(row)
            else:
                # Buscar coincidencia con album_name
                album_match = process.extractOne(row['nominee_clean'], spotify_merge['album_name_clean'], scorer=fuzz.ratio)
                if album_match and album_match[1] >= album_threshold:
                    matched_rows = spotify_merge[spotify_merge['album_name_clean'] == album_match[0]]
                    artist_matches = []

                    for _, matched_row in matched_rows.iterrows():
                        artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                        if artist_match and artist_match[1] >= artist_threshold:
                            artist_matches.append(matched_row)

                    if artist_matches:
                        matches.extend(artist_matches)
                    else:
                        matches.append(row)
                else:
                    matches.append(row)

            # Mostrar progreso cada 100 filas y guardar progreso
            if (i + 1) % 100 == 0:
                print(f"Procesadas {i + 1} filas de {len(no_match_df)}")
                # Guardar el progreso en un archivo
                save_to_csv(pd.DataFrame(matches), progress_file)

        # Crear DataFrame con coincidencias difusas
        fuzzy_matched_df = pd.DataFrame(matches)

        # Concatenar el merge directo y las coincidencias difusas
        final_merged_df = pd.concat([merged_df.reset_index(drop=True), fuzzy_matched_df.reset_index(drop=True)], ignore_index=True)

        # Eliminar columnas auxiliares
        final_merged_df = final_merged_df.drop(columns=['artist_clean', 'nominee_clean', 'artists_clean', 'track_name_clean', 'album_name_clean'], errors='ignore')

        # Eliminar duplicados
        final_merged_df = final_merged_df.drop_duplicates(subset=['nominee', 'artist', 'category'])

        print(f"Cantidad de filas después del merge: {final_merged_df.shape[0]}")
        save_to_csv(final_merged_df, 'data/processed/merged_data.csv')  # Guardar el DataFrame final
        return final_merged_df

    def get_spotify_info(spotify_id, access_token):
        url = f"https://api.spotify.com/v1/tracks/{spotify_id}"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        while True:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 401:
                    print("Token expirado. Renovando token...")
                    access_token = sp.auth_manager.get_access_token(as_dict=False)
                    continue
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    print(f"Demasiadas solicitudes. Esperando {retry_after} segundos...")
                    time.sleep(retry_after)
                else:
                    response.raise_for_status()
                    return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Error al obtener información del track {spotify_id}: {e}")
                return None

    def get_audio_features(spotify_id, access_token):
        url = f"https://api.spotify.com/v1/audio-features/{spotify_id}"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        while True:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 401:
                    print("Token expirado. Renovando token...")
                    access_token = sp.auth_manager.get_access_token(as_dict=False)
                    continue
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    print(f"Demasiadas solicitudes. Esperando {retry_after} segundos...")
                    time.sleep(retry_after)
                else:
                    response.raise_for_status()
                    return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Error al obtener características de audio para {spotify_id}: {e}")
                return None

    def fill_missing_data(final_merged_df, access_token):
        for index, row in final_merged_df[final_merged_df['track_id'].isnull()].iterrows():
            spotify_id = row['spotify_id']
            
            if pd.notna(spotify_id):
                try:
                    track_info = get_spotify_info(spotify_id, access_token)
                    if track_info:
                        final_merged_df.loc[index, 'track_id'] = track_info.get('id')
                        final_merged_df.loc[index, 'artists'] = ', '.join([artist['name'] for artist in track_info.get('artists', [])])
                        final_merged_df.loc[index, 'album_name'] = track_info.get('album', {}).get('name')
                        final_merged_df.loc[index, 'track_name'] = track_info.get('name')
                        final_merged_df.loc[index, 'popularity'] = track_info.get('popularity')
                        final_merged_df.loc[index, 'duration_ms'] = track_info.get('duration_ms')

                        audio_features = get_audio_features(spotify_id, access_token)
                        if audio_features:
                            final_merged_df.loc[index, 'danceability'] = audio_features.get('danceability')
                            final_merged_df.loc[index, 'energy'] = audio_features.get('energy')
                            final_merged_df.loc[index, 'tempo'] = audio_features.get('tempo')

                    if index % 100 == 0:
                        print(f"Información rellenada en fila {index}")

                    time.sleep(5)

                except Exception as e:
                    print(f"Error al procesar fila {index}: {e}")

        return final_merged_df

    def main():
        # Cargar los archivos CSV
        spotify_csv_path = os.path.join('data', 'processed', 'spotify.csv')
        grammy_csv_path = os.path.join('data', 'processed', 'grammys.csv')

        spotify_merge = pd.read_csv(spotify_csv_path)
        grammy_merge = pd.read_csv(grammy_csv_path)

        # Comprobar si existe un archivo de progreso y cargarlo
        if os.path.exists(progress_file):
            print("Cargando progreso desde el archivo...")
            merged_df = pd.read_csv(progress_file)
            print(f"Progreso cargado: {merged_df.shape[0]} filas.")
        else:
            merged_df = merge_datasets(grammy_merge, spotify_merge)

        # Llenar los datos faltantes
        final_df = fill_missing_data(merged_df, access_token)
        save_to_csv(final_df, 'data/processed/final_data.csv')  # Guardar el DataFrame final en un CSV

    if __name__ == "__main__":
        main()
        
if __name__ == "__main__":
    merge()



