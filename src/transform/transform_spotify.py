import os
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../extract'))
from extract_spotify import load_spotify_data

def transform_spotify_data(df_spotify: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la transformación del DataFrame de Spotify.

    Args:
        df_spotify (pd.DataFrame): DataFrame original cargado desde el archivo CSV.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    # Eliminar columnas innecesarias
    columns_to_drop = [
        'explicit', 'key', 'loudness', 'mode', 'speechiness', 
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'time_signature'
    ]
    df_cleaned_spotify = df_spotify.drop(columns=columns_to_drop)

    # Eliminar filas con valores nulos en las columnas 'artists' y 'track_name'
    df_cleaned_spotify = df_cleaned_spotify.dropna(subset=['artists', 'track_name'])

    # Verificar que ya no haya nulos
    print("Conteo de valores nulos por columna después de la limpieza:")
    print(df_cleaned_spotify.isnull().sum())
    

    return df_cleaned_spotify

if __name__ == "__main__":
    try:
        # Definir la ruta del archivo CSV
        csv_path = os.path.join("data", "raw", "spotify_dataset.csv")

        # Cargar los datos de Spotify
        df_spotify = load_spotify_data(csv_path)

        # Transformar los datos
        df_cleaned_spotify = transform_spotify_data(df_spotify)

        # Guardar el DataFrame limpio en un nuevo archivo CSV
        processed_csv_path = os.path.join("data", "processed", "spotify_cleaned.csv")
        df_cleaned_spotify.to_csv(processed_csv_path, index=False)

        print("Transformación completada y archivo guardado exitosamente.")
    except Exception as e:
        print(f"Error durante la transformación: {e}")
