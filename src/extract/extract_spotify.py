import os
import pandas as pd

def load_spotify_data(csv_path: str) -> pd.DataFrame:
    """
    Carga los datos desde un archivo CSV de Spotify en un DataFrame de pandas.

    Args:
        csv_path (str): Ruta del archivo CSV.

    Returns:
        pd.DataFrame: DataFrame con los datos cargados.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"El archivo {csv_path} no existe.")

    df_spotify = pd.read_csv(csv_path)
    
    # Opciones para mostrar más filas y columnas si es necesario (opcional)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    return df_spotify

if __name__ == "__main__":
    # Definir la ruta del archivo CSV
    csv_path = os.path.join("data", "raw", "spotify_dataset.csv")
    
    # Cargar los datos de Spotify
    df_spotify = load_spotify_data(csv_path)
    
    # Mostrar las primeras 5 filas del DataFrame para verificar
    print(df_spotify.head(5))
