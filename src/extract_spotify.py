def read_csvs():
    import os
    import pandas as pd
    from airflow.models import Variable

    def read_csv1(csv_path: str) -> pd.DataFrame:
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

    def read_csv_task(**kwargs):
        """
        Tarea que lee el CSV y lo carga en un DataFrame de pandas.

        Args:
            **kwargs: Parámetros pasados por Airflow.
        """
        # Obtener la ruta del archivo CSV desde las variables de Airflow
        csv_path = Variable.get("spotify_csv_path")  # Asegúrate de definir esta variable en Airflow
        df_spotify = read_csv1(csv_path)
        
        # Mostrar las primeras 5 filas del DataFrame para verificar
        print(df_spotify.head(5))  

    # Si estás ejecutando esto como un script independiente, puedes incluir una sección como esta:
    if __name__ == "__main__":
        # Definir la ruta del archivo CSV localmente
        csv_path = os.path.join("data", "raw", "spotify_dataset.csv")
        df_spotify = read_csv1(csv_path)
        print(df_spotify.head(5))

if __name__ == "__main__":
    read_csvs()
