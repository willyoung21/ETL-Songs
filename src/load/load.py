import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener los valores de las variables de entorno
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

def load_data_to_db(df, table_name: str):
    """
    Carga un DataFrame a una tabla en PostgreSQL.

    Args:
        df (pd.DataFrame): DataFrame a cargar.
        table_name (str): Nombre de la tabla en la base de datos.
    """
    try:
        # Crear la cadena de conexión a la base de datos
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        # Cargar los datos a la tabla en la base de datos (si no existe la tabla, se crea)
        df.to_sql(table_name, engine, index=False, if_exists='replace')
        
        print(f"Datos cargados exitosamente en la tabla '{table_name}'.")
    except Exception as e:
        print(f"Error al cargar los datos en la base de datos: {e}")

if __name__ == "__main__":
    # Definir la ruta del archivo CSV fusionado
    merged_csv_path = os.path.join("data", "processed", "final_merged_spotify_grammys.csv")
    
    # Cargar el CSV en un DataFrame
    df_merged_cleaned = pd.read_csv(merged_csv_path)
    
    # Cargar el DataFrame a la base de datos
    load_data_to_db(df_merged_cleaned, table_name='spotify_grammys')
