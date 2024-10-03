import pandas as pd
from db_connection import get_db_engine

def load_grammy_data() -> pd.DataFrame:
    """
    Carga los datos de los premios Grammy desde la base de datos en un DataFrame de pandas.

    Returns:
        pd.DataFrame: DataFrame con los datos de los Grammy.
    """
    # Obtener una conexión a la base de datos
    engine = get_db_engine()

    # Definir la consulta SQL
    query = "SELECT * FROM grammy_awards;"

    # Ejecutar la consulta y cargar los datos en un DataFrame
    df = pd.read_sql(query, engine)
    
    return df

if __name__ == "__main__":
    try:
        # Cargar los datos de los Grammy
        df_grammy = load_grammy_data()

        # Configurar pandas para mostrar más filas y columnas si es necesario (opcional)
        pd.set_option('display.max_rows', 100)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        # Mostrar las primeras 5 filas del DataFrame para verificar
        print(df_grammy.head(5))
    
    except Exception as e:
        print(f"Error al cargar los datos: {e}")
