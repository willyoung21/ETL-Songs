import pandas as pd
from db_connection import get_db_engine

# Obtener una conexión a la base de datos
engine = get_db_engine()

# Consultar datos y cargarlos en un DataFrame
query = "SELECT * FROM grammy_awards;"
df = pd.read_sql(query, engine)


# Configurar pandas para mostrar más filas y columnas
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

df.head(5)
