import pandas as pd
from db_connection import establecer_conexion, cerrar_conexion

def extract_data():
    # Establece la conexión usando SQLAlchemy
    engine, session = establecer_conexion()  # Ahora engine es el primero que se recibe

    # Consulta SQL para extraer datos
    query = "SELECT * FROM grammy_awards"  # Asegúrate de que la tabla exista

    # Carga los datos en un DataFrame de pandas usando el engine de SQLAlchemy
    df = pd.read_sql(query, con=engine)

    # Cierra la sesión y la conexión
    cerrar_conexion(session)
    print("Datos cargados con éxito")
    print(df.head())  # Mostrar las primeras filas del DataFrame para verificar

# Puedes llamar a la función `extract_data()` si deseas ejecutar el script directamente
if __name__ == "__main__":
    extract_data()











