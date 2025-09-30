# scripts/extract.py
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

def extract_spotify(csv_path="/opt/airflow/raw_data/spotify_dataset.csv") -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
        print(f"Spotify CSV cargado correctamente: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"Error al leer Spotify CSV: {e}")
        raise

def extract_grammy(mysql_conn_str="mysql+pymysql://root:root@host.docker.internal:3306/grammys_db",
                   table_name="grammy_awards") -> pd.DataFrame:
    try:
        engine = create_engine(mysql_conn_str)
        df = pd.read_sql_table(table_name, con=engine)
        print(f"Tabla Grammys cargada correctamente: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"Error al extraer Grammys desde MySQL: {e}")
        raise