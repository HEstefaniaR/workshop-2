from airflow.decorators import dag, task
from datetime import datetime
import sys
import os
import pandas as pd

sys.path.append("/opt/airflow")

from scripts.extract import extract_spotify, extract_grammy
from scripts.transform import transform_spotify, transform_grammy
from scripts.merge import merge_dw
from scripts.load import load_to_Drive, load_to_postgres

OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


@dag(
    dag_id="etl_spotify_grammy",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "spotify", "grammy"],
)
def etl_spotify_grammy():

    @task()
    def extract_spotify_task():
        return extract_spotify()

    @task()
    def transform_spotify_task(df: pd.DataFrame):
        return transform_spotify(df)

    @task()
    def extract_grammy_task():
        return extract_grammy()

    @task()
    def transform_grammy_task(df: pd.DataFrame):
        return transform_grammy(df)

    @task()
    def merge_task(spotify_dims: dict, grammy_dims: dict):
        dw = merge_dw(spotify_dims, grammy_dims)
        csv_paths = []

        for name, df_dw in dw.items():
            output_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df_dw.to_csv(output_path, index=False)
            csv_paths.append(output_path)
            print(f"Data Warehouse '{name}' guardado en {output_path}")
        dw_dict = {}
        for path in csv_paths:
            table_name = os.path.splitext(os.path.basename(path))[0]
            dw_dict[table_name] = pd.read_csv(path)
        return csv_paths

    @task()
    def load_task(csv_paths: list[str]):
        results_drive = load_to_Drive(csv_paths, replace=True)
        print("Archivos cargados a Drive:", results_drive)

        dw_dict = {}
        for path in csv_paths:
            table_name = os.path.splitext(os.path.basename(path))[0]
            dw_dict[table_name] = pd.read_csv(path)

        conn_str = "postgresql+psycopg2://airflow:airflow@postgres_airflow:5432/airflow"
        results_postgres = load_to_postgres(dw_dict, mysql_conn_str=conn_str)
        print("Tablas cargadas a Postgres:", results_postgres)

        return {"drive": results_drive, "postgres": results_postgres}
    raw_spotify = extract_spotify_task()
    spotify_dims = transform_spotify_task(raw_spotify)

    raw_grammy = extract_grammy_task()
    grammy_dims = transform_grammy_task(raw_grammy)

    csv_files = merge_task(spotify_dims, grammy_dims)
    load_task(csv_files) 


etl_spotify_grammy()