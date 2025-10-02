from airflow.decorators import dag, task
from datetime import datetime
import sys
import os
import pandas as pd

sys.path.append("/opt/airflow")

from scripts.extract import extract_spotify, extract_grammy
from scripts.transform import transform_spotify, transform_grammy
from scripts.merge import merge_dw

OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@dag(
    dag_id="etl_spotify_grammy",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "spotify", "grammy"]
)
def etl_spotify_grammy():

    @task()
    def extract_spotify_task():
        df = extract_spotify()
        return df  

    @task()
    def transform_spotify_task(df: pd.DataFrame):
        dims_dict = transform_spotify(df)
        return dims_dict 

    @task()
    def extract_grammy_task():
        df = extract_grammy()
        return df  

    @task()
    def transform_grammy_task(df: pd.DataFrame):
        dims_dict = transform_grammy(df)
        return dims_dict 

    @task()
    def merge_task(spotify_dims: dict, grammy_dims: dict):
        dw = merge_dw(spotify_dims, grammy_dims)
        for name, df_dw in dw.items():
            output_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df_dw.to_csv(output_path, index=False)
            print(f"Data Warehouse '{name}' guardado en {output_path}")
        return list(dw.keys())

    raw_spotify = extract_spotify_task()
    spotify_dims = transform_spotify_task(raw_spotify)

    raw_grammy = extract_grammy_task()
    grammy_dims = transform_grammy_task(raw_grammy)

    dw_tables = merge_task(spotify_dims, grammy_dims)

etl_spotify_grammy()