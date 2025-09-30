from airflow.decorators import dag, task
from datetime import datetime
import sys
import os
import pandas as pd

sys.path.append("/opt/airflow")

from scripts.extract import extract_spotify, extract_grammy
from scripts.transform import transform_spotify, transform_grammy

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

    # --- Spotify ---
    @task()
    def extract_spotify_task():
        df = extract_spotify()
        output_path = "/opt/airflow/processed_data/spotify_raw.csv"
        df.to_csv(output_path, index=False)
        print(f"Spotify CSV guardado en {output_path}")
        return output_path

    @task()
    def transform_spotify_task(csv_path: str):
        df = pd.read_csv(csv_path)
        df_clean = transform_spotify(df)
        output_path = "/opt/airflow/processed_data/spotify_clean.csv"
        df_clean.to_csv(output_path, index=False)
        print(f"Spotify limpio guardado en {output_path}")

    # --- Grammy ---
    @task()
    def extract_grammy_task():
        df = extract_grammy()
        return df.to_dict(orient="list")

    @task()
    def transform_grammy_task(raw_df: dict):
        df = pd.DataFrame(raw_df)
        df_clean = transform_grammy(df)
        output_path = os.path.join(OUTPUT_DIR, "grammy_clean.csv")
        df_clean.to_csv(output_path, index=False)
        print(f"[ETL] Grammy limpio guardado en {output_path}")

    raw_spotify = extract_spotify_task()
    transform_spotify_task(raw_spotify)

    raw_grammy = extract_grammy_task()
    transform_grammy_task(raw_grammy)


etl_spotify_grammy()