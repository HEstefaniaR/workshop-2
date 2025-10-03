import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def load_to_Drive(csv_paths: List[str], replace: bool = False) -> Dict[str, Dict[str, Any]]:
    drive = _get_drive()
    results: Dict[str, Dict[str, Any]] = {}

    folder_name = "data_backup_warehouse"
    folder_id = _get_or_create_drive_folder(drive, folder_name)

    for path in csv_paths:
        if not os.path.exists(path):
            results[path] = {"status": "error", "message": "file_not_found"}
            continue

        title = os.path.basename(path)

        if replace:
            query = f"title = '{title}' and trashed = false and '{folder_id}' in parents"
            existing = drive.ListFile({"q": query, "supportsAllDrives": True}).GetList()
            for f in existing:
                f.Delete()

        metadata = {"title": title, "parents": [{"id": folder_id}]}

        f = drive.CreateFile(metadata)
        f.SetContentFile(path)
        try:
            f.Upload(param={"supportsAllDrives": True})
            results[path] = {"status": "ok", "id": f.get("id"), "title": title}
            print(f"Subido: {path} -> ID {f.get('id')}")
        except Exception as e:
            results[path] = {"status": "error", "message": str(e)}
            print(f"Error subiendo {path}: {e}")

    return results

def create_database_if_not_exists_mysql(user: str, password: str, host: str, port: int, db_name: str):
    conn_str_server = f"mysql+pymysql://{user}:{password}@{host}:{port}/"
    engine_server = create_engine(conn_str_server)

    with engine_server.connect() as conn:
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        print(f"Base de datos '{db_name}' verificada/creada.")

def load_to_mysql(df_dict: Dict[str, pd.DataFrame],
                  user: str = "root",
                  password: str = "root",
                  host: str = "localhost",
                  port: int = 3306,
                  db_name: str = "mydb") -> Dict[str, Dict[str, Any]]:

    create_database_if_not_exists_mysql(user, password, host, port, db_name)
    results: Dict[str, Dict[str, Any]] = {}
    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_str)
    metadata = MetaData()

    table_order = [
        "track_dim", "artist_dim", "genre_dim", "grammy_event_dim",
        "artist_dim_has_track_dim", "genre_dim_has_track_dim", "award_fact"
    ]

    try:
        with engine.connect() as conn:
            conn.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for table_name in table_order[::-1]:
                conn.execute(f"DROP TABLE IF EXISTS {table_name};")
                print(f"Tabla '{table_name}' eliminada (si existía)")
            conn.execute("SET FOREIGN_KEY_CHECKS = 1;")

        for table_name in table_order:
            df = df_dict.get(table_name)
            if df is None:
                continue

            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].astype(str).str.slice(0, 255)

            df = df.replace(['nan', '', pd.NA], None)

            if table_name != "award_fact":
                df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                results[table_name] = {"status": "ok", "rows": df.shape[0]}
                print(f"Tabla '{table_name}' cargada correctamente: {df.shape[0]} filas")
            else:
                valid_track = df[df['track_dim_track_id'].notnull()]
                null_track = df[df['track_dim_track_id'].isnull()]

                valid_artist = valid_track[valid_track['artist_dim_artist_id'].notnull()]
                null_artist = valid_track[valid_track['artist_dim_artist_id'].isnull()]

                if not valid_artist.empty:
                    valid_artist.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                if not null_artist.empty:
                    null_artist.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                if not null_track.empty:
                    null_track.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                total_rows = len(valid_artist) + len(null_artist) + len(null_track)
                results[table_name] = {"status": "ok", "rows": total_rows}
                print(f"Tabla '{table_name}' cargada correctamente: {total_rows} filas")

    except SQLAlchemyError as e:
        print(f"Error cargando tablas: {e}")
        for table_name in df_dict.keys():
            results.setdefault(table_name, {"status": "error", "message": str(e)})

    return results


def _get_or_create_drive_folder(drive: GoogleDrive, folder_name: str) -> str:
    query = f"title = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    existing = drive.ListFile({"q": query, "supportsAllDrives": True}).GetList()
    if existing:
        return existing[0]["id"]

    folder_metadata = {
        "title": folder_name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    folder = drive.CreateFile(folder_metadata)
    folder.Upload(param={"supportsAllDrives": True})
    return folder["id"]


def _get_drive():
    creds_path = "/opt/airflow/client_secret.json"
    token_path = "/opt/airflow/token.json"

    if not os.path.exists(creds_path):
        raise RuntimeError("No se encontró el archivo client_secret.json")

    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(creds_path)

    if os.path.exists(token_path):
        gauth.LoadCredentialsFile(token_path)

    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile(token_path)

    return GoogleDrive(gauth)