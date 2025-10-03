import os
import pandas as pd
from sqlalchemy import create_engine
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

def load_to_postgres(df_dict: Dict[str, pd.DataFrame],
                     mysql_conn_str: str = "postgresql+psycopg2://airflow:airflow@postgres_db:5432/grammys_dw",
                     if_exists: str = "replace") -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    
    try:
        engine = create_engine(mysql_conn_str)
    except Exception as e:
        raise RuntimeError(f"No se pudo crear la conexión a Postgres: {e}")

    for table_name, df in df_dict.items():
        try:
            df.to_sql(name=table_name, con=engine, index=False, if_exists=if_exists)
            results[table_name] = {"status": "ok", "rows": df.shape[0]}
            print(f"Tabla '{table_name}' cargada correctamente: {df.shape[0]} filas")
        except Exception as e:
            results[table_name] = {"status": "error", "message": str(e)}
            print(f"Error cargando tabla '{table_name}': {e}")

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