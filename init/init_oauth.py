from pydrive2.auth import GoogleAuth
import os

def init_oauth():
    creds_path = "../client_secret.json" 
    token_path = "../token.json" 

    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(creds_path)

    if os.path.exists(token_path):
        gauth.LoadCredentialsFile(token_path)

    if not gauth.credentials or gauth.access_token_expired:
        print("No se encontró token o expiró, iniciando autenticación...")
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(token_path) 
        print(f"Token creado en {token_path}")
    else:
        print("Token válido, autorizado.")
        gauth.Authorize()

if __name__ == "__main__":
    init_oauth()