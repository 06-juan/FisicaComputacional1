import os
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow # <--- ESTA ES LA QUE FALTA
from googleapiclient.discovery import build

# Cargar las variables del cofre .env
load_dotenv()

# Ahora traemos los nombres de los archivos desde el .env
SCOPES          = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
TOKEN_FILE       = os.getenv('TOKEN_FILE')
DRIVE_FOLDER     = os.getenv('DRIVE_FOLDER_NAME')

def autenticar_drive():
    creds = None
    # El código ahora usa las variables que cargamos de .env
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(f"❌ No se encontró '{CREDENTIALS_FILE}'.")
            
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    print("✅ Autenticación Drive exitosa")
    return build('drive', 'v3', credentials=creds)

autenticar_drive()