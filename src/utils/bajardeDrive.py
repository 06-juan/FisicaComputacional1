# utils/bajardeDrive.py
import io
import os
import zipfile
import pickle
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def obtener_servicio_drive():
    creds = None
    # El archivo token.pickle almacena las credenciales de acceso del usuario
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Requiere el archivo credentials.json en la raíz del proyecto
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def descargar_carpeta_drive(nombre_carpeta_drive, destino_local):
    """
    Busca una carpeta en Drive, descarga su contenido y extrae ZIPs si existen.
    """
    service = obtener_servicio_drive()
    os.makedirs(destino_local, exist_ok=True)

    # 1. Buscar el ID de la carpeta
    query = f"name = '{nombre_carpeta_drive}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get('files', [])

    if not folders:
        print(f"⚠️ No se encontró la carpeta '{nombre_carpeta_drive}' en Google Drive.")
        return

    folder_id = folders[0]['id']

    # 2. Listar archivos dentro
    query_files = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query_files, fields="files(id, name)").execute()
    files = results.get('files', [])

    print(f"📥 Encontrados {len(files)} archivos en Drive. Iniciando descarga...")

    for f in files:
        f_name = f['name']
        f_id = f['id']
        path_final = os.path.join(destino_local, f_name)

        # Evitar re-descargar si el CSV ya está ahí (o si el ZIP ya fue extraído)
        if os.path.exists(path_final) or os.path.exists(path_final.replace('.zip', '.csv')):
            print(f"⏩ Saltando {f_name} (ya existe).")
            continue

        print(f"⏬ Descargando {f_name}...")
        request = service.files().get_media(fileId=f_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()

        # 3. Lógica de extracción "Anti-Zip"
        fh.seek(0)
        if f_name.endswith('.zip'):
            with zipfile.ZipFile(fh) as z:
                z.extractall(destino_local)
            print(f"📦 {f_name} extraído correctamente.")
        else:
            with open(path_final, 'wb') as local_file:
                local_file.write(fh.read())