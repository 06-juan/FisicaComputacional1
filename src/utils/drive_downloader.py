import os
import io
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload
from src.utils.GoogleAutenticator import autenticar_drive

def descargar_desde_drive():
    # Cargamos configuración desde el entorno
    folder_name = os.getenv('DRIVE_FOLDER_NAME')
    bronze_path = Path('data/bronze')
    bronze_path.mkdir(parents=True, exist_ok=True)

    service = autenticar_drive()
    
    # 1. Buscar Carpeta
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, spaces='drive').execute()
    items = results.get('files', [])
    
    if not items:
        print(f"⚠️ No se encontró la carpeta {folder_name} en Drive.")
        return
    
    folder_id = items[0]['id']

    # 2. Listar y Descargar CSVs
    query_csv = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    csv_results = service.files().list(q=query_csv, spaces='drive', fields='files(id, name)').execute()
    csvs = csv_results.get('files', [])

    print(f"📥 Sincronizando {len(csvs)} archivos desde Drive...")

    for f in csvs:
        destino = bronze_path / f['name']
        if destino.exists():
            continue
        
        print(f"  ⬇️ Descargando {f['name']}...")
        request = service.files().get_media(fileId=f['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        with open(destino, 'wb') as out:
            out.write(fh.getbuffer())