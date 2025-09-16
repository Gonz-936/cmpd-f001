# src/invoice_pipeline/google_drive_downloader.py
import os
import io
import json
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

class GoogleDriveDownloader:
    def __init__(self, credentials_json: str, local_dir: str):
        self.local_dir = local_dir
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        creds_dict = json.loads(credentials_json)
        self.credentials = service_account.Credentials.from_service_account_info(creds_dict)
        self.service = build('drive', 'v3', credentials=self.credentials)
        logging.info("Servicio de Google Drive inicializado correctamente.")

    def get_folder_id(self, folder_name: str, parent_folder_id: str | None = None) -> str | None:
        """
        Busca el ID de una carpeta por su nombre. Opcionalmente, busca dentro de otra carpeta.
        """
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
            
        try:
            response = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            folders = response.get('files', [])
            if folders:
                folder_id = folders[0].get('id')
                logging.info(f"Se encontró la carpeta '{folder_name}' con ID: {folder_id}")
                return folder_id
            else:
                logging.warning(f"No se encontró la carpeta con el nombre '{folder_name}'.")
                return None
        except HttpError as error:
            logging.error(f"Ocurrió un error al buscar la carpeta: {error}")
            return None
    def list_all_files_recursively(self, start_folder_id: str) -> list[dict]:
        """
        Lista todos los archivos encontrados dentro de una carpeta y todas sus subcarpetas.
        """
        if not start_folder_id:
            return []
            
        all_files = []
        
        # Usamos una pila (stack) para no exceder los límites de recursión de Python
        folders_to_scan = [start_folder_id]

        while folders_to_scan:
            current_folder_id = folders_to_scan.pop()
            logging.info(f"Escaneando carpeta con ID: {current_folder_id}")
            
            try:
                # Paginación para manejar carpetas con muchos archivos/subcarpetas
                page_token = None
                while True:
                    response = self.service.files().list(
                        q=f"'{current_folder_id}' in parents",
                        spaces='drive',
                        fields='nextPageToken, files(id, name, mimeType)',
                        pageToken=page_token
                    ).execute()
                    
                    items = response.get('files', [])
                    for item in items:
                        if item.get('mimeType') == 'application/vnd.google-apps.folder':
                            # Si es una carpeta, la añadimos a la lista para escanearla después
                            folders_to_scan.append(item.get('id'))
                        else:
                            # Si es un archivo, lo añadimos a nuestra lista de resultados
                            all_files.append({'id': item.get('id'), 'name': item.get('name')})
                    
                    page_token = response.get('nextPageToken', None)
                    if page_token is None:
                        break # Salimos del bucle si no hay más páginas de resultados

            except HttpError as error:
                logging.error(f"Ocurrió un error al listar contenido de la carpeta {current_folder_id}: {error}")
                continue # Continuamos con la siguiente carpeta si una falla

        logging.info(f"Búsqueda recursiva completada. Se encontraron {len(all_files)} archivos en total.")
        return all_files
    def download_file(self, file_id: str, file_name: str) -> str | None:
        """Descarga un archivo de Google Drive por su ID."""
        local_path = os.path.join(self.local_dir, file_name)
        logging.info(f"Iniciando descarga del archivo de Drive ID: {file_id} en {local_path}")
        
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.FileIO(local_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.info(f"Descargando {file_name}: {int(status.progress() * 100)}%.")

            logging.info(f"Archivo descargado exitosamente en: {local_path}")
            return local_path
        except Exception as e:
            logging.error(f"Error al descargar el archivo de Drive {file_id}: {e}")
            return None