# src/invoice_pipeline/orchestrator.py

import json
import logging
import re
from datetime import datetime, UTC
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# --- Nuestros bloques de Lego (ahora incluimos TextractService) ---
from .parser import ParserService, BusinessException
from .google_drive_downloader import GoogleDriveDownloader
from .athena_connector import AthenaConnector
from .s3_uploader import S3Uploader
from .config import (
    BASE_DIR,
    AWS_REGION,
    GDRIVE_SECRET_NAME,
    AWS_BUCKET_NAME
)

# --- Constantes y Configuración del Pipeline ---
ATHENA_DATABASE = "automations_finanzas_db"
ATHENA_TABLE_SUCCESS = "f001_mastercard_invoices"
INVOICE_FILENAME_PATTERN = re.compile(r"^MCI_Invoice_.*\.pdf$", re.IGNORECASE)

# --- Función Auxiliar para obtener credenciales ---
def get_secret(secret_name: str, region_name: str) -> str | None:
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    logging.info(f"Intentando obtener el secreto: {secret_name}")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        logging.info("Secreto obtenido exitosamente.")
        return secret
    except ClientError as e:
        logging.error(f"No se pudo recuperar el secreto '{secret_name}': {e}")
        return None

# --- Pipeline Principal ---
def run_pipeline(base_dir: str = BASE_DIR):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("=======================================")
    logging.info("======= INICIANDO PIPELINE DE FACTURAS (V2 con Textract) =======")
    logging.info("=======================================")

    # 1. Inicialización de Conectores
    gdrive_credentials_json = get_secret(GDRIVE_SECRET_NAME, AWS_REGION)
    if not gdrive_credentials_json:
        logging.critical("FINALIZANDO: No se pudieron obtener las credenciales de Google Drive.")
        return

    downloader = GoogleDriveDownloader(gdrive_credentials_json, f"{base_dir}/downloads")
    parser = ParserService(base_dir)
    athena = AthenaConnector(ATHENA_DATABASE, AWS_BUCKET_NAME, AWS_REGION)
    s3_uploader = S3Uploader(AWS_REGION)

    # 2. Descubrimiento de Archivos Nuevos
    try:
        logging.info("--- Fase 1: Descubrimiento de Archivos ---")
        processed_ids = athena.get_processed_file_ids(ATHENA_TABLE_SUCCESS)
        main_folder_id = downloader.get_folder_id("Invoice Mastercard")
        current_year = str(datetime.now().year)
        year_folder_id = downloader.get_folder_id(current_year, parent_folder_id=main_folder_id)

        all_files = []
        if year_folder_id:
            all_files = downloader.list_all_files_recursively(start_folder_id=year_folder_id)
        else:
            logging.warning(f"No se encontró carpeta para el año {current_year}.")

        files_to_process = [
            f for f in all_files
            if INVOICE_FILENAME_PATTERN.match(f['name']) and f['id'] not in processed_ids
        ]

        if not files_to_process:
            logging.info("No se encontraron facturas nuevas para procesar. Proceso finalizado.")
            return

        logging.info(f"Se encontraron {len(files_to_process)} facturas nuevas para procesar.")
    except Exception as e:
        logging.critical(f"FINALIZANDO: Falló la fase de descubrimiento. Error: {e}", exc_info=True)
        return

    # 3. Procesamiento de Cada Archivo
    logging.info("--- Fase 2: Procesamiento de Archivos ---")
    summary = {"processed_successfully": [], "failed_to_process": []}
    
    final_jsons_dir = Path(base_dir) / "final_jsons"
    final_jsons_dir.mkdir(exist_ok=True)

    for file_info in files_to_process:
        file_id = file_info['id']
        file_name = file_info['name']
        logging.info(f"--> Procesando: {file_name} (ID: {file_id})")
        
        local_pdf_path = None
        # REEMPLAZA el bloque try/except dentro del bucle for en orchestrator.py
        try:
            # 3.1. Descargar el PDF localmente
            local_pdf_path = downloader.download_file(file_id, file_name)
            if not local_pdf_path:
                raise Exception("La descarga desde Google Drive falló.")

            # 3.2. Llamar al Parser directamente con la ruta del PDF
            # Ya no se sube a S3 ni se llama a Textract en este paso.
            parsed_rows = parser.run(local_pdf_path) # <-- Llamada simplificada
            if not parsed_rows:
                raise BusinessException("PARSER_EMPTY_RESULT", "El parser no devolvió ninguna fila.")

            # 3.3. Enriquecer y Guardar (Esta parte queda igual)
            timestamp = datetime.now(UTC).isoformat()
            for row in parsed_rows:
                row.update({'file_id': file_id, 'file_name': file_name, 'processing_timestamp': timestamp})

            final_json_data = json.dumps(parsed_rows, indent=2, ensure_ascii=False)

            local_json_path = final_jsons_dir / f"{Path(file_name).stem}.json"
            local_json_path.write_text(final_json_data, encoding='utf-8')

            date_obj = datetime.fromisoformat(parsed_rows[0]['billing_cycle_date'])
            s3_json_key = (f"invoices/mastercard/year={date_obj.year}/month={date_obj.month:02d}/"
                        f"{Path(file_name).stem}.json")

            success = s3_uploader.upload_json_to_s3(final_json_data, AWS_BUCKET_NAME, s3_json_key)
            if not success:
                raise Exception("La subida del archivo JSON a S3 falló.")

            logging.info(f"<-- ÉXITO: {file_name} procesado y subido a S3.")
            summary["processed_successfully"].append(file_name)

        except Exception as e:
            logging.error(f"<-- ERROR al procesar {file_name}: {e}", exc_info=True)
            summary["failed_to_process"].append(file_name)

            
    # 4. Resumen Final
    logging.info("=======================================")
    logging.info("======= PIPELINE FINALIZADO =======")
    logging.info(f"Procesados con éxito: {len(summary['processed_successfully'])}")
    logging.info(f"Fallaron: {len(summary['failed_to_process'])}")
    logging.info("=======================================")
    
if __name__ == "__main__":
    run_pipeline()