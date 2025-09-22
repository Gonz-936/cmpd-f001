# src/invoice_pipeline/orchestrator.py
import json
import logging
import re
from datetime import datetime, UTC
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# --- ¡Importando desde nuestra nueva librería! ---
from py_toolbox.aws.athena_connector import AthenaConnector
from py_toolbox.aws.s3_uploader import S3Uploader
from py_toolbox.google.google_drive_downloader import GoogleDriveDownloader
from py_toolbox.processing.tika_parser import TikaParser
from py_toolbox.utils.file_handler import FileHandler

# --- Bloques específicos de esta aplicación ---
from .parser import ParserService, BusinessException
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

# --- Función Auxiliar para obtener credenciales (sin cambios) ---
def get_secret(secret_name: str, region_name: str) -> str | None:
    # ... (sin cambios)
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

# --- Pipeline Principal (Refactorizado) ---
def run_pipeline(base_dir: str = BASE_DIR):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("=======================================")
    logging.info("======= INICIANDO PIPELINE DE FACTURAS =======")
    logging.info("=======================================")

    # 1. Definición de directorios de trabajo
    downloads_dir = f"{base_dir}/downloads"
    html_output_dir = f"{base_dir}/html_output"
    final_jsons_dir = f"{base_dir}/final_jsons"
    FileHandler.ensure_dirs(downloads_dir, html_output_dir, final_jsons_dir)

    # 2. Inicialización de Conectores
    gdrive_credentials_json = get_secret(GDRIVE_SECRET_NAME, AWS_REGION)
    if not gdrive_credentials_json:
        logging.critical("FINALIZANDO: No se pudieron obtener las credenciales de Google Drive.")
        return

    downloader = GoogleDriveDownloader(gdrive_credentials_json, downloads_dir)
    parser = ParserService() # Ahora no necesita base_dir
    s3_uploader = S3Uploader(AWS_REGION)
    athena = AthenaConnector(ATHENA_DATABASE, AWS_BUCKET_NAME, AWS_REGION)

    # 3. Descubrimiento de Archivos y Duplicados
    try:
        logging.info("--- Fase 1: Descubrimiento de Archivos y Duplicados ---")
        processed_file_ids = athena.get_processed_file_ids(ATHENA_TABLE_SUCCESS)
        processed_invoice_numbers = athena.get_processed_invoice_numbers(ATHENA_TABLE_SUCCESS)

        main_folder_id = downloader.get_folder_id("Invoice Mastercard")
        current_year = str(datetime.now().year)
        year_folder_id = downloader.get_folder_id(current_year, parent_folder_id=main_folder_id)

        all_files = downloader.list_all_files_recursively(start_folder_id=year_folder_id) if year_folder_id else []
        if not all_files:
             logging.warning(f"No se encontró carpeta para el año {current_year} o no contiene archivos.")

        files_to_process = [
            f for f in all_files
            if INVOICE_FILENAME_PATTERN.match(f['name']) and f['id'] not in processed_file_ids
        ]

        if not files_to_process:
            logging.info("No se encontraron facturas nuevas para procesar. Proceso finalizado.")
            return

        logging.info(f"Se encontraron {len(files_to_process)} facturas candidatas para procesar.")
    except Exception as e:
        logging.critical(f"FINALIZANDO: Falló la fase de descubrimiento. Error: {e}", exc_info=True)
        return

    # 4. Procesamiento de Cada Archivo
    logging.info("--- Fase 2: Procesamiento de Archivos ---")
    summary = {"processed_successfully": [], "failed_to_process": [], "skipped_duplicates": [], "metadata_extraction_failed": []}

    for file_info in files_to_process:
        file_id, file_name = file_info['id'], file_info['name']
        logging.info(f"--> Verificando: {file_name} (ID: {file_id})")

        local_pdf_path = None
        try:
            # 4.1. Descargar el PDF
            local_pdf_path = downloader.download_file(file_id, file_name)
            if not local_pdf_path:
                raise Exception("La descarga desde Google Drive falló.")

            # 4.2. Convertir PDF a HTML usando la librería
            html_path = TikaParser.pdf_to_html(local_pdf_path, html_output_dir)
            if not html_path:
                 raise Exception("La conversión de PDF a HTML con Tika falló.")

            # 4.3. Verificación temprana de metadatos desde el HTML
            logging.info(f"Extrayendo metadatos de {file_name} para verificar...")
            inv_no, _, _ = parser.extract_invoice_meta(html_path)

            if not inv_no:
                logging.error(f"FALLO METADATOS: No se pudo extraer N° de factura de {file_name}. Probablemente es un PDF de solo imagen.")
                summary["metadata_extraction_failed"].append(file_name)
                continue

            # 4.4. Verificación de duplicados por invoice_number
            if inv_no in processed_invoice_numbers:
                logging.warning(f"SALTANDO DUPLICADO: La factura N° {inv_no} del archivo {file_name} ya fue procesada.")
                summary["skipped_duplicates"].append(file_name)
                continue

            # 4.5. Si pasamos ambas verificaciones, procedemos a parsear el HTML
            logging.info(f"Procesando: {file_name} (Factura N° {inv_no}) no es un duplicado.")
            parsed_rows = parser.run(html_path) # Ahora le pasamos el HTML
            if not parsed_rows:
                raise BusinessException("PARSER_EMPTY_RESULT", "El parser no devolvió ninguna fila de detalle.")

            # 4.6. Enriquecer y Guardar
            timestamp = datetime.now(UTC).isoformat()
            for row in parsed_rows:
                row.update({'file_id': file_id, 'file_name': file_name, 'processing_timestamp': timestamp})

            # Convertimos a formato JSONL (una línea por objeto JSON)
            final_json_data = '\n'.join([json.dumps(row, ensure_ascii=False) for row in parsed_rows])
            
            # Guardamos el JSON localmente usando la librería
            local_json_path = Path(final_jsons_dir) / f"{Path(file_name).stem}.json"
            FileHandler.write_text(local_json_path, final_json_data)
            
            # Subimos a S3
            date_obj = datetime.fromisoformat(parsed_rows[0]['billing_cycle_date'])
            s3_json_key = f"invoices/mastercard/year={date_obj.year}/month={date_obj.month:02d}/{Path(file_name).stem}.json"
            
            if not s3_uploader.upload_json_to_s3(final_json_data, AWS_BUCKET_NAME, s3_json_key):
                raise Exception("La subida del archivo JSON a S3 falló.")

            logging.info(f"<-- ÉXITO: {file_name} procesado y subido a S3.")
            summary["processed_successfully"].append(file_name)

        except Exception as e:
            logging.error(f"<-- ERROR al procesar {file_name}: {e}", exc_info=True)
            summary["failed_to_process"].append(file_name)

    # 5. Resumen Final (sin cambios)
    logging.info("=======================================")
    logging.info("======= PIPELINE FINALIZADO =======")
    logging.info(f"Procesados con éxito: {len(summary['processed_successfully'])}")
    logging.info(f"Fallaron durante el proceso: {len(summary['failed_to_process'])}")
    logging.info(f"Fallaron por no poder extraer metadatos: {len(summary['metadata_extraction_failed'])}")
    logging.info(f"Saltados por duplicado: {len(summary['skipped_duplicates'])}")
    logging.info("=======================================")

if __name__ == "__main__":
    run_pipeline()