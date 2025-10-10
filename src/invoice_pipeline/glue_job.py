# /src/invoice_pipeline/glue_job.py

import sys
import logging
import re
from datetime import datetime, timezone

# --- Librerías de AWS Glue (ahora importadas directamente) ---
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- Librerías de Terceros ---
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# --- Tus módulos ---
from py_toolbox.aws.athena_connector import AthenaConnector
from py_toolbox.google.google_drive_downloader import GoogleDriveDownloader
from py_toolbox.processing.tika_parser import TikaParser
from py_toolbox.utils.file_handler import FileHandler
from invoice_pipeline.parser import ParserService, BusinessException

# --- Configuración del Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_secret(secret_name: str, region_name: str) -> str | None:
    """
    Obtiene un secreto de AWS Secrets Manager.
    """
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

def run_pipeline(spark, args):
    """
    Esta función contiene la lógica de negocio principal.
    """
    logging.info(f"Argumentos recibidos: {args}")

    # --- 1. Definición de directorios de trabajo ---
    # Se asume siempre un entorno tipo Glue, por lo que usamos /tmp
    base_dir = "/tmp"
    downloads_dir = f"{base_dir}/downloads"
    html_output_dir = f"{base_dir}/html_output"
    FileHandler.ensure_dirs(downloads_dir, html_output_dir)

    # --- 2. Inicialización de Conectores ---
    gdrive_credentials_json = get_secret(args['GDRIVE_SECRET_NAME'], args['AWS_REGION'])
    if not gdrive_credentials_json:
        raise RuntimeError("FINALIZANDO: No se pudieron obtener las credenciales de Google Drive.")

    downloader = GoogleDriveDownloader(gdrive_credentials_json, downloads_dir)
    parser = ParserService()
    athena = AthenaConnector(args['ATHENA_DATABASE'], args['S3_OUTPUT_BUCKET'], args['AWS_REGION'])

    # --- 3. Descubrimiento de Archivos y Duplicados (Lógica sin cambios) ---
    try:
        logging.info("--- Fase 1: Descubrimiento de Archivos y Duplicados ---")
        processed_file_ids = athena.get_processed_file_ids(args['ATHENA_TABLE'])
        processed_invoice_numbers = athena.get_processed_invoice_numbers(args['ATHENA_TABLE'])

        main_folder_id = args['GDRIVE_ROOT_FOLDER_ID']
        logging.info(f"ID de la carpeta raíz de Google Drive: {main_folder_id}")

        current_year = str(datetime.now().year)
        year_folder_id = downloader.get_folder_id(current_year, parent_folder_id=main_folder_id)

        all_files = downloader.list_all_files_recursively(start_folder_id=year_folder_id) if year_folder_id else []
        if not all_files:
            logging.warning(f"No se encontró la subcarpeta para el año {current_year} o no contiene archivos.")

        invoice_filename_pattern = re.compile(r"^MCI_Invoice_.*\.pdf$", re.IGNORECASE)
        files_to_process = [
            f for f in all_files
            if invoice_filename_pattern.match(f['name']) and f['id'] not in processed_file_ids
        ]

        if not files_to_process:
            logging.info("No se encontraron facturas nuevas para procesar. Proceso finalizado.")
            return

        logging.info(f"Se encontraron {len(files_to_process)} facturas candidatas para procesar.")

    except Exception as e:
        raise RuntimeError(f"FINALIZANDO: Falló la fase de descubrimiento. Error: {e}")

    # --- 4. Procesamiento de Cada Archivo (Lógica sin cambios) ---
    logging.info("--- Fase 2: Procesamiento de Archivos ---")
    all_processed_rows = []
    summary = {"processed_successfully": 0, "failed_to_process": 0, "skipped_duplicates": 0, "metadata_extraction_failed": 0}

    for file_info in files_to_process:
        file_id, file_name = file_info['id'], file_info['name']
        logging.info(f"--> Procesando: {file_name} (ID: {file_id})")

        try:
            local_pdf_path = downloader.download_file(file_id, file_name)
            if not local_pdf_path:
                raise Exception("La descarga desde Google Drive falló.")

            html_path = TikaParser.pdf_to_html(local_pdf_path, html_output_dir)
            if not html_path:
                raise Exception("La conversión de PDF a HTML con Tika falló.")

            inv_no, _, _ = parser.extract_invoice_meta(html_path)
            if not inv_no:
                logging.error(f"FALLO METADATOS: No se pudo extraer N° de factura de {file_name}.")
                summary["metadata_extraction_failed"] += 1
                continue

            if inv_no in processed_invoice_numbers:
                logging.warning(f"SALTANDO DUPLICADO: La factura N° {inv_no} del archivo {file_name} ya fue procesada.")
                summary["skipped_duplicates"] += 1
                continue

            logging.info(f"Parseando: {file_name} (Factura N° {inv_no}).")
            parsed_rows = parser.run(html_path)
            if not parsed_rows:
                raise BusinessException("PARSER_EMPTY_RESULT", "El parser no devolvió ninguna fila de detalle.")

            timestamp = datetime.now(timezone.utc).isoformat()
            for row in parsed_rows:
                row.update({'file_id': file_id, 'file_name': file_name, 'processing_timestamp': timestamp})
            
            all_processed_rows.extend(parsed_rows)
            summary["processed_successfully"] += 1
            logging.info(f"<-- ÉXITO: {file_name} parseado correctamente.")

        except Exception as e:
            logging.error(f"<-- ERROR al procesar {file_name}: {e}", exc_info=True)
            summary["failed_to_process"] += 1

    # --- 5. Guardado de Datos en Formato Parquet (Lógica sin cambios) ---
    if not all_processed_rows:
        logging.warning("No se procesaron filas nuevas. No hay datos para guardar en S3.")
    else:
        try:
            logging.info(f"--- Fase 3: Guardando {len(all_processed_rows)} filas ---")
            
            pd_df = pd.DataFrame(all_processed_rows)
            
            pd_df['billing_cycle_date'] = pd.to_datetime(pd_df['billing_cycle_date'])
            pd_df['year'] = pd_df['billing_cycle_date'].dt.year
            pd_df['month'] = pd_df['billing_cycle_date'].dt.month
            pd_df['billing_cycle_date'] = pd_df['billing_cycle_date'].dt.date
            
            numeric_cols = ['quantity_amount', 'rate', 'charge', 'tax_amount', 'total_charge']
            for col in numeric_cols:
                pd_df[col] = pd.to_numeric(pd_df[col], errors='coerce')
            
            spark_df = spark.createDataFrame(pd_df)
            
            s3_output_path = f"s3://{args['S3_OUTPUT_BUCKET']}/invoices/mastercard/"
            logging.info(f"Escribiendo DataFrame en formato Parquet en: {s3_output_path}")
            spark_df.write.partitionBy("year", "month").mode("append").parquet(s3_output_path)

            logging.info("Escritura de datos completada exitosamente.")

        except Exception as e:
            raise RuntimeError(f"FINALIZANDO: Falló la escritura de datos. Error: {e}")

    # --- 6. Resumen Final (Lógica sin cambios) ---
    logging.info("=======================================")
    logging.info("======= PROCESO FINALIZADO =======")
    logging.info(f"Archivos procesados con éxito: {summary['processed_successfully']}")
    logging.info(f"Archivos que fallaron durante el proceso: {summary['failed_to_process']}")
    logging.info(f"Archivos que fallaron por extracción de metadatos: {summary['metadata_extraction_failed']}")
    logging.info(f"Archivos saltados por duplicado: {summary['skipped_duplicates']}")
    logging.info("=======================================")

def main():

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'GDRIVE_ROOT_FOLDER_ID', 'GDRIVE_SECRET_NAME',
        'AWS_REGION', 'ATHENA_DATABASE', 'ATHENA_TABLE', 'S3_OUTPUT_BUCKET'
    ])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        run_pipeline(spark, args)
        job.commit()
    except Exception as e:
        logging.critical(f"El job ha fallado con una excepción no controlada: {e}", exc_info=True)
        raise e

if __name__ == "__main__":
    main()