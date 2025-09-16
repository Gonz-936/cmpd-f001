# src/invoice_pipeline/athena_connector.py
import logging
import time
import boto3
from botocore.exceptions import ClientError

class AthenaConnector:
    def __init__(self, database_name: str, bucket_name: str, region_name: str):
        """
        Inicializa el conector de Athena.
        :param database_name: El nombre de la base de datos en Athena.
        :param bucket_name: El bucket de S3 donde Athena guardará los resultados de las consultas.
        :param region_name: La región de AWS.
        """
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.database = database_name
        self.s3_output_location = f's3://{bucket_name}/athena_query_results/'
        logging.info("AthenaConnector inicializado.")

    def get_processed_file_ids(self, table_name: str) -> set[str]:
        """
        Consulta la tabla de facturas en Athena para obtener todos los file_id únicos
        que ya han sido procesados.
        
        :param table_name: El nombre de la tabla que contiene los datos de las facturas.
        :return: Un conjunto (set) de strings, donde cada string es un file_id.
        """
        query = f'SELECT DISTINCT file_id FROM "{table_name}" WHERE file_id IS NOT NULL;'
        logging.info(f"Ejecutando consulta en Athena para obtener IDs procesados: {query}")

        try:
            # 1. Iniciar la ejecución de la consulta
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            query_execution_id = response['QueryExecutionId']
            logging.info(f"Consulta iniciada con ID: {query_execution_id}")

            # 2. Esperar a que la consulta termine
            while True:
                stats = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = stats['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1) # Esperar 1 segundo antes de volver a preguntar

            if status != 'SUCCEEDED':
                # Si la consulta falló por otra razón, lo registramos.
                error_info = stats['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido.')
                logging.error(f"La consulta de Athena falló con estado: {status}. Razón: {error_info}")
                return set()

            # 3. Obtener y procesar los resultados
            results_paginator = self.athena_client.get_paginator('get_query_results')
            results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
            
            processed_ids = set()
            for results in results_iter:
                for row in results['ResultSet']['Rows'][1:]: # Omitir la primera fila que es el header
                    if 'Data' in row and row['Data'][0].get('VarCharValue'):
                        processed_ids.add(row['Data'][0]['VarCharValue'])
            
            logging.info(f"Se encontraron {len(processed_ids)} file_ids ya procesados en Athena.")
            return processed_ids

        except ClientError as e:
            # --- ESTA ES LA LÓGICA QUE FALTABA ---
            # Verificamos si el error es específicamente porque la tabla no existe.
            if e.response['Error']['Code'] == 'InvalidRequestException' and 'does not exist' in e.response['Error']['Message']:
                logging.warning(f"La tabla '{table_name}' no existe aún. Se asume que no hay archivos procesados.")
                # Devolvemos un conjunto vacío, que es el comportamiento correcto para la primera ejecución.
                return set()
            else:
                # Si es otro tipo de error, lo registramos.
                logging.error(f"Ocurrió un ClientError al consultar Athena: {e}")
                return set()
        except Exception as e:
            logging.error(f"Ocurrió un error inesperado al consultar Athena: {e}", exc_info=True)
            return set()