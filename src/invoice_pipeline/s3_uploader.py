# src/invoice_pipeline/s3_uploader.py
import logging
import boto3
from botocore.exceptions import ClientError

class S3Uploader:
    def __init__(self, region_name: str):
        """
        Inicializa el uploader de S3.
        :param region_name: La región de AWS donde está el bucket.
        """
        self.s3_client = boto3.client('s3', region_name=region_name)
        logging.info("S3Uploader inicializado.")

    def upload_json_to_s3(self, json_data: str, bucket_name: str, s3_key: str) -> bool:
        """
        Sube un string en formato JSON a una ruta específica en un bucket de S3.
        
        :param json_data: El contenido del archivo JSON como un string.
        :param bucket_name: El nombre del bucket de S3.
        :param s3_key: La ruta completa del archivo en S3 (ej. 'invoices/2025/02/factura_123.json').
        :return: True si la subida fue exitosa, False en caso contrario.
        """
        try:
            # S3 espera los datos en formato de bytes, así que codificamos el string a utf-8
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            logging.info(f"Archivo JSON subido exitosamente a s3://{bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logging.error(f"Error al subir el archivo a S3: {e}")
            return False
        except Exception as e:
            logging.error(f"Un error inesperado ocurrió durante la subida a S3: {e}")
            return False