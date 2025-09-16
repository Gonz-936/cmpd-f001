# src/invoice_pipeline/config.py
import os

BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "process_data")
LOG_LEVEL = os.getenv("PIPELINE_LOG_LEVEL", "INFO")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TEXTRACT_SYNC_MAX_SIZE_MB = int(os.getenv("TEXTRACT_SYNC_MAX_SIZE_MB", "10"))
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "copec-invoice-pipeline-data")

# NUEVA LÍNEA: Nombre del secreto en AWS Secrets Manager
GDRIVE_SECRET_NAME = os.getenv("GDRIVE_SECRET_NAME", "invoice-pipeline/gdrive-credentials")
# Rol de IAM que Textract asume para operaciones asíncronas
TEXTRACT_ROLE_ARN = os.getenv("TEXTRACT_ROLE_ARN", "arn:aws:iam::551529992598:role/Textract-Service-Role")