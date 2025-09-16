# src/invoice_pipeline/__init__.py
__all__ = ["config", "textract_client", "parser", "storage", "orchestrator", "handler"]

# Carga automática del .env desde la raíz del repo
try:
    from pathlib import Path
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except Exception:
    pass
