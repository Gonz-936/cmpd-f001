# src/invoice_pipeline/storage.py
from pathlib import Path

def ensure_dirs(base_dir: str | Path = "process_data"):
    base = Path(base_dir)
    # Ahora solo define las rutas a las carpetas
    paths = {
        "base": base,
        "html_output": base / "html_output",
        "json_output": base / "json_output",
        "logs": base / "logs",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths