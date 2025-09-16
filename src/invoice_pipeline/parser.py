# src/invoice_pipeline/parser.py

import re
import json
import logging
import os
from datetime import date
from pathlib import Path
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from tika import parser as tika_parser

from .storage import ensure_dirs
from .config import BASE_DIR, LOG_LEVEL

# =========================
#   Excepción de negocio
# =========================
class BusinessException(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code

# =========================
#   Regex y firmas clave
# =========================
NUM_RX = r"-?[\d,]+(?:\.\d+)?"
ONLY_NUM_RX = re.compile(rf"^{NUM_RX}$")

ROW_RX = re.compile(
    rf"""^
    (?P<event_code>[A-Z0-9]+)\s+
    (?P<description>.*?)\s+
    (?P<service_code>[A-Z0-9]{{1,4}})\s+
    (?P<uom>[A-Z])\s+
    (?P<qty>{NUM_RX})\s+
    (?P<rate>{NUM_RX})\s+
    (?P<charge>{NUM_RX})\s+
    (?P<tax>{NUM_RX})\s+
    (?P<total>{NUM_RX})
    $""",
    re.VERBOSE,
)
INVOICE_FIELD_RX = re.compile(
    r"(?:Invoice\s*(?:#|No\.?|Number)?\s*:?\s*)(\d[\d\-]{9,})",
    re.IGNORECASE,
)

BILLING_DATE_RX = re.compile(
    r"Billing\s*Cycle\s*Date\s*:\s*([A-Z]{3})\s+(\d{1,2})\s+(\d{4})",
    re.IGNORECASE,
)
CURRENCY_RX = re.compile(r"Currency\s*:\s*([A-Z]{3})", re.IGNORECASE)
MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}
HEADER_SIGNATURE_PARTS = [
    "Event Service Quantity/ Tax Total",
    "Code Description Code UOM Amount Rate Charge Amount Charge",
]

# =========================
#         Helpers
# =========================
def norm(s: str | None) -> str:
    return " ".join((s or "").replace("\xa0", " ").strip().split())

def is_subtotal_line(line: str) -> bool:
    return bool(ONLY_NUM_RX.fullmatch(line.strip()))

def is_detail_header(text: str) -> bool:
    t = norm(text)
    return all(sig in t for sig in HEADER_SIGNATURE_PARTS)

def to_float(s: str) -> float:
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return 0.0

def fuzzy_best(s: str, candidates: list[str]) -> tuple[str | None, float]:
    s_n = norm(s).lower()
    best, score = None, 0.0
    for c in candidates:
        r = SequenceMatcher(None, s_n, norm(c).lower()).ratio()
        if r > score:
            best, score = c, r
    return best, score

# =========================
#     Servicio principal
# =========================
class ParserService:
    def __init__(self, base_dir: str = BASE_DIR):
        self.dirs = ensure_dirs(base_dir)
        # La configuración del logging ahora la maneja el orquestador
        # para tener un log centralizado.
    def pdf_to_html(self, pdf_path: str) -> str:
        pdf_path_obj = Path(pdf_path)
        # Creamos un nombre de archivo HTML basado en el nombre del PDF
        html_file_name = pdf_path_obj.stem + ".html"
        html_out_path = self.dirs["html_output"] / html_file_name

        logging.info(f"Iniciando conversión de PDF a HTML para: {pdf_path}")
        try:
            parsed = tika_parser.from_file(str(pdf_path), xmlContent=True)
            html = parsed.get("content")
            
            if not html:
                raise BusinessException("TIKA_EMPTY_CONTENT", "Tika no pudo extraer contenido.")

            html_out_path.write_text(html, encoding="utf-8")
            logging.info(f"HTML generado en: {html_out_path}")
            return str(html_out_path)
        except Exception as e:
            raise BusinessException("TIKA_FAILURE", f"Error procesando PDF con Tika: {e}")
    def extract_invoice_meta(self, html_path: str) -> tuple[int | None, date | None, str | None]:
        soup = BeautifulSoup(Path(html_path).read_text(encoding="utf-8"), "html.parser")
        ps = soup.find_all("p")

        def digits_only(s: str) -> str:
            return re.sub(r"\D", "", s)

        def grab_number(text: str) -> int | None:
            m = INVOICE_FIELD_RX.search(text)
            if m: return int(digits_only(m.group(1)))
            m2 = re.search(r"(\d[\d\s\-]{8,})", text)
            if m2: return int(digits_only(m2.group(1)))
            return None

        def grab_billing_date(text: str) -> date | None:
            m = BILLING_DATE_RX.search(text)
            if not m: return None
            mon, day, year = m.groups()
            mon = mon.upper()
            if mon not in MONTHS: return None
            try:
                return date(int(year), MONTHS[mon], int(day))
            except ValueError:
                return None

        def grab_currency(text: str) -> str | None:
            m = CURRENCY_RX.search(text)
            return m.group(1).upper() if m else None

        inv_no = inv_dt = curr = None
        for i, p in enumerate(ps):
            if norm(p.get_text()).lower() == "invoice":
                next_txt = ps[i + 1].get_text(" ") if i + 1 < len(ps) else ""
                block_txt = p.get_text(" ")
                inv_no = grab_number(next_txt) or grab_number(block_txt)
                inv_dt = grab_billing_date(next_txt) or grab_billing_date(block_txt)
                curr = grab_currency(next_txt) or grab_currency(block_txt)
                break

        if inv_no is None or inv_dt is None or curr is None:
            full_text = "\n".join(p.get_text(" ") for p in ps)
            inv_no = inv_no or grab_number(full_text)
            inv_dt = inv_dt or grab_billing_date(full_text)
            curr = curr or grab_currency(full_text)

        if inv_no is None: logging.warning("No se encontró 'Invoice # <nro>'.")
        if inv_dt is None: logging.warning("No se encontró 'Billing Cycle Date: <MON DD YYYY>'.")
        if curr is None: logging.warning("No se encontró 'Currency: <CCC>'.")

        return inv_no, inv_dt, curr
    def parse_detail_table(self, html_path: str) -> list[dict]:
            soup = BeautifulSoup(Path(html_path).read_text(encoding="utf-8"), "html.parser")
            invoice_number, billing_date, currency = self.extract_invoice_meta(html_path)
            paras = [p.get_text("\n") for p in soup.find_all("p")]
            rows_out: list[dict] = []


            for idx, ptxt in enumerate(paras):
                for line in ptxt.split("\n"):
                    m = ROW_RX.match(norm(line))
                    if not m: 
                        continue
                    gd = m.groupdict()
                    rows_out.append({
                        "invoice_number": invoice_number,
                        "billing_cycle_date": billing_date.isoformat() if billing_date else None,
                        "currency": currency,
                        "event_code": gd["event_code"], "description": gd["description"],
                        "service_code": gd["service_code"], "uom": gd["uom"],
                        "quantity_amount": to_float(gd["qty"]), "rate": to_float(gd["rate"]),
                        "charge": to_float(gd["charge"]), "tax_amount": to_float(gd["tax"]),
                        "total_charge": to_float(gd["total"]),
                    })
            return rows_out

    def run(self, local_pdf_path: str) -> list[dict]:
        """
        Ejecuta el pipeline de parseo.
        """
        if not local_pdf_path or not Path(local_pdf_path).exists():
            raise BusinessException("INPUT_MISSING", f"El archivo PDF no se encuentra en: {local_pdf_path}")

        logging.info(f"Iniciando parseo del archivo: {local_pdf_path}")

        # 1. Convertir el PDF a HTML
        html_file = self.pdf_to_html(local_pdf_path)

        # 2. Parsear la tabla de detalles (ya no necesita el catálogo)
        rows = self.parse_detail_table(html_file)

        logging.info(f"Parseo completado. Se extrajeron {len(rows)} filas.")
        return rows
