# src/core/utils.py
from __future__ import annotations

# -------------------------
# Bootstrap interno
# -------------------------
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import re
import unicodedata
from datetime import datetime, date


# -------------------------
# Normalización de texto
# -------------------------
def normalize_text(text: str | None) -> str:
    if not text:
        return ""

    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower().strip()

    t = re.sub(r"[^a-z0-9\s\-\.\&/]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# -------------------------
# Limpieza de montos
# -------------------------
def clean_amount(value) -> float:
    if value is None or value == "":
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    v = re.sub(r"[^\d.,\-]", "", str(value).strip())

    if "," in v and "." in v:
        if v.rfind(",") > v.rfind("."):
            v = v.replace(".", "").replace(",", ".")
        else:
            v = v.replace(",", "")
    elif "," in v:
        v = v.replace(",", ".")

    try:
        return float(v)
    except:
        return 0.0


# -------------------------
# Parse flexible de fechas (ULTRA ROBUSTO)
# -------------------------
def parse_date(value) -> date | None:
    """
    Acepta:
        - 2025-01-30
        - 30/01/2025
        - 2025/01/30
        - 20250130
        - 30-01-2025
        - 2025-01-30 00:00:00
        - 2025/01/30 12:45:00
        - 00:00:00       ← ahora retorna None
        - valores basura
    """

    if value is None:
        return None

    v = str(value).strip()

    # Vacíos o basura
    if v in ("", "nan", "NaT", "None", "0", "00", "0000", "0000-00-00"):
        return None

    # Solo hora → no es fecha
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", v):
        return None

    # Fecha con hora → quedarse solo con la fecha
    if " " in v:
        solo_fecha = v.split(" ")[0]
        v = solo_fecha

    # Si ya es datetime/date
    try:
        if isinstance(value, (datetime, date)):
            return value if isinstance(value, date) else value.date()
    except:
        pass

    # Intentar varios formatos
    formatos = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%Y%m%d",
        "%d-%m-%Y",
    ]

    for f in formatos:
        try:
            return datetime.strptime(v, f).date()
        except:
            pass

    # No se reconoce → retornamos None (YA NO ROMPE TODO)
    return None


# -------------------------
# Formato YYYYMMDD
# -------------------------
def format_date_yyyymmdd(d: date | None) -> str | None:
    if d is None:
        return None
    return d.strftime("%Y%m%d")


# -------------------------
# Diferencia de días
# -------------------------
def date_diff_days(d1: date, d2: date) -> int:
    return abs((d2 - d1).days)


# -------------------------
# Limpieza de RUC
# -------------------------
def clean_ruc(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^\d]", "", str(value))
