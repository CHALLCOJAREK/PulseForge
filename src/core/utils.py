# src/core/utils.py
from __future__ import annotations

# --- BOOTSTRAP RUTAS ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import re
import unicodedata
from datetime import datetime, date


# ============================================================
#   UTILIDADES CORE — SILENCIOSAS Y OPTIMIZADAS
# ============================================================


# ------------------------------------------------------------
#  NORMALIZACIÓN DE TEXTO PARA COMPARACIONES
# ------------------------------------------------------------
def normalize_text(text: str | None) -> str:
    """
    Limpia tildes, pasa a minúsculas y deja solo caracteres comunes.
    No imprime logs (silencioso).
    """
    if not text:
        return ""

    # quitar tildes
    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))

    # minúsculas
    t = t.lower().strip()

    # limpieza suave (no agresiva)
    t = re.sub(r"[^a-z0-9\s\-\.\&/]", "", t)

    # colapsar espacios
    t = re.sub(r"\s+", " ", t).strip()

    return t


# ------------------------------------------------------------
#  PARSER SIMPLE DE MONTOS
# ------------------------------------------------------------
def clean_amount(value) -> float:
    """
    Convierte strings numéricos razonables en float.
    Silencioso y rápido (no usa logger).
    """
    if value is None or value == "":
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    v = str(value).strip()

    # eliminar símbolos monetarios básicos
    v = re.sub(r"[^\d.,\-]", "", v)

    # si tiene coma decimal
    if "," in v and "." in v:
        # europeo: 1.234,56
        if v.rfind(",") > v.rfind("."):
            v = v.replace(".", "")
            v = v.replace(",", ".")
        else:
            v = v.replace(",", "")

    elif "," in v:
        v = v.replace(",", ".")

    # convertir final
    try:
        return float(v)
    except:
        return 0.0


# ------------------------------------------------------------
#  PARSE DE FECHA MULTIFORMATO
# ------------------------------------------------------------
def parse_date(value) -> date:
    """
    Convierte múltiples formatos a date (YYYY-MM-DD).
    Silencioso.
    """
    if value is None or value == "":
        raise ValueError("Fecha vacía.")

    if isinstance(value, (date, datetime)):
        return value if isinstance(value, date) else value.date()

    v = str(value).strip()

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

    raise ValueError(f"Formato de fecha desconocido: {value}")


# ------------------------------------------------------------
#  FORMATOS DE FECHA
# ------------------------------------------------------------
def format_date_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


# ------------------------------------------------------------
#  DIFERENCIA DE DÍAS ENTRE FECHAS
# ------------------------------------------------------------
def date_diff_days(d1: date, d2: date) -> int:
    return abs((d2 - d1).days)


# ------------------------------------------------------------
#  NORMALIZACIÓN DE RUC: SOLO DÍGITOS
# ------------------------------------------------------------
def clean_ruc(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^\d]", "", str(value))
