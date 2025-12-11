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
from typing import Optional


# =====================================================
# NORMALIZACIÓN UNIVERSAL DE TEXTO
# =====================================================
# Permitimos SOLO caracteres útiles en negocio y data:
WHITELIST = r"a-zA-Z0-9\s\-\.\&/_,:@#\%\+\(\)\[\]"

def normalize_text(text: str | None) -> str:
    if not text:
        return ""

    # Quitar tildes, ñ → n, etc.
    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower().strip()

    # Eliminar TODO lo que NO esté en whitelist
    t = re.sub(fr"[^{WHITELIST}]", "", t)

    # Normalizar espacios
    t = re.sub(r"\s+", " ", t).strip()

    return t


# =====================================================
# LIMPIEZA UNIVERSAL DE MONTOS (TODOS LOS FORMATOS)
# =====================================================
def clean_amount(value) -> float:
    """
    Limpieza PRO de montos:
    - Acepta formatos EU (1.234,56) o US (1,234.56)
    - Elimina símbolos: S/, $, PEN, etc.
    - Elimina NBSP, tabs y espacios invisibles
    - Soporta negativos raros: - 1.200,50  / (1,200.50)
    """

    # ------------------------------------
    # 1. Nulos
    # ------------------------------------
    if value is None:
        return 0.0

    # Si ya es número, devolver float directo
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except:
            return 0.0

    # ------------------------------------
    # 2. Convertir a string y normalizar unicode
    # ------------------------------------
    v = str(value)
    v = unicodedata.normalize("NFKD", v)

    # Quitar espacios invisibles
    v = v.replace("\u00A0", " ").strip()  # NBSP → espacio normal

    if v == "":
        return 0.0

    # ------------------------------------
    # 3. Manejar negativos tipo "(123.45)"
    # ------------------------------------
    negative = False
    if "(" in v and ")" in v:
        negative = True
        v = v.replace("(", "").replace(")", "")

    # ------------------------------------
    # 4. Quitar letras y símbolos no numéricos
    # ------------------------------------
    v = re.sub(r"[^\d\.,\-]", "", v)

    # Quitar múltiples signos menos
    v = v.replace("--", "-")

    # ------------------------------------
    # 5. Si hay varias comas y puntos, decidir formato
    # ------------------------------------
    # Caso: EU → "1.234,56"  (coma es decimal)
    # Caso: US → "1,234.56"  (punto es decimal)
    if v.count(",") + v.count(".") > 1:

        # Si la última coma está DESPUÉS del último punto → formato EU
        if v.rfind(",") > v.rfind("."):
            # EU → quitar puntos (miles), coma → punto decimal
            v = v.replace(".", "").replace(",", ".")
        else:
            # US → quitar comas (miles)
            v = v.replace(",", "")

    else:
        # Si tiene solo coma → decimal
        if "," in v:
            v = v.replace(",", ".")

    # ------------------------------------
    # 6. Conversión final
    # ------------------------------------
    try:
        num = float(v)
        return -num if negative else num
    except:
        return 0.0



# =====================================================
# PARSE UNIVERSAL DE FECHAS
# =====================================================
MESES_MAP = {
    # Español
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    # Inglés
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def _parse_mes_escrito(v: str) -> Optional[date]:
    m = re.match(r"(\d{1,2})\s*[- ]?\s*([A-Za-z]{3,})\s*[- ]?\s*(\d{4})", v)
    if not m:
        return None

    d = int(m.group(1))
    mes_txt = m.group(2)[:3].lower()
    y = int(m.group(3))

    if mes_txt not in MESES_MAP:
        return None

    try:
        return date(y, MESES_MAP[mes_txt], d)
    except:
        return None


def parse_date(value) -> date | None:
    if value is None:
        return None

    v = str(value).strip()

    # Basura o vacío
    if v in ("", "nan", "NaT", "None", "0", "00", "0000", "0000-00-00"):
        return None

    # Hora sola → no es fecha
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", v):
        return None

    # Si trae hora → cortar
    if " " in v:
        v = v.split(" ")[0]

    # Si ya es date/datetime
    if isinstance(value, (datetime, date)):
        return value if isinstance(value, date) else value.date()

    # Mes escrito (Ene, Jan...)
    d = _parse_mes_escrito(v)
    if d:
        return d

    # Formatos globales
    formatos = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y.%m.%d",
        "%d.%m.%Y",
        "%Y%m%d",
    ]

    for f in formatos:
        try:
            return datetime.strptime(v, f).date()
        except:
            pass

    return None


# =====================================================
# FORMATO YYYYMMDD
# =====================================================
def format_date_yyyymmdd(d: date | None) -> str | None:
    if d is None:
        return None
    return d.strftime("%Y%m%d")


# =====================================================
# DIFERENCIA EN DÍAS
# =====================================================
def date_diff_days(d1: date, d2: date) -> int:
    return abs((d2 - d1).days)


# =====================================================
# LIMPIEZA UNIVERSAL DE DOCUMENTOS (RUC, DNI, CE, ETC.)
# =====================================================
def clean_ruc(value: str | None) -> str:
    if not value:
        return ""
    # Eliminar TODO lo que no sea número
    return re.sub(r"[^\d]", "", str(value))
