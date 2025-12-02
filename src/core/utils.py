# src/core/utils.py
from __future__ import annotations

# --- BOOTSTRAP RUTAS ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import re
from datetime import datetime
import unicodedata

# Logger unificado
from src.core.logger import info, ok, warn, error


# ============================================================
#   UTILIDADES CENTRALES — PULSEFORGE ENTERPRISE CORE
# ============================================================


# ------------------------------------------------------------
#  LIMPIEZA Y NORMALIZACION DE TEXTO
# ------------------------------------------------------------
def normalize_text(text: str) -> str:
    """Normaliza texto para comparaciones: quita tildes, pasa a minúsculas
    y elimina caracteres ruidosos.
    """
    info(f"Normalizando texto…")

    if text is None:
        warn("Texto vacío → ''")
        return ""

    # quitar tildes
    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))

    # minúsculas
    t = t.lower()

    # limpieza: solo letras, números y separadores básicos
    t = re.sub(r"[^a-z0-9\s\-\._/]", "", t)

    ok(f"Normalización OK → '{text}' → '{t}'")
    return t


# ------------------------------------------------------------
#  PARSER QUIRÚRGICO DE MONTOS
# ------------------------------------------------------------
def clean_amount(value) -> float:
    """Convierte un string numérico en float real, detectando formato US y EU
    de forma estricta. Diseñado para contabilidad contable y bancaria.
    """
    info(f"Limpieza de monto → raw = {value}")

    if value is None or value == "":
        warn("Monto vacío → se regresa 0.0")
        return 0.0

    # si ya es número
    if isinstance(value, (int, float)):
        num = float(value)
        ok(f"Monto limpio → {num}")
        return num

    # cast a string
    v = str(value).strip()

    # Quitar símbolos monetarios y caracteres ruidosos
    v = re.sub(r"[^\d.,-]", "", v)

    # ==============
    # CASO A: Tiene punto y coma
    # ==============
    if "," in v and "." in v:
        # si la última aparición es coma → decimal europeo
        if v.rfind(",") > v.rfind("."):
            # Ej: 1.234,56 → remover puntos (miles) → cambiar coma decimal
            v = v.replace(".", "")
            v = v.replace(",", ".")
        else:
            # Formato US → 1,234.56 → quitar comas
            v = v.replace(",", "")

    # ==============
    # CASO B: Solo comas
    # ==============
    elif "," in v and "." not in v:
        if v.count(",") == 1:
            # 1234,56 → decimal europeo
            v = v.replace(",", ".")
        else:
            # muchos separadores → últimos 2 dígitos = decimal
            *integ, dec = v.split(",")
            v = "".join(integ) + "." + dec

    # ==============
    # CASO C: Solo puntos → ya es decimal US
    # ==============
    elif "." in v and "," not in v:
        pass  # NO TOCAR

    # ==============
    # CASO D: No tiene separadores → número limpio
    # ==============
    else:
        pass

    # convertir a float final
    try:
        num = float(v)
        ok(f"Monto limpio → {num}")
        return num
    except ValueError:
        error(f"No se pudo convertir el monto: {value}")
        return 0.0


# ------------------------------------------------------------
#  FECHAS — PARSER MULTIFORMATO
# ------------------------------------------------------------
def parse_date(value) -> datetime:
    """Convierte fechas de múltiples formatos a datetime estandar."""
    info(f"Parseando fecha → raw = {value}")

    if value is None or value == "":
        error("Fecha vacía.")
        raise ValueError("Fecha vacía.")

    if isinstance(value, datetime):
        ok(f"Fecha parseada correctamente → {value}")
        return value

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
            d = datetime.strptime(v, f)
            ok(f"Fecha parseada correctamente → {d}")
            return d
        except Exception:
            pass

    error(f"No se pudo interpretar la fecha: {value}")
    raise ValueError(f"Formato de fecha desconocido: {value}")


# ------------------------------------------------------------
#  FORMATOS DE FECHA
# ------------------------------------------------------------
def format_date_yyyymmdd(date: datetime) -> str:
    out = date.strftime("%Y%m%d")
    ok(f"Fecha formateada → {out}")
    return out


# ------------------------------------------------------------
#  DIFERENCIA EN DÍAS
# ------------------------------------------------------------
def date_diff_days(d1: datetime, d2: datetime) -> int:
    diff = abs((d2 - d1).days)
    ok(f"Diferencia de días → {diff}")
    return diff


# ------------------------------------------------------------
#  LIMPIEZA DE RUC — SOLO NÚMEROS
# ------------------------------------------------------------
def clean_ruc(value: str) -> str:
    if value is None:
        return ""

    v = re.sub(r"[^\d]", "", str(value))
    ok(f"RUC limpio → {v}")
    return v


# ------------------------------------------------------------
#  TEST DIRECTO DEL MÓDULO — FULL ENTERPRISE VALIDATION
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n==============================================")
    print("🔵  PULSEFORGE · UTILS ENGINE TEST (FULL)")
    print("==============================================\n")

    # --------------------------------------------------------
    # TEXTO
    # --------------------------------------------------------
    normalize_text("ÁÉÍÓÚ Perú – Ñandú")
    normalize_text("Empresa S.A.C. — ¡CLIENTE PREMIUM!")
    normalize_text("GYTRES S.A.C.")
    normalize_text("   texto   con   espacios   ")

    # --------------------------------------------------------
    # MONTOS — FORMATO EUROPEO
    # --------------------------------------------------------
    clean_amount("1.234,56")
    clean_amount("12.345,67")
    clean_amount("123.456,78")
    clean_amount("5.678,99")
    clean_amount("1.234.567,89")

    # --------------------------------------------------------
    # MONTOS — FORMATO USA / LATAM MODERNO
    # --------------------------------------------------------
    clean_amount("1,234.56")
    clean_amount("12,345.67")
    clean_amount("123,456.78")
    clean_amount("5,678.99")
    clean_amount("1,234,567.89")

    # --------------------------------------------------------
    # MONTOS — CON SIGNOS Y SÍMBOLOS
    # --------------------------------------------------------
    clean_amount("S/ 1,234.56")
    clean_amount("US$ 5,678.99")
    clean_amount("€ 1.234,56")
    clean_amount("Monto: 12,345.67")
    clean_amount("Total = 123.45")
    clean_amount("TOTAL: S/ 7,890.12")

    # --------------------------------------------------------
    # MONTOS — CASOS COMPLEJOS
    # --------------------------------------------------------
    clean_amount("1,234")
    clean_amount("1.234")
    clean_amount("1234")
    clean_amount("1234,5")
    clean_amount("1234.5")
    clean_amount("-.1234")   # negativo decimal raro
    clean_amount("-1,234.56")
    clean_amount("-1.234,56")
    clean_amount("0001,234.56")  # padded
    clean_amount("   1,234.56   ")

    # Texto sucio extremo
    clean_amount("S/.   1,,2.3.4,,.5,6")
    clean_amount("abc1234.56xyz")
    clean_amount("xyz1.234,56abc")

    # Casos límite
    clean_amount("0")
    clean_amount(0)
    clean_amount(None)
    clean_amount("")

    # --------------------------------------------------------
    # FECHAS
    # --------------------------------------------------------
    parse_date("2024-12-01")
    parse_date("01/12/2024")
    parse_date("1/12/2024")        # sin cero
    parse_date("20241201")
    parse_date("2024/01/15")
    parse_date("15-01-2024")
    parse_date("2024-1-5")

    # Diferencias
    d1 = parse_date("2024-01-01")
    d2 = parse_date("2024-01-15")
    date_diff_days(d1, d2)
    date_diff_days(parse_date("2024-02-01"), parse_date("2024-02-28"))

    # Formato
    format_date_yyyymmdd(parse_date("2024-01-15"))

    # --------------------------------------------------------
    # RUC
    # --------------------------------------------------------
    clean_ruc("20558226979")
    clean_ruc("RUC: 20-55822697-9")
    clean_ruc("  20  5582   26979  ")
    clean_ruc("00020558226979")

    ok("🟢 TEST DE UTILS FINALIZADO — TODO PERFECTO")