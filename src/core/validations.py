# src/core/validations.py
from __future__ import annotations

# -------------------------
# Bootstrap interno
# -------------------------
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from typing import Dict, Any, List
from pathlib import Path
import sqlite3

from src.core.utils import parse_date, clean_ruc, clean_amount, normalize_text


# =====================================================
#  EXCEPCIONES
# =====================================================
class ValidationError(Exception):
    """Errores críticos → detienen el sistema."""
    pass


# =====================================================
#  VALIDACIONES CRÍTICAS (CONFIG)
# =====================================================
def validate_numeric_range(name: str, value: float, min_v: float, max_v: float) -> float:
    try:
        v = float(value)
    except:
        raise ValidationError(f"{name} inválido: no es número → {value}")

    if not (min_v <= v <= max_v):
        raise ValidationError(f"{name} fuera de rango permitido → {value}")

    return v


def validate_igv(v: float) -> float:
    return validate_numeric_range("IGV", v, 0.05, 0.40)


def validate_detraccion(v: float) -> float:
    return validate_numeric_range("Detracción", v, 0.01, 0.30)


def validate_tipo_cambio(v: float) -> float:
    return validate_numeric_range("Tipo de cambio", v, 2.00, 6.00)


def validate_path_exists(path: str, label: str):
    if not path or not Path(path).exists():
        raise ValidationError(f"{label} no existe → {path}")
    return path


# =====================================================
#  VALIDACIÓN DE BASES DE DATOS
# =====================================================
def validate_database_can_open(path: str):
    try:
        conn = sqlite3.connect(path)
        conn.close()
    except Exception as e:
        raise ValidationError(f"No se puede abrir la base de datos: {path} → {e}")


def validate_table_exists(path: str, table: str):
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        exists = cur.fetchone()

        conn.close()

        if not exists:
            raise ValidationError(f"Tabla no encontrada en DB ({path}) → '{table}'")

    except Exception as e:
        raise ValidationError(f"Error validando tabla '{table}' en DB '{path}': {e}")


# =====================================================
#  VALIDACIÓN DE SETTINGS
# =====================================================
def validate_settings_tables(settings: Dict[str, Any], db_path: str):
    tablas = settings.get("tablas", {})
    bancos = settings.get("tablas_bancos", {})
    tabla_unica = settings.get("tabla_movimientos_unica", None)

    # Validar tablas contables/financieras
    for alias, tabla in tablas.items():
        if not tabla.strip():
            raise ValidationError(f"La tabla '{alias}' está vacía en settings.json")

        validate_table_exists(db_path, tabla)

    # Validar bancos
    for alias, tabla in bancos.items():
        if not tabla.strip():
            raise ValidationError(f"La tabla de banco '{alias}' está vacía en settings.json")

        validate_table_exists(db_path, tabla)

    # Validar tabla única (opcional)
    if tabla_unica:
        validate_table_exists(db_path, tabla_unica)


def validate_settings_columns(settings: Dict[str, Any]):
    """
    Verifica que las columnas estén bien configuradas en formato:
    "campo": ["col1", "col2", ...]
    No valida contra DB (porque las columnas pueden variar dinámicamente).
    """
    for section_name in ("columnas_facturas", "columnas_bancos"):
        section = settings.get(section_name, {})
        if not isinstance(section, dict):
            raise ValidationError(f"{section_name} debe ser un objeto con mapeos de columnas")

        for key, lista in section.items():
            if not isinstance(lista, list) or not lista:
                raise ValidationError(
                    f"'{key}' en {section_name} debe ser una lista de posibles columnas"
                )


# =====================================================
#  VALIDACIONES SUAVES (DATOS)
# =====================================================
def is_empty(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def validate_required(value) -> bool:
    return not is_empty(value)


def validate_positive(value) -> bool:
    try:
        return float(value) >= 0
    except:
        return False


def validate_date(value) -> bool:
    try:
        return parse_date(value) is not None
    except:
        return False


def validate_ruc(value) -> bool:
    r = clean_ruc(value)
    return 8 <= len(r) <= 11   # DNI, RUC, CE universales


def validate_amount(value) -> bool:
    try:
        clean_amount(value)
        return True
    except:
        return False


def validate_text(value) -> bool:
    try:
        normalize_text(value)
        return True
    except:
        return False


# =====================================================
#  VALIDACIÓN GLOBAL DEL SISTEMA
# =====================================================
def validate_system_config(cfg, settings):
    """
    Validación completa antes de ejecutar PulseForge.
    Detiene todo si la configuración está mal.
    """
    # Validación de rutas/DBs
    validate_path_exists(cfg.db_source, "DB ORIGEN")
    validate_path_exists(cfg.db_pulseforge, "DB DESTINO")

    validate_database_can_open(cfg.db_source)
    validate_database_can_open(cfg.db_pulseforge)

    # Validación de parámetros contables
    validate_igv(cfg.igv)
    validate_detraccion(cfg.detraccion)
    validate_tipo_cambio(cfg.tipo_cambio)

    # Validación de settings
    validate_settings_columns(settings)
    validate_settings_tables(settings, cfg.db_source)
