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


# -------------------------
# Excepción para configuración
# -------------------------
class ValidationError(Exception):
    pass


# -------------------------
# Validaciones únicas de configuración
# -------------------------
def validate_igv(value: float) -> float:
    v = float(value)
    if not (0.05 <= v <= 0.40):
        raise ValidationError(f"IGV inválido: {value}")
    return v


def validate_detraccion(value: float) -> float:
    v = float(value)
    if not (0.01 <= v <= 0.30):
        raise ValidationError(f"Detracción inválida: {value}")
    return v


def validate_tipo_cambio(value: float) -> float:
    v = float(value)
    if not (2.0 <= v <= 6.0):
        raise ValidationError(f"Tipo de cambio inválido: {value}")
    return v


# -------------------------
# Validaciones silenciosas de datos
# -------------------------
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
    from src.core.utils import parse_date
    try:
        parse_date(value)
        return True
    except:
        return False


def validate_ruc(value) -> bool:
    from src.core.utils import clean_ruc
    return len(clean_ruc(value)) >= 8
