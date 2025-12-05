# src/core/validations.py
from __future__ import annotations

# --- BOOTSTRAP RUTAS ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Validaciones silenciosas
class ValidationError(Exception):
    """Errores críticos del sistema (solo config)."""
    pass


# ============================================================
#   VALIDACIONES PARA CONFIGURACIÓN (SE USAN 1 VEZ)
# ============================================================

def validate_igv(value: float) -> float:
    """Valida IGV solo al iniciar el sistema."""
    if not (0.05 <= float(value) <= 0.40):
        raise ValidationError(f"IGV inválido: {value}")
    return float(value)


def validate_detraccion(value: float) -> float:
    if not (0.01 <= float(value) <= 0.30):
        raise ValidationError(f"Detracción inválida: {value}")
    return float(value)


def validate_tipo_cambio(value: float) -> float:
    if not (2.0 <= float(value) <= 6.0):
        raise ValidationError(f"Tipo de cambio inválido: {value}")
    return float(value)


# ============================================================
#   VALIDACIONES SILENCIOSAS PARA DATOS (SE USAN MILES DE VECES)
# ============================================================

def is_empty(value) -> bool:
    """Retorna True si el valor es vacío."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def validate_required(value) -> bool:
    """Valida campo requerido (no lanza excepción)."""
    return not is_empty(value)


def validate_positive(value) -> bool:
    """Valida que sea número positivo. 0 también se permite (caso bancos)."""
    try:
        return float(value) >= 0
    except:
        return False


def validate_date(value) -> bool:
    """Valida formato de fecha sin romper el sistema."""
    from src.core.utils import parse_date

    try:
        parse_date(value)
        return True
    except:
        return False


def validate_ruc(value) -> bool:
    """Valida que el RUC tenga al menos 8 dígitos."""
    from src.core.utils import clean_ruc
    r = clean_ruc(value)
    return len(r) >= 8
