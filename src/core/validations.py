# src/core/validations.py
from __future__ import annotations

# --- BOOTSTRAP RUTAS ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Logger
from src.core.logger import info, ok, warn, error


# ============================================================
#   VALIDATION ENGINE — PULSEFORGE ENTERPRISE CORE
# ============================================================
class ValidationError(Exception):
    """Errores de validación empresariales."""
    pass


# ------------------------------------------------------------
#  VALIDACIONES BÁSICAS
# ------------------------------------------------------------
def require_not_empty(value, field_name: str):
    """Validación para asegurar que un campo obligatorio tenga contenido."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        error(f"El campo '{field_name}' no puede estar vacío.")
        raise ValidationError(f"Campo obligatorio vacío: {field_name}")
    ok(f"Validación OK → Campo '{field_name}'")

    return value


def require_positive_number(value, field_name: str):
    """Valida que un número sea > 0."""
    try:
        num = float(value)
    except Exception:
        error(f"El campo '{field_name}' debe ser numérico.")
        raise ValidationError(f"Campo no numérico: {field_name}")

    if num <= 0:
        error(f"El campo '{field_name}' debe ser mayor a cero.")
        raise ValidationError(f"Valor no permitido en '{field_name}'")

    ok(f"Validación OK → Número positivo '{field_name}' = {num}")
    return num


def require_in_range(value, field_name: str, min_v: float, max_v: float):
    """Validación para rangos permitidos."""
    try:
        num = float(value)
    except Exception:
        error(f"'{field_name}' debe ser numérico.")
        raise ValidationError(f"Valor inválido → {field_name}")

    if not (min_v <= num <= max_v):
        error(f"'{field_name}' fuera de rango permitido → {num} (rango {min_v}-{max_v})")
        raise ValidationError(f"'{field_name}' fuera de rango")

    ok(f"Validación OK → '{field_name}' dentro del rango ({min_v}-{max_v})")
    return num


# ------------------------------------------------------------
#  VALIDACIONES ESPECÍFICAS DE NEGOCIO (PULSEFORGE)
# ------------------------------------------------------------
def validate_igv(value):
    """IGV peruano recomendado: 0.18 pero configurable."""
    info("Validando IGV…")
    return require_in_range(value, "IGV", 0.05, 0.40)  # rango abierto pero razonable


def validate_detraccion(value):
    """Detracción típica: 4%, 10%, etc."""
    info("Validando detracción…")
    return require_in_range(value, "Detracción", 0.01, 0.30)


def validate_tipo_cambio(value):
    """Tipo cambio aproximado: entre 2.5 y 6."""
    info("Validando tipo de cambio USD→PEN…")
    return require_in_range(value, "Tipo de Cambio", 2.0, 6.0)


def validate_periodo(periodo):
    """Ej.: 202401, 202402, 202312"""
    info("Validando periodo…")

    if not isinstance(periodo, (int, str)):
        error("El periodo debe ser numérico (YYYYMM).")
        raise ValidationError("Periodo inválido.")

    p = str(periodo)
    if len(p) != 6:
        error("Periodo incorrecto. Debe tener 6 dígitos (YYYYMM).")
        raise ValidationError("Formato periodo inválido.")

    year = int(p[:4])
    month = int(p[4:])

    if not (1 <= month <= 12):
        error("El mes del periodo no es válido.")
        raise ValidationError("Periodo inválido.")

    ok(f"Validación OK → Periodo {p}")
    return p


# ------------------------------------------------------------
#  TEST DEL MÓDULO
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n============================================")
    print("🔵  PULSEFORGE · VALIDATION ENGINE TEST")
    print("============================================\n")

    try:
        validate_igv(0.18)
        validate_detraccion(0.04)
        validate_tipo_cambio(3.5)
        validate_periodo("202401")

        require_not_empty("Juan", "Responsable")
        require_positive_number(123, "Monto")

        ok("Todas las validaciones se ejecutaron correctamente.")

    except ValidationError as e:
        error(f"❌ Error de validación: {e}")

    print("\n🟢 TEST FINALIZADO\n")
