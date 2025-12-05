# src/core/env_loader.py
from __future__ import annotations

# --- BOOTSTRAP PARA QUE FUNCIONE DESDE CUALQUIER RUTA ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# ---------------------------------------------------------

import os
from dataclasses import dataclass
from typing import Any, Optional

# ---------------------------
#  LOGGING CORPORATIVO REAL
# ---------------------------
from src.core.logger import info, ok, warn, error


# ---------------------------
#  ERRORES ESPECÍFICOS DE ENV
# ---------------------------
class EnvConfigError(Exception):
    pass


_ENV_LOADED = False


# ---------------------------
#  CARGA DEL ARCHIVO .env
# ---------------------------
def _load_env_file(dotenv_path: Optional[Path] = None) -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    if dotenv_path is None:
        dotenv_path = Path(__file__).resolve().parents[2] / ".env"

    if not dotenv_path.is_file():
        warn(f"No se encontró archivo .env en: {dotenv_path}")
        _ENV_LOADED = True
        return

    try:
        with dotenv_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)

        ok(f".env cargado correctamente desde: {dotenv_path}")
    except Exception as e:
        error(f"Error cargando .env: {e}")
        raise EnvConfigError(f"Error cargando .env: {e}")

    _ENV_LOADED = True


# ---------------------------
#  CASTEOS Y VALIDACIONES
# ---------------------------
def _cast_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _cast_value(raw: str, t: type) -> Any:
    if t is bool:
        return _cast_bool(raw)
    if t is int:
        return int(raw)
    if t is float:
        return float(raw.replace(",", "."))
    return raw


# ---------------------------
#  LECTOR PRINCIPAL GET_ENV
# ---------------------------
def get_env(
    key: str,
    *,
    required: bool = False,
    default: Any = None,
    cast_type: type = str,
) -> Any:

    _load_env_file()
    raw = os.environ.get(key)

    if raw is None or raw == "":
        if required:
            error(f"Variable requerida faltante: {key}")
            raise EnvConfigError(f"Variable requerida faltante: {key}")
        warn(f"Variable opcional no encontrada: {key}, usando default: {default}")
        return default

    try:
        return _cast_value(raw, cast_type)
    except Exception as e:
        error(f"Error casteando {key}='{raw}' → {cast_type.__name__}")
        raise EnvConfigError(f"Error casteando {key}: {e}")


# ---------------------------
#  MODELO CENTRAL DE CONFIG
# ---------------------------
@dataclass
class PulseForgeConfig:

    # BASES DE DATOS
    db_type: str
    db_source_path: str         # <-- DataPulse
    db_path: str                # <-- PulseForge actual
    newdb_path: str             # <-- PulseForge destino

    # CONTABILIDAD
    detraccion_porcentaje: float
    igv: float
    days_tolerance_pago: int
    monto_variacion: float
    tipo_cambio_usd_pen: float

    # BANCOS
    cuenta_empresa: Optional[str]
    cuenta_detraccion: Optional[str]

    # IA
    activar_ia: bool
    api_gemini_key: Optional[str]

    # OTROS
    modo_debug: bool


_CONFIG_CACHE: Optional[PulseForgeConfig] = None


# ---------------------------
#  CARGA PRINCIPAL DE CONFIG
# ---------------------------
def load_pulseforge_config(force_reload: bool = False) -> PulseForgeConfig:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None and not force_reload:
        return _CONFIG_CACHE

    info("Cargando configuración de PulseForge…")

    db_type = get_env("PULSEFORGE_DB_TYPE", required=True).lower()
    if db_type not in ("sqlite", "postgres", "mysql"):
        raise EnvConfigError(f"PULSEFORGE_DB_TYPE inválido: {db_type}")

    cfg = PulseForgeConfig(

        # BASES DE DATOS
        db_type=db_type,
        db_source_path=get_env("PULSEFORGE_SOURCE_DB", required=True),
        db_path=get_env("PULSEFORGE_DB_PATH", required=True),
        newdb_path=get_env("PULSEFORGE_NEWDB_PATH", required=True),

        # CONTABILIDAD
        detraccion_porcentaje=get_env("DETRACCION_PORCENTAJE", required=True, cast_type=float),
        igv=get_env("IGV", required=True, cast_type=float),
        days_tolerance_pago=get_env("DAYS_TOLERANCE_PAGO", required=True, cast_type=int),
        monto_variacion=get_env("MONTO_VARIACION", required=True, cast_type=float),
        tipo_cambio_usd_pen=get_env("TIPO_CAMBIO_USD_PEN", default=3.80, cast_type=float),

        # BANCOS
        cuenta_empresa=get_env("CUENTA_EMPRESA", default="") or None,
        cuenta_detraccion=get_env("CUENTA_DETRACCION", default="") or None,

        # IA
        activar_ia=get_env("ACTIVAR_IA", default=True, cast_type=bool),
        api_gemini_key=get_env("API_GEMINI_KEY", default="") or None,

        # OTROS
        modo_debug=get_env("MODO_DEBUG", default=False, cast_type=bool),
    )

    ok("Configuración cargada correctamente.")
    _CONFIG_CACHE = cfg
    return cfg


def get_config() -> PulseForgeConfig:
    return load_pulseforge_config()


# ---------------------------
#  TEST INTERNO
# ---------------------------
if __name__ == "__main__":
    cfg = load_pulseforge_config(force_reload=True)

    info("===== CONFIGURACIÓN CARGADA =====")
    for field, value in cfg.__dict__.items():
        print(f"{field}: {value}")
