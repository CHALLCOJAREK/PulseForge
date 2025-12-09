# src/core/env_loader.py
from __future__ import annotations
import sys
from pathlib import Path
import os
import json
from dataclasses import dataclass, field
from typing import Any, Optional, Dict

# ---------------------------------------------------------
# Bootstrap interno
# ---------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ---------------------------------------------------------
# Logging corporativo
# ---------------------------------------------------------
from src.core.logger import info, ok, warn, error


# ---------------------------------------------------------
# Excepciones
# ---------------------------------------------------------
class EnvConfigError(Exception):
    pass


_ENV_LOADED = False
_JSON_CACHE: Dict[str, Dict[str, Any]] = {}
_CONFIG_CACHE = None


# ---------------------------------------------------------
# Lectura de JSONs seguros
# ---------------------------------------------------------
def _load_json(path: Path, name: str) -> Dict[str, Any]:
    if not path.exists():
        warn(f"{name} no encontrado → {path}")
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            ok(f"{name} cargado.")
            return data
    except Exception as e:
        error(f"Error leyendo {name}: {e}")
        return {}


# ---------------------------------------------------------
# Cargar archivo .env
# ---------------------------------------------------------
def _load_env_file(path: Optional[Path] = None):
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    path = path or (ROOT / ".env")

    if not path.exists():
        warn(f".env no encontrado → {path}")
        _ENV_LOADED = True
        return

    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "=" not in line or line.startswith("#"):
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

        ok(".env cargado.")
    except Exception as e:
        error(f"Error cargando .env: {e}")
        raise EnvConfigError(e)

    _ENV_LOADED = True


# ---------------------------------------------------------
# Casting universal
# ---------------------------------------------------------
def _cast(raw: Any, t: type) -> Any:
    if raw is None:
        return None
    try:
        if t is bool:
            return str(raw).lower() in ("1", "true", "yes", "y", "on")
        if t is int:
            return int(raw)
        if t is float:
            return float(str(raw).replace(",", "."))
        return raw
    except Exception:
        warn(f"No se pudo castear '{raw}' como {t.__name__}")
        return raw


# ---------------------------------------------------------
# Resolución jerárquica
#         env → settings.json → constants.json
# ---------------------------------------------------------
def _get(key: str, default: Any = None):
    return (
        os.environ.get(key)
        or _JSON_CACHE.get("settings", {}).get(key)
        or _JSON_CACHE.get("constants", {}).get(key)
        or default
    )


def get_config_value(key: str, *, cast: type = str, default: Any = None):
    raw = _get(key, default)
    return _cast(raw, cast)


# ---------------------------------------------------------
# MODELO PRINCIPAL DE CONFIGURACIÓN
# ---------------------------------------------------------
@dataclass
class PulseForgeConfig:

    # BD config
    db_source: str
    db_pulseforge: str
    db_new: str

    # Reglas contables
    igv: float
    detraccion: float
    variacion_monto: float
    tolerancia_dias: int
    tipo_cambio: float

    # Config dinámico
    tablas: Dict[str, Any] = field(default_factory=dict)
    bancos: Dict[str, Any] = field(default_factory=dict)
    tabla_movimientos_unica: Optional[str] = None

    # Columnas del settings.json
    columnas_facturas: Dict[str, Any] = field(default_factory=dict)
    columnas_bancos: Dict[str, Any] = field(default_factory=dict)

    # IA y Modo debug
    activar_ia: bool = False
    gemini_key: Optional[str] = None
    modo_debug: bool = False


# ---------------------------------------------------------
# CARGA PRINCIPAL
# ---------------------------------------------------------
def load_pulseforge_config(force_reload: bool = False) -> PulseForgeConfig:
    global _CONFIG_CACHE, _JSON_CACHE

    if _CONFIG_CACHE is not None and not force_reload:
        return _CONFIG_CACHE

    info("Cargando configuración universal...")

    _load_env_file()

    _JSON_CACHE["settings"] = _load_json(ROOT / "config" / "settings.json", "settings.json")
    _JSON_CACHE["constants"] = _load_json(ROOT / "config" / "constants.json", "constants.json")

    # --- Tablas principales (si falta alguna NO revienta)
    tablas_raw = _JSON_CACHE["settings"].get("tablas", {})
    tablas = {k: v for k, v in tablas_raw.items() if isinstance(v, str) and v.strip()}

    # --- Tabla única de movimientos (opcional)
    tabla_unica = _JSON_CACHE["settings"].get("tabla_movimientos_unica", None)
    if tabla_unica is not None and not isinstance(tabla_unica, str):
        tabla_unica = None

    # --- Tablas de bancos (puede haber 0, 1 o muchas)
    bancos_raw = _JSON_CACHE["settings"].get("tablas_bancos", {})
    bancos = {
        alias: tabla
        for alias, tabla in bancos_raw.items()
        if isinstance(tabla, str) and tabla.strip()
    }

    cfg = PulseForgeConfig(
        db_source=get_config_value("PULSEFORGE_SOURCE_DB", cast=str),
        db_pulseforge=get_config_value("PULSEFORGE_DB_PATH", cast=str),
        db_new=get_config_value("PULSEFORGE_NEWDB_PATH", cast=str),

        # Reglas
        igv=get_config_value("IGV", cast=float),
        detraccion=get_config_value("DETRACCION_PORCENTAJE", cast=float),
        variacion_monto=get_config_value("MONTO_VARIACION", cast=float, default=1.0),
        tolerancia_dias=get_config_value("DAYS_TOLERANCE_PAGO", cast=int, default=3),
        tipo_cambio=get_config_value("TIPO_CAMBIO_USD_PEN", cast=float, default=3.80),

        # Config dinámico
        tablas=tablas,
        bancos=bancos,
        tabla_movimientos_unica=tabla_unica,

        # Columnas del settings.json
        columnas_facturas=_JSON_CACHE["settings"].get("columnas_facturas", {}),
        columnas_bancos=_JSON_CACHE["settings"].get("columnas_bancos", {}),

        activar_ia=get_config_value("ACTIVAR_IA", cast=bool, default=False),
        gemini_key=get_config_value("API_GEMINI_KEY", cast=str, default=None),
        modo_debug=get_config_value("MODO_DEBUG", cast=bool, default=False),
    )

    ok("Configuración universal cargada.")
    _CONFIG_CACHE = cfg
    return cfg


def get_config() -> PulseForgeConfig:
    return load_pulseforge_config()
