# src/core/env_loader.py
from __future__ import annotations
import sys
from pathlib import Path
import os
import json
from dataclasses import dataclass, field
from typing import Any, Optional, Dict

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]     # C:/Proyectos/PulseForge
CONFIG_DIR = ROOT / "config"                   # settings.json & constants.json
ENV_FILE = ROOT / ".env"                       # Archivo .env en raíz del proyecto

if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Logging corporativo
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error


# ============================================================
#  EXCEPCIÓN
# ============================================================
class EnvConfigError(Exception):
    pass


# ============================================================
#  CARGAR .ENV MANUALMENTE
# ============================================================
def _load_env():
    if not ENV_FILE.exists():
        warn(f".env no encontrado → {ENV_FILE}")
        return

    try:
        with ENV_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()
    except Exception as e:
        error(f"Error cargando .env: {e}")


# ============================================================
#  CACHES
# ============================================================
_JSON_CACHE: Dict[str, Dict[str, Any]] = {}
_CONFIG_CACHE = None


# ============================================================
#  LECTOR JSON SEGURO
# ============================================================
def _load_json(filename: str) -> Dict[str, Any]:
    path = CONFIG_DIR / filename

    if not path.exists():
        warn(f"{filename} no encontrado → {path}")
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        error(f"Error leyendo {filename}: {e}")
        return {}


# ============================================================
#  MODELOS DE CONFIGURACIÓN
# ============================================================
@dataclass
class ParametrosContables:
    detraccion: float = 0.0
    igv: float = 0.0
    dias_tolerancia_pago: int = 0
    monto_variacion: float = 0.0
    tipo_cambio_usd_pen: float = 0.0


@dataclass
class PulseForgeConfig:
    env: str = "development"
    run_mode: str = "incremental"

    # Paths
    data_dir: str = "./data"
    logs_dir: str = "./logs"
    exports_dir: str = "./data/exports"
    temp_dir: str = "./data/temp"

    # DBs
    db_source: str = ""
    db_destino: str = ""
    db_new: str = ""

    # Param contables
    parametros: ParametrosContables = field(default_factory=ParametrosContables)

    # Bancos / DataTables
    tablas: Dict[str, str] = field(default_factory=dict)
    tablas_bancos: Dict[str, str] = field(default_factory=dict)
    tabla_movimientos_unica: str = ""

    # Columnas
    columnas_bancos: Dict[str, list] = field(default_factory=dict)
    columnas_facturas: Dict[str, list] = field(default_factory=dict)

    # Cuentas empresa
    cuentas_empresa: list = field(default_factory=list)
    cuenta_detraccion: str = ""


# ============================================================
#  CARGA PRINCIPAL DE CONFIGURACIÓN
# ============================================================
def get_config() -> PulseForgeConfig:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    info("Cargando configuración PulseForge 2025...")

    # Cargar .env ANTES DE LEER os.getenv()
    _load_env()

    settings = _load_json("settings.json")
    constants = _load_json("constants.json")

    # -----------------------------
    # Extraer parámetros contables
    # -----------------------------
    parametros_raw = settings.get("parametros_contables", {})

    pc = ParametrosContables(
        detraccion=parametros_raw.get("detraccion", 0.0),
        igv=parametros_raw.get("igv", 0.0),
        dias_tolerancia_pago=parametros_raw.get("dias_tolerancia_pago", 0),
        monto_variacion=parametros_raw.get("monto_variacion", 0.0),
        tipo_cambio_usd_pen=parametros_raw.get("tipo_cambio_usd_pen", 0.0),
    )

    if pc.igv <= 0 or pc.igv > 0.5:
        raise EnvConfigError(f"IGV fuera de rango permitido → {pc.igv}")

    if pc.detraccion < 0 or pc.detraccion > 1:
        raise EnvConfigError(f"Detracción fuera de rango → {pc.detraccion}")

    # -----------------------------
    # Rutas DB desde .env
    # -----------------------------
    db_source = os.getenv("PULSEFORGE_SOURCE_DB", "").strip()
    db_destino = os.getenv("PULSEFORGE_DB_PATH", "").strip()
    db_new = os.getenv("PULSEFORGE_NEWDB_PATH", "").strip()

    if not db_source:
        raise EnvConfigError("DB ORIGEN no existe → ruta vacía en .env")

    # -----------------------------
    # Construcción final de config
    # -----------------------------
    cfg = PulseForgeConfig(
    env=settings.get("app", {}).get("env", "development"),
    run_mode=settings.get("app", {}).get("run_mode", "incremental"),

    data_dir=settings.get("paths", {}).get("data_dir", "./data"),
    logs_dir=settings.get("paths", {}).get("logs_dir", "./logs"),
    exports_dir=settings.get("paths", {}).get("exports_dir", "./data/exports"),
    temp_dir=settings.get("paths", {}).get("temp_dir", "./data/temp"),

    db_source=db_source,
    db_destino=db_destino,
    db_new=db_new,

    parametros=pc,

    tablas=settings.get("tablas", {}),
    tablas_bancos=settings.get("tablas_bancos", {}),
    tabla_movimientos_unica=settings.get("tabla_movimientos_unica", ""),

    columnas_bancos=settings.get("columnas_bancos", {}),
    columnas_facturas=settings.get("columnas_facturas", {}),

    cuentas_empresa=settings.get("cuentas_bancarias", {}).get("cuentas_empresa", []),
    cuenta_detraccion=settings.get("cuentas_bancarias", {}).get("cuenta_detraccion", ""),
    )   

    # 👉 ALIAS PROFESIONAL PARA EL TEST
    cfg.db_pulseforge = cfg.db_destino
    cfg.igv = cfg.parametros.igv
    cfg.detraccion = cfg.parametros.detraccion
    cfg.tipo_cambio = cfg.parametros.tipo_cambio_usd_pen


    _CONFIG_CACHE = cfg
    ok("Configuración cargada correctamente.")
    return cfg

# ============================================================
#  GET ENV
# ============================================================
def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key, default)
