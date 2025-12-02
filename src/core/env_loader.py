# src/core/env_loader.py
from __future__ import annotations

# --- BOOTSTRAP PARA EJECUTAR DESDE CUALQUIER PARTE ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# ------------------------------------------------------

import os
from dataclasses import dataclass
from typing import Any, Optional


# -----------------------------
#  LOGGING CORPORATIVO
# -----------------------------
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


# -----------------------------
#  ERRORES DE CONFIGURACIÓN
# -----------------------------
class EnvConfigError(Exception):
    pass


_ENV_LOADED = False


# -----------------------------
#  CARGA DEL ARCHIVO .env
# -----------------------------
def _load_env_file(dotenv_path: Optional[Path] = None) -> None:
    """Carga las variables del archivo .env sin dependencias externas."""
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


# -----------------------------
#  CASTEOS SEGUROS
# -----------------------------
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


# -----------------------------
#  LECTOR SEMIESTRICTO DEL ENV
# -----------------------------
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


# -----------------------------
#  MODELO DE CONFIGURACIÓN
# -----------------------------
@dataclass
class PulseForgeConfig:
    # Críticas
    db_type: str
    db_path: str
    newdb_path: str
    detraccion_porcentaje: float
    igv: float
    days_tolerance_pago: int
    monto_variacion: float

    # Opcionales
    tipo_cambio_usd_pen: float
    cuenta_empresa: Optional[str]
    cuenta_detraccion: Optional[str]
    activar_ia: bool
    modo_debug: bool
    api_gemini_key: Optional[str]


_CONFIG_CACHE: Optional[PulseForgeConfig] = None


# -----------------------------
#  CARGA PRINCIPAL DE CONFIG
# -----------------------------
def load_pulseforge_config(force_reload: bool = False) -> PulseForgeConfig:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None and not force_reload:
        return _CONFIG_CACHE

    info("Cargando configuración de PulseForge…")

    db_type = get_env("PULSEFORGE_DB_TYPE", required=True).lower()
    if db_type not in ("sqlite", "mysql", "postgres"):
        raise EnvConfigError(f"PULSEFORGE_DB_TYPE inválido: {db_type}")

    cfg = PulseForgeConfig(
        # CRÍTICAS
        db_type=db_type,
        db_path=get_env("PULSEFORGE_DB_PATH", required=True),
        newdb_path=get_env("PULSEFORGE_NEWDB_PATH", required=True),
        detraccion_porcentaje=get_env("DETRACCION_PORCENTAJE", required=True, cast_type=float),
        igv=get_env("IGV", required=True, cast_type=float),
        days_tolerance_pago=get_env("DAYS_TOLERANCE_PAGO", required=True, cast_type=int),
        monto_variacion=get_env("MONTO_VARIACION", required=True, cast_type=float),

        # OPCIONALES
        tipo_cambio_usd_pen=get_env("TIPO_CAMBIO_USD_PEN", default=3.80, cast_type=float),
        cuenta_empresa=get_env("CUENTA_EMPRESA", default="") or None,
        cuenta_detraccion=get_env("CUENTA_DETRACCION", default="") or None,
        activar_ia=get_env("ACTIVAR_IA", default=True, cast_type=bool),
        modo_debug=get_env("MODO_DEBUG", default=False, cast_type=bool),
        api_gemini_key=get_env("API_GEMINI_KEY", default="") or None,
    )

    ok("Configuración cargada correctamente.")
    _CONFIG_CACHE = cfg
    return cfg


def get_config() -> PulseForgeConfig:
    return load_pulseforge_config()


# -----------------------------
#  BLOQUE DE PRUEBA PROFESIONAL
# -----------------------------
if __name__ == "__main__":
    info("Iniciando prueba de env_loader.py…")
    try:
        cfg = load_pulseforge_config()
        ok("PRUEBA EXITOSA — Configuración cargada.\n")

        print("====================================================")
        print("        ⚙️  CONFIGURACIÓN PULSEFORGE (ENV)")
        print("====================================================\n")

        print("🔶 VARIABLES CRÍTICAS")
        print(f"   • DB Type                : {cfg.db_type}")
        print(f"   • Ruta DB origen         : {cfg.db_path}")
        print(f"   • Ruta DB nueva          : {cfg.newdb_path}")
        print(f"   • IGV                    : {cfg.igv}")
        print(f"   • % Detracción           : {cfg.detraccion_porcentaje}")
        print(f"   • Tolerancia Fecha (días): {cfg.days_tolerance_pago}")
        print(f"   • Tolerancia Monto       : {cfg.monto_variacion}\n")

        print("🔷 VARIABLES OPCIONALES")
        print(f"   • TC USD→PEN             : {cfg.tipo_cambio_usd_pen}")
        print(f"   • Cuenta Empresa         : {cfg.cuenta_empresa}")
        print(f"   • Cuenta Detracción      : {cfg.cuenta_detraccion}")
        print(f"   • IA Activada            : {cfg.activar_ia}")
        print(f"   • Modo Debug             : {cfg.modo_debug}")
        print(f"   • Gemini API Key         : {'✔ OK' if cfg.api_gemini_key else '— Sin clave'}\n")

        print("====================================================")
        print("        🟢 CONFIGURACIÓN COMPLETA")
        print("====================================================\n")

    except EnvConfigError as e:
        error("ERROR EN PRUEBA DE ENV LOADER:")
        error(str(e))
