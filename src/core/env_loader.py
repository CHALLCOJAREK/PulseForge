# src/core/env_loader.py

import os
from dotenv import load_dotenv

def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class EnvConfig:

    def __init__(self):
        info("Cargando configuración desde archivo .env...")

        # Cargar siempre desde la RAÍZ del proyecto
        ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        ENV_PATH = os.path.join(ROOT, ".env")

        if not os.path.exists(ENV_PATH):
            error(f"No se encontró archivo .env en: {ENV_PATH}")
            raise FileNotFoundError(".env no encontrado")

        load_dotenv(ENV_PATH)

        # ========================
        # BASE DE DATOS
        # ========================
        self.DB_TYPE        = self._get("PULSEFORGE_DB_TYPE")
        self.DB_PATH_ORIGEN = self._get("PULSEFORGE_DB_PATH")
        self.DB_PATH_NUEVA  = self._get("PULSEFORGE_NEWDB_PATH")

        # Alias de compatibilidad (MUY IMPORTANTE)
        self.PULSEFORGE_NEWDB_PATH = self.DB_PATH_NUEVA

        # ========================
        # FINANZAS
        # ========================
        self.DETRACCION_PORCENTAJE = self._get_float("DETRACCION_PORCENTAJE")
        self.IGV                   = self._get_float("IGV")

        # ========================
        # IA
        # ========================
        self.API_GEMINI_KEY = self._get("API_GEMINI_KEY")

        # ========================
        # REGLAS
        # ========================
        self.DAYS_TOLERANCE_PAGO = self._get_int("DAYS_TOLERANCE_PAGO")
        self.MONTO_VARIACION     = self._get_float("MONTO_VARIACION")

        # ========================
        # BANCOS
        # ========================
        self.CUENTA_EMPRESA    = self._get("CUENTA_EMPRESA")
        self.CUENTA_DETRACCION = self._get("CUENTA_DETRACCION")

        # ========================
        # FLAGS
        # ========================
        self.ACTIVAR_IA = self._get_bool("ACTIVAR_IA")
        self.MODO_DEBUG = self._get_bool("MODO_DEBUG")

        ok("Variables de entorno cargadas correctamente. PulseForge listo. 🚀")


    # ---------- VALIDADORES ----------
    def _get(self, key):
        v = os.getenv(key)
        if v is None:
            error(f"Variable faltante: {key}")
            raise ValueError(f"Falta variable: {key}")
        info(f"Variable cargada: {key} = {v}")
        return v

    def _get_int(self, key): return int(self._get(key))
    def _get_float(self, key): return float(self._get(key))

    def _get_bool(self, key):
        v = self._get(key).lower()
        if v in ("true","1","yes","si"): return True
        if v in ("false","0","no"): return False
        raise ValueError(f"{key} debe ser booleano")

    # ---------- API para .get() ----------
    def get(self, key, default=None):
        return getattr(self, key, default)


# SINGLETON
_env = None
def get_env():
    global _env
    if _env is None:
        info("Inicializando configuración global PulseForge...")
        _env = EnvConfig()
    return _env
