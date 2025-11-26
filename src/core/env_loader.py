import os
from dotenv import load_dotenv

# Estilos de prints amigables
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class EnvConfig:
    """
    Carga y valida todas las variables del archivo .env.
    Expone todo como atributos, con validaciones y mensajes claros.
    """

    def __init__(self):
        info("Cargando configuración desde archivo .env...")

        # Cargar archivo .env
        load_dotenv()

        # ============================
        #  BASES DE DATOS
        # ============================
        self.DB_TYPE = self._get("PULSEFORGE_DB_TYPE")
        self.DB_PATH_ORIGEN = self._get("PULSEFORGE_DB_PATH")
        self.DB_PATH_NUEVA = self._get("PULSEFORGE_NEWDB_PATH")

        # ============================
        #  PARÁMETROS FINANCIEROS
        # ============================
        self.DETRACCION_PORCENTAJE = self._get_float("DETRACCION_PORCENTAJE")
        self.IGV = self._get_float("IGV")

        # ============================
        #  IA (GEMINI)
        # ============================
        self.API_GEMINI_KEY = self._get("API_GEMINI_KEY")

        # ============================
        #  REGLAS DE BÚSQUEDA
        # ============================
        self.DAYS_TOLERANCE_PAGO = self._get_int("DAYS_TOLERANCE_PAGO")
        self.MONTO_VARIACION = self._get_float("MONTO_VARIACION")

        # ============================
        #  CUENTAS / BANCOS
        # ============================
        self.CUENTA_EMPRESA = self._get("CUENTA_EMPRESA")
        self.CUENTA_DETRACCION = self._get("CUENTA_DETRACCION")

        # ============================
        #  FLAGS DEL SISTEMA
        # ============================
        self.ACTIVAR_IA = self._get_bool("ACTIVAR_IA")
        self.MODO_DEBUG = self._get_bool("MODO_DEBUG")

        ok("Variables de entorno cargadas correctamente. PulseForge está listo para conectarse. 🚀")


    # ============================================================
    #  GETTERS PRIVADOS — VALIDACIÓN DE VARIABLES
    # ============================================================

    def _get(self, var_name: str) -> str:
        """Obtiene una variable del .env o lanza error si no existe."""
        value = os.getenv(var_name)
        if value is None:
            error(f"Variable faltante en .env: {var_name}")
            raise ValueError(f"[ENV ERROR] Falta la variable: {var_name}")
        info(f"Variable cargada: {var_name} = {value}")
        return value


    def _get_int(self, var_name: str) -> int:
        """Obtiene una variable como entero."""
        value = self._get(var_name)
        try:
            return int(value)
        except ValueError:
            error(f"'{var_name}' debe ser un número entero.")
            raise


    def _get_float(self, var_name: str) -> float:
        """Obtiene una variable como float."""
        value = self._get(var_name)
        try:
            return float(value)
        except ValueError:
            error(f"'{var_name}' debe ser un número decimal.")
            raise


    def _get_bool(self, var_name: str) -> bool:
        """Convierte textos tipo true/false en booleano."""
        value = self._get(var_name).lower()
        if value in ["true", "1", "yes", "si"]:
            return True
        if value in ["false", "0", "no"]:
            return False
        error(f"'{var_name}' debe ser true/false o 1/0.")
        raise ValueError(f"[ENV ERROR] '{var_name}' inválido como booleano.")



# ============================================================
#  SINGLETON — Instancia única para todo el sistema
# ============================================================

_env_instance = None

def get_env():
    """
    Devuelve una única instancia de EnvConfig.
    Para evitar recargar .env múltiples veces.
    """
    global _env_instance
    if _env_instance is None:
        info("Inicializando configuración global de PulseForge...")
        _env_instance = EnvConfig()
    return _env_instance

if __name__ == "__main__":
    print("🔧 Ejecutando prueba directamente de env_loader.py")
    env = EnvConfig()
