# src/extractors/clients_extractor.py
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from sqlalchemy import text
from src.core.db import get_db
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class ClientsExtractor:
    """
    Extrae razón social según el RUC desde la tabla 'excel_1_clientes_proveedores'.
    Detecta nombres de columnas aunque vengan mal escritas.
    Ignora columnas innecesarias.
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de clientes...")

        # ============================
        #  Cargar settings.json
        # ============================
        from json import load

        # Ruta absoluta del archivo settings.json
        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("Archivo settings.json no encontrado.")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        ok("Extractor de clientes listo para trabajar.")



    # =======================================================
    #   CARGAR TABLA COMPLETA DESDE SQL
    # =======================================================
    def _load_clients_table(self):
        """Carga la tabla excel_1_clientes_proveedores completa desde SQL."""

        tabla = self.settings["tablas"]["clientes"]
        info(f"Cargando tabla de clientes: {tabla}")

        query = text(f"SELECT * FROM {tabla}")

        try:
            df = pd.read_sql(query, self.db.engine_origen)
        except Exception as e:
            error(f"Error leyendo tabla de clientes: {e}")
            raise

        ok(f"Registros de clientes cargados: {len(df)}")
        return df



    # =======================================================
    #   NORMALIZAR COLUMNAS PARA ENCONTRAR RUC Y RAZÓN SOCIAL
    # =======================================================
    @staticmethod
    def _find_column(df, possible_names):
        """
        Busca una columna por nombres equivalentes,
        tolerante a:
        - mayúsculas
        - espacios
        - guiones
        - slash
        """
        normalized = {col.lower().strip(): col for col in df.columns}

        for name in possible_names:
            if name in normalized:
                return normalized[name]

        return None



    # =======================================================
    #   PROCESO PRINCIPAL
    # =======================================================
    def get_client_data(self):
        """
        Retorna DataFrame con RUC y Razón Social.
        """

        df = self._load_clients_table()

        # Posibles nombres para columna RUC
        possible_ruc_names = {
            "ruc", "ruc/dni", "ruc dni", "ruc_dni", "dni_ruc", "ruc / dni", "ruc /dni"
        }

        # Posibles nombres para Razón Social
        possible_rs_names = {
            "razon social", "razón social", "razon_social",
            "nombre", "nombre cliente", "cliente"
        }

        col_ruc = self._find_column(df, possible_ruc_names)
        col_rs  = self._find_column(df, possible_rs_names)

        if not col_ruc:
            error("No se encontró la columna del RUC en la tabla de clientes.")
            raise KeyError("Columna RUC no encontrada.")

        if not col_rs:
            error("No se encontró la columna de Razón Social.")
            raise KeyError("Columna Razón Social no encontrada.")

        ok(f"Columna RUC detectada como: {col_ruc}")
        ok(f"Columna Razón Social detectada como: {col_rs}")

        # Crear un DataFrame limpio
        df_clean = df[[col_ruc, col_rs]].copy()
        df_clean.columns = ["RUC", "Razon_Social"]

        # Vista previa
        info("Vista previa de clientes:")
        print(df_clean.head())

        return df_clean



# =======================================================
#   EJECUCIÓN DIRECTA (para pruebas)
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando extractor de clientes...")
    extractor = ClientsExtractor()
    df = extractor.get_client_data()
    ok("Extracción completada correctamente.")
