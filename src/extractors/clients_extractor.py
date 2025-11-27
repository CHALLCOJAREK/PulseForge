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
    Extrae razón social desde la tabla de clientes.
    Basado en:
    - settings.json → tabla de origen
    - constants.json → nombres posibles y reglas
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de clientes...")

        # -------------------------------
        # Cargar settings.json
        # -------------------------------
        from json import load

        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )
        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("settings.json no encontrado")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)


        # -------------------------------
        # Cargar constants.json (palabras clave opcionales)
        # -------------------------------
        constants_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/constants.json")
        )
        if not os.path.exists(constants_path):
            warn("⚠ No se encontró constants.json. Continuando sin él.")
            self.constants = {}
        else:
            with open(constants_path, "r", encoding="utf-8") as f:
                self.constants = load(f)

        ok("Extractor de clientes listo para trabajar.")



    # =======================================================
    #   CARGAR TABLA COMPLETA DESDE SQL
    # =======================================================
    def _load_clients_table(self):
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
    #   BUSCAR COLUMNA DE MANERA INTELIGENTE
    # =======================================================
    @staticmethod
    def _find_column(df, possible_names):
        """
        Encuentra la columna correcta aunque:
        - tenga mayúsculas
        - tenga espacios variados
        - tenga slash
        - venga pésimamente escrita
        """
        normalized = {col.lower().strip(): col for col in df.columns}

        for name in possible_names:
            clean = name.lower().strip()
            if clean in normalized:
                return normalized[clean]

        return None



    # =======================================================
    #   PROCESO PRINCIPAL
    # =======================================================
    def get_client_data(self):
        df = self._load_clients_table()

        # Nombres que sí vienen en tu BD (te los puse exactos)
        possible_ruc_names = {
            "ruc / dni", "ruc/dni", "ruc", "dni_ruc", "ruc dni"
        }

        possible_rs_names = {
            "razon social", "razón social", "razon_social", 
            "razon", "nombre", "cliente"
        }

        col_ruc = self._find_column(df, possible_ruc_names)
        col_rs  = self._find_column(df, possible_rs_names)

        if not col_ruc:
            error("❌ No se encontró la columna del RUC en la tabla de clientes.")
            raise KeyError("Columna RUC no encontrada.")

        if not col_rs:
            error("❌ No se encontró la columna de Razón Social.")
            raise KeyError("Columna Razón Social no encontrada.")

        ok(f"Columna RUC detectada como: {col_ruc}")
        ok(f"Columna Razón Social detectada como: {col_rs}")

        # ---------------------------------------------------
        # Limpieza final
        # ---------------------------------------------------
        df_clean = df[[col_ruc, col_rs]].copy()
        df_clean.columns = ["RUC", "Razon_Social"]

        df_clean["RUC"] = df_clean["RUC"].astype(str).str.strip()
        df_clean["Razon_Social"] = df_clean["Razon_Social"].astype(str).str.strip()

        info("Vista previa de clientes normalizados:")
        print(df_clean.head())

        return df_clean



# =======================================================
#   TEST DIRECTO MANUAL
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando extractor de clientes (solo carga y mapeo)...")
    extractor = ClientsExtractor()
    df = extractor.get_client_data()
    ok("Extracción completada correctamente.")
