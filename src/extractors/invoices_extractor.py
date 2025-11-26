# src/extractors/invoices_extractor.py

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


class InvoicesExtractor:
    """
    Extrae facturas desde la BD origen (DataPulse)
    y devuelve un DataFrame limpio con:

    - Serie
    - Número
    - Combinada (Serie-Número)
    - Subtotal
    - RUC
    - Fecha de emisión
    - Forma de pago
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de facturas...")

        # Cargar settings
        from json import load

        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("settings.json no encontrado")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        ok("Extractor de facturas listo para trabajar.")



    # =======================================================
    #   Carga directa desde SQL
    # =======================================================
    def _load_invoices_table(self):
        tabla = self.settings["tablas"]["facturas"]
        info(f"Cargando tabla de facturas: {tabla}")

        query = text(f"SELECT * FROM {tabla}")

        try:
            df = pd.read_sql(query, self.db.engine_origen)
        except Exception as e:
            error(f"Error leyendo tabla de facturas: {e}")
            raise

        ok(f"Facturas cargadas: {len(df)}")
        return df



    # =======================================================
    #   Proceso principal
    # =======================================================
    def load_invoices(self):
        col = self.settings["columnas_facturas"]
        df = self._load_invoices_table()

        # Validar columnas obligatorias
        required = [
            col["serie"], col["numero"], col["subtotal"],
            col["ruc"], col["fecha_emision"], col["forma_pago"]
        ]

        missing = [c for c in required if c not in df.columns]

        if missing:
            error(f"Faltan columnas en tabla de facturas: {missing}")
            raise KeyError(f"Columnas faltantes: {missing}")

        ok("Columnas necesarias detectadas correctamente.")

        # Crear columna combinada
        df["Combinada"] = (
            df[col["serie"]].astype(str) + "-" + df[col["numero"]].astype(str)
        )

        info("Vista previa de facturas extraídas:")
        print(df.head())

        df_clean = df[[
            col["serie"],
            col["numero"],
            "Combinada",
            col["subtotal"],
            col["ruc"],
            col["fecha_emision"],
            col["forma_pago"]
        ]]

        return df_clean



# =======================================================
#   EJECUCIÓN DIRECTA (test)
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando extractor de facturas...")
    extractor = InvoicesExtractor()
    df = extractor.load_invoices()
    ok("Extracción de facturas completada.")
