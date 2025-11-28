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

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de facturas...")

        from json import load
        
        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )
        constants_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/constants.json")
        )

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        with open(constants_path, "r", encoding="utf-8") as f:
            self.constants = load(f)

        ok("Extractor de facturas listo para trabajar.")


    # =======================================================
    # Lectura SQL
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
    # Resolver nombre real de columna
    # =======================================================
    def _resolve_column(self, df, candidates):
        """
        Devuelve el nombre REAL de una columna aunque tenga:
        - Mayúsculas/minúsculas
        - Slashes
        - Espacios
        - Aliases raros ("Ruc / Dni")
        """
        normalized = {c.lower().replace(" ", "").replace("/", ""): c for c in df.columns}

        for candidate in candidates:
            key = candidate.lower().replace(" ", "").replace("/", "")
            if key in normalized:
                return normalized[key]

        return None


    # =======================================================
    # PROCESO PRINCIPAL
    # =======================================================
    def load_invoices(self):
        df = self._load_invoices_table()
        col = self.settings["columnas_facturas"]
        cst = self.constants

        # Diccionario mapeado inteligente
        resolved = {}

        # Posibles alias para cada columna importante
        aliases = {
            "ruc": ["ruc", "rucdni", "ruc/dni", "ruc / dni", "documento", "cliente_ruc"],
            "cliente_generador": ["cliente_generador", "razon social", "cliente"],
            "subtotal": ["subtotal", "sub total", "monto"],
            "serie": ["serie", "seriefactura"],
            "numero": ["numero", "num", "número"],
            "fecha_emision": ["fechaemision", "fecha emision", "f_emision"],
            "forma_pago": ["condicion de pago", "forma_pago", "condicionpago"],
        }

        # Resolver cada columna real
        for key, options in aliases.items():
            # primero prueba el nombre exacto del settings.json
            preferred = col[key]
            candidates = [preferred] + options

            real = self._resolve_column(df, candidates)

            if real is None:
                error(f"No se pudo resolver columna: {key} | intentado: {candidates}")
                raise KeyError(f"Columna no encontrada: {key}")

            resolved[key] = real

        ok("Columnas críticas detectadas correctamente.")

        # =======================================================
        # Construcción dataframe limpio
        # =======================================================
        df_clean = pd.DataFrame()

        df_clean["RUC"] = df[resolved["ruc"]].astype(str).str.strip()
        df_clean["Cliente_Generador"] = df[resolved["cliente_generador"]].astype(str).str.strip()

        df_clean["Serie"] = df[resolved["serie"]].astype(str)
        df_clean["Numero"] = df[resolved["numero"]].astype(str)
        df_clean["Combinada"] = df_clean["Serie"] + "-" + df_clean["Numero"]

        df_clean["Subtotal"] = pd.to_numeric(df[resolved["subtotal"]], errors="coerce").fillna(0)

        df_clean["Fecha_Emision"] = pd.to_datetime(df[resolved["fecha_emision"]], errors="coerce")
        df_clean["Forma_Pago"] = (
            df[resolved["forma_pago"]].astype(str).str.extract(r"(\d+)").fillna("0").astype(int)
        )

        # Opcionales
        if col["vencimiento"] in df.columns:
            df_clean["Vencimiento"] = pd.to_datetime(df[col["vencimiento"]], errors="coerce")
        else:
            df_clean["Vencimiento"] = None

        df_clean["Estado_FS"] = df.get(col["estado_fs"], "DESCONOCIDO")
        df_clean["Estado_Cont"] = df.get(col["estado_cont"], "DESCONOCIDO")

        ok("Facturas normalizadas correctamente.")
        return df_clean


if __name__ == "__main__":
    info("🚀 Test extractor facturas...")
    e = InvoicesExtractor()
    df = e.load_invoices()
    print(df.head())
    ok("Test OK.")
