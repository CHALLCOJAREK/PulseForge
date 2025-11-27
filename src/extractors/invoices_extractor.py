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
    Extrae y normaliza facturas desde la BD DataPulse,
    usando nombres reales declarados en settings.json.

    Devuelve DataFrame con:
    - RUC
    - Cliente Generador
    - Serie
    - Número
    - Combinada
    - Subtotal
    - Fecha Emisión
    - Condición Pago (días)
    - Vencimiento (si existe)
    - Estados (fs y cont)
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de facturas...")

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
        # Cargar constants.json
        # -------------------------------
        constants_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/constants.json")
        )

        if not os.path.exists(constants_path):
            error(f"No se encontró constants.json en: {constants_path}")
            raise FileNotFoundError("constants.json no encontrado")

        with open(constants_path, "r", encoding="utf-8") as f:
            self.constants = load(f)

        ok("Extractor de facturas listo para trabajar.")


    # =======================================================
    #   CARGA COMPLETA DESDE SQL
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
    #   PROCESO PRINCIPAL (NO HACE PRUEBAS)
    # =======================================================
    def load_invoices(self):
        col = self.settings["columnas_facturas"]
        cst = self.constants

        df = self._load_invoices_table()

        # ---------------------------------------------------
        # Validar columnas obligatorias
        # ---------------------------------------------------
        required = [
            col["ruc"], col["cliente_generador"],
            col["subtotal"], col["serie"], col["numero"],
            col["fecha_emision"], col["forma_pago"]
        ]

        missing = [c for c in required if c not in df.columns]
        if missing:
            error(f"Faltan columnas en tabla de facturas: {missing}")
            raise KeyError(f"Columnas faltantes: {missing}")

        ok("Columnas necesarias detectadas correctamente.")


        # ---------------------------------------------------
        # Filtrar facturas inválidas según constants.json
        # ---------------------------------------------------
        if col["estado_fs"] in df.columns:
            df = df[~df[col["estado_fs"]].astype(str).str.upper().isin(
                [s.upper() for s in cst["estados_factura_invalidos"]]
            )]
            ok(f"Filtrado facturas inválidas. Quedan: {len(df)}")


        # ---------------------------------------------------
        # Crear columna combinada (si no existe)
        # ---------------------------------------------------
        if col["combinada"] in df.columns:
            df["Combinada"] = df[col["combinada"]].astype(str)
        else:
            df["Combinada"] = df[col["serie"]].astype(str) + "-" + df[col["numero"]].astype(str)

        # ---------------------------------------------------
        # Limpieza y renombrado
        # ---------------------------------------------------
        df_clean = pd.DataFrame()

        df_clean["RUC"] = df[col["ruc"]].astype(str).str.strip()
        df_clean["Cliente_Generador"] = df[col["cliente_generador"]].astype(str).str.strip()

        df_clean["Serie"] = df[col["serie"]].astype(str)
        df_clean["Numero"] = df[col["numero"]].astype(str)
        df_clean["Combinada"] = df["Combinada"].astype(str)

        df_clean["Subtotal"] = pd.to_numeric(df[col["subtotal"]], errors="coerce").fillna(0)

        df_clean["Fecha_Emision"] = pd.to_datetime(df[col["fecha_emision"]], errors="coerce")
        df_clean["Forma_Pago"] = df[col["forma_pago"]].astype(str).str.extract(r"(\d+)").fillna("0").astype(int)

        # Fecha Vencimiento (si existe)
        if col["vencimiento"] in df.columns:
            df_clean["Vencimiento"] = pd.to_datetime(df[col["vencimiento"]], errors="coerce")
        else:
            df_clean["Vencimiento"] = None

        # Estados
        df_clean["Estado_FS"] = df.get(col["estado_fs"], "DESCONOCIDO")
        df_clean["Estado_Cont"] = df.get(col["estado_cont"], "DESCONOCIDO")

        ok("Facturas normalizadas correctamente.")
        return df_clean



# =======================================================
#   TEST DIRECTO MANUAL (NO AUTOMÁTICO)
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando extractor de facturas (solo carga y normaliza)...")
    extractor = InvoicesExtractor()
    df = extractor.load_invoices()
    ok("Extracción de facturas completada.")
    print(df.head())
