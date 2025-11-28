# src/transformers/data_mapper.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from json import load
from src.core.env_loader import get_env

def info(msg): print(f"🔵 {msg}")
def ok(msg):   print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class DataMapper:

    def __init__(self):
        self.env = get_env()

        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error("settings.json no encontrado. DataMapper no puede iniciar.")
            raise FileNotFoundError("settings.json no encontrado")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        ok("DataMapper inicializado correctamente.")


    # =======================================================
    # MAPEO DE CLIENTES
    # =======================================================
    def map_clientes(self, df_clientes: pd.DataFrame):
        info("Normalizando clientes...")

        df = df_clientes.copy()

        df["RUC"] = df["RUC"].astype(str).str.strip()
        df["Razon_Social"] = df["Razon_Social"].astype(str).str.strip()

        ok(f"Clientes normalizados: {len(df)} registrados.")
        return df


    # =======================================================
    # MAPEO DE FACTURAS
    # =======================================================
    def map_facturas(self, df_fact: pd.DataFrame):
        info("Normalizando facturas...")

        df = df_fact.copy()
        df_std = pd.DataFrame()

        df_std["ruc"]               = df["RUC"].astype(str).str.strip()
        df_std["cliente_generador"] = df["Cliente_Generador"].astype(str).str.strip()
        df_std["subtotal"]          = df["Subtotal"]
        df_std["serie"]             = df["Serie"].astype(str).str.strip()
        df_std["numero"]            = df["Numero"].astype(str).str.strip()
        df_std["combinada"]         = df["Combinada"].astype(str)

        df_std["estado_fs"]   = df["Estado_FS"].astype(str).str.lower().str.strip()
        df_std["estado_cont"] = df["Estado_Cont"].astype(str).str.lower().str.strip()

        df_std["fecha_emision"] = pd.to_datetime(df["Fecha_Emision"], errors="coerce")
        df_std["forma_pago"]    = df["Forma_Pago"]
        df_std["Vencimiento"]   = df["Vencimiento"]

        ok(f"Facturas normalizadas: {len(df_std)} registros.")
        return df_std


    # =======================================================
    # MAPEO DE BANCOS (COMPATIBLE CON MATCHER ULTRA-BLINDADO)
    # =======================================================
    def map_bancos(self, df_banco: pd.DataFrame):
        info("Normalizando movimientos bancarios...")

        df = df_banco.copy()

        # ——————————————————————————————
        # DETECCIÓN DE FECHA
        # ——————————————————————————————
        fecha_vars = [c for c in df.columns if "fecha" in c.lower()]
        if not fecha_vars:
            error("No existe columna Fecha en bancos.")
            raise KeyError("df_banco NO contiene columna Fecha")

        col_fecha = fecha_vars[0]
        df["Fecha"] = pd.to_datetime(df[col_fecha], errors="coerce")

        # ——————————————————————————————
        # RENOMBRAR TODAS LAS COLUMNAS
        # ——————————————————————————————
        col_map = {
            "Banco": "Banco",
            "Tipo_Mov": "tipo_mov",
            "Descripcion": "Descripcion",     # <-- AHORA ENVIAMOS EXACTO LO QUE EL MATCHER REQUIERE
            "Monto": "Monto",
            "Moneda": "Moneda",
            "Operacion": "Operacion",
            "Destinatario": "destinatario",
            "Tipo_Documento": "tipo_documento"
        }

        df_std = pd.DataFrame()

        for old, new in col_map.items():
            if old in df.columns:
                df_std[new] = df[old]
            else:
                df_std[new] = ""

        df_std["Fecha"] = df["Fecha"]

        ok(f"Movimientos bancarios normalizados: {len(df_std)} registros.")

        return df_std[[
            "Banco",
            "Fecha",
            "tipo_mov",
            "Descripcion",   # 🔥 YA ES COMPATIBLE
            "Monto",
            "Moneda",
            "Operacion",
            "destinatario",
            "tipo_documento"
        ]]
