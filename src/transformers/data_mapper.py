# src/transformers/data_mapper.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from src.core.env_loader import get_env
from json import load

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class DataMapper:
    """
    Estandariza TODA la data que viene de:
      - Extractor de clientes
      - Extractor de facturas
      - Extractor de bancos

    El objetivo es que el Calculator y el Matcher NO dependan
    de nombres reales de columnas.
    """

    def __init__(self):
        self.env = get_env()

        # Cargar settings.json
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
    #   MAPEO DE CLIENTES
    # =======================================================
    def map_clientes(self, df_clientes: pd.DataFrame):
        """
        Devuelve:
        RUC - string
        Razon_Social - string
        """
        info("Normalizando clientes...")

        df = df_clientes.copy()

        df["RUC"] = df["RUC"].astype(str).str.strip()
        df["Razon_Social"] = df["Razon_Social"].astype(str).str.strip()

        ok(f"Clientes normalizados: {len(df)} registrados.")
        return df



    # =======================================================
    #   MAPEO DE FACTURAS
    # =======================================================
    def map_facturas(self, df_fact: pd.DataFrame):
        """
        Devuelve DataFrame estandarizado:

            ruc
            cliente_generador
            subtotal
            serie
            numero
            combinada
            estado_fs
            estado_cont
            fecha_emision (datetime)
            forma_pago (int)
            vencimiento (datetime)
        """

        info("Normalizando facturas...")

        col = self.settings["columnas_facturas"]
        df = df_fact.copy()

        # Asegurar strings limpios
        df[col["serie"]] = df[col["serie"]].astype(str).str.strip()
        df[col["numero"]] = df[col["numero"]].astype(str).str.strip()
        df[col["ruc"]]    = df[col["ruc"]].astype(str).str.strip()

        # Fecha de emisión → datetime
        df["fecha_emision"] = pd.to_datetime(df[col["fecha_emision"]], errors="coerce")

        # Forma de pago → int
        df["forma_pago"] = pd.to_numeric(df[col["forma_pago"]], errors="coerce").fillna(0).astype(int)

        # Subtotal → número
        df["subtotal"] = pd.to_numeric(df[col["subtotal"]], errors="coerce")

        # Estado FS y CONT
        df["estado_fs"] = df[col["estado_fs"]].astype(str).str.lower().str.strip()
        df["estado_cont"] = df[col["estado_cont"]].astype(str).str.lower().str.strip()

        # Cliente generador
        df["cliente_generador"] = df[col["cliente_generador"]].astype(str).str.strip()

        # Combinada
        df["combinada"] = df[col["serie"]].astype(str) + "-" + df[col["numero"]].astype(str)

        # Vencimiento a datetime
        df["vencimiento"] = pd.to_datetime(df[col["vencimiento"]], errors="coerce")

        ok(f"Facturas normalizadas: {len(df)} registros.")
        return df[
            [
                "ruc",
                "cliente_generador",
                "subtotal",
                col["serie"],
                col["numero"],
                "combinada",
                "estado_fs",
                "estado_cont",
                "fecha_emision",
                "forma_pago",
                "vencimiento",
            ]
        ]



    # =======================================================
    #   MAPEO DE BANCOS
    # =======================================================
    def map_bancos(self, df_banco: pd.DataFrame):
        """
        Devuelve DataFrame estandarizado:

            banco
            fecha_mov
            tipo_mov
            descripcion
            serie
            numero
            monto
            moneda
            operacion
            destinatario
            tipo_documento
        """

        info("Normalizando movimientos bancarios...")

        col = self.settings["columnas_bancos"]
        df = df_banco.copy()

        # Fecha movimiento → datetime
        df["fecha_mov"] = pd.to_datetime(df[col["fecha"]], errors="coerce")

        # Serie / número → str
        df["serie"] = df[col["serie"]].astype(str).str.strip() if col["serie"] in df else ""
        df["numero"] = df[col["numero"]].astype(str).str.strip() if col["numero"] in df else ""

        # Monto → numérico
        df["monto"] = pd.to_numeric(df[col["monto"]], errors="coerce").fillna(0)

        # Moneda → string normalizado
        df["moneda"] = df[col["moneda"]].astype(str).str.upper().str.strip()

        # Tipo movimiento
        df["tipo_mov"] = df[col["tipo_mov"]].astype(str).str.upper().str.strip()

        # Descripción
        df["descripcion"] = df[col["descripcion"]].astype(str).str.strip()

        # Operación
        df["operacion"] = df[col["operacion"]].astype(str).str.strip()

        # Destinatario
        df["destinatario"] = df[col["destinatario"]].astype(str).str.strip()

        # Tipo de documento
        df["tipo_documento"] = df[col["tipo_documento"]].astype(str).str.upper().str.strip()

        ok(f"Movimientos bancarios normalizados: {len(df)} registros.")

        return df[
            [
                "Banco",
                "fecha_mov",
                "tipo_mov",
                "descripcion",
                "serie",
                "numero",
                "monto",
                "moneda",
                "operacion",
                "destinatario",
                "tipo_documento"
            ]
        ]
