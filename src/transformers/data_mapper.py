# src/transformers/data_mapper.py
from __future__ import annotations

# ============================================================
#  BOOTSTRAP RUTAS
# ============================================================
import os
import sys
import pandas as pd
from pathlib import Path
from json import load

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


# ============================================================
#  DATA MAPPER CORPORATIVO · PULSEFORGE V2
# ============================================================
class DataMapper:

    def __init__(self):
        info("Inicializando DataMapper PulseForge…")

        # Cargar settings.json
        config_path = ROOT / "config" / "settings.json"
        if not config_path.exists():
            error("settings.json no encontrado — DataMapper NO puede iniciar.")
            raise FileNotFoundError("settings.json no encontrado")

        with open(config_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        ok("DataMapper cargado correctamente.")


    # ============================================================
    #  CLIENTES
    # ============================================================
    def map_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        info("Normalizando clientes…")

        df = df.copy()

        df["RUC"] = df["RUC"].astype(str).str.strip()
        df["Razon_Social"] = df["Razon_Social"].astype(str).str.strip()

        ok(f"Clientes normalizados: {len(df)} registros.")

        return df[["RUC", "Razon_Social"]]


    # ============================================================
    #  FACTURAS
    # ============================================================
    def map_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        info("Normalizando facturas…")

        df = df.copy()

        df_std = pd.DataFrame()

        # ID base
        df_std["ruc"] = df["RUC"].astype(str).str.strip()
        df_std["cliente_generador"] = df["Cliente_Generador"].astype(str).str.strip()

        # Monto base
        df_std["subtotal"] = pd.to_numeric(df["Subtotal"], errors="coerce")

        df_std["serie"] = df["Serie"].astype(str).str.strip()
        df_std["numero"] = df["Numero"].astype(str).str.strip()
        df_std["combinada"] = df["Combinada"].astype(str).str.strip()

        # Estados administrativos
        df_std["estado_fs"] = df["Estado_FS"].astype(str).str.lower().str.strip()
        df_std["estado_cont"] = df["Estado_Cont"].astype(str).str.lower().str.strip()

        # Fechas
        df_std["fecha_emision"] = pd.to_datetime(df["Fecha_Emision"], errors="coerce")
        df_std["fecha_limite_pago"] = pd.to_datetime(df["Vencimiento"], errors="coerce")

        # Ventanas dinámicas (procesadas después por calculator)
        df_std["fecha_inicio_ventana"] = None
        df_std["fecha_fin_ventana"] = None

        # Valores derivados — se llenan después por calculator
        df_std["neto_recibido"] = None
        df_std["total_con_igv"] = None
        df_std["detraccion_monto"] = None

        ok(f"Facturas normalizadas: {len(df_std)} registros.")

        return df_std


    # ============================================================
    #  BANCOS — COMPATIBLE CON MATCHER ULTRA-BLINDADO
    # ============================================================
    def map_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        info("Normalizando movimientos bancarios…")

        df = df.copy()

        # -------------------------------------------
        # 1) Detección automática de columna Fecha
        # -------------------------------------------
        fecha_vars = [c for c in df.columns if "fecha" in c.lower()]
        if not fecha_vars:
            error("No existe columna Fecha en bancos.")
            raise KeyError("df_banco no contiene columna de fecha")

        col_fecha = fecha_vars[0]
        df["Fecha"] = pd.to_datetime(df[col_fecha], errors="coerce")

        # -------------------------------------------
        # 2) Mapeo estándar
        # -------------------------------------------
        col_map = {
            "Banco": "Banco",
            "Descripcion": "Descripcion",
            "Monto": "Monto",
            "Moneda": "Moneda",
            "Operacion": "Operacion",
            "Tipo_Mov": "tipo_mov",
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

        # -------------------------------------------
        # 3) Conversión de moneda (opcional)
        # -------------------------------------------
        df_std["es_dolares"] = df_std["Moneda"].astype(str).str.upper().str.contains("USD")

        # -------------------------------------------
        # 4) Reordenar columnas
        # -------------------------------------------
        df_std = df_std[[
            "Banco",
            "Fecha",
            "tipo_mov",
            "Descripcion",
            "Monto",
            "Moneda",
            "Operacion",
            "es_dolares",
            "destinatario",
            "tipo_documento",
        ]]

        ok(f"Movimientos bancarios normalizados: {len(df_std)} registros.")

        return df_std
