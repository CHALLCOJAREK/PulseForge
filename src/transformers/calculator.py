# src/transformers/calculator.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from datetime import timedelta
from json import load

from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Calculator:

    def __init__(self):
        self.env = get_env()

        # Load settings
        cfg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config"))

        with open(os.path.join(cfg_dir, "settings.json"), "r", encoding="utf-8") as f:
            self.settings = load(f)

        with open(os.path.join(cfg_dir, "constants.json"), "r", encoding="utf-8") as f:
            self.constants = load(f)

        ok("Calculator inicializado correctamente.")


    # =======================================================
    #  PARSE DE FORMA DE PAGO (robusto)
    # =======================================================
    def _parse_forma_pago(self, value):
        """
        Convierte valores típicos en días:
        - "15" → 15
        - "15 días" → 15
        - "CREDITO 30" → 30
        - "CONTADO" → 0
        - "CRÉDITO" → 0 si no especifica
        - valores vacíos → 0
        """
        if value is None:
            return 0

        txt = str(value).strip().lower()

        # Contado
        if "contado" in txt:
            return 0

        # Crédito sin número → asumimos 0
        if "credito" in txt and not any(char.isdigit() for char in txt):
            return 0

        # Si contiene un número, lo extraemos
        nums = "".join([c for c in txt if c.isdigit()])
        if nums:
            return int(nums)

        # Si no, 0 por defecto
        return 0


    # =======================================================
    #  PROCESO PRINCIPAL
    # =======================================================
    def process_facturas(self, df_facturas: pd.DataFrame):
        info("Aplicando cálculos financieros a facturas...")

        df = df_facturas.copy()

        IGV = float(self.env.get("IGV", 0.18))
        DTR = float(self.env.get("DETRACCION_PORCENTAJE", 0.04))
        TOL = int(self.env.get("DAYS_TOLERANCE_PAGO", 14))

        # --------------------------
        # Conversión y limpieza
        # --------------------------
        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)
        df["fecha_emision"] = pd.to_datetime(df["fecha_emision"], errors="coerce")

        df["dias_pago"] = df["forma_pago"].apply(self._parse_forma_pago)

        # ANULADOS / SUBTOTAL = 0 → ignorar cálculos
        df_valid = df[df["subtotal"] > 0].copy()

        # --------------------------
        # Cálculos principales
        # --------------------------
        df_valid["igv"]             = df_valid["subtotal"] * IGV
        df_valid["total_con_igv"]   = df_valid["subtotal"] + df_valid["igv"]
        df_valid["detraccion_monto"] = df_valid["total_con_igv"] * DTR
        df_valid["neto_recibido"]    = df_valid["total_con_igv"] - df_valid["detraccion_monto"]

        # Fechas
        df_valid["fecha_limite_pago"] = df_valid["fecha_emision"] + df_valid["dias_pago"].apply(lambda x: timedelta(days=x))

        # Ventanas
        df_valid["fecha_inicio_ventana"] = df_valid["fecha_limite_pago"] - timedelta(days=TOL)
        df_valid["fecha_fin_ventana"]    = df_valid["fecha_limite_pago"] + timedelta(days=TOL)

        ok("Cálculos financieros aplicados con éxito.")
        return df_valid


    # =======================================================
    #   MOVIMIENTOS BANCARIOS
    # =======================================================
    def process_bancos(self, df_bancos: pd.DataFrame):
        info("Preparando movimientos bancarios...")

        df = df_bancos.copy()

        VAR = float(self.env.get("MONTO_VARIACION", 0.50))

        df["monto_variacion_min"] = df["Monto"] - VAR
        df["monto_variacion_max"] = df["Monto"] + VAR
        df["es_dolares"] = df["Moneda"].astype(str).str.upper().str.contains("USD")

        ok("Movimientos bancarios preparados correctamente.")
        return df
