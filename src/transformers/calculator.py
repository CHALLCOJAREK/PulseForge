# src/transformers/calculator.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from datetime import timedelta

from src.core.env_loader import get_env
from json import load

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Calculator:
    """
    Calcula:
      - IGV
      - detracción
      - monto neto real (lo que ingresa a IBK)
      - monto detracción (lo que ingresa a BN)
      - fecha límite de pago (fecha_emisión + forma_pago)
      - ventana de tolerancia (± DAYS_TOLERANCE_PAGO)
    """

    def __init__(self):
        self.env = get_env()

        # ⚙ Cargar settings.json
        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error("settings.json no encontrado en Calculator.")
            raise FileNotFoundError("settings.json no encontrado.")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        # ⚙ Cargar constants.json
        constants_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/constants.json")
        )

        if not os.path.exists(constants_path):
            error("constants.json no encontrado.")
            raise FileNotFoundError("constants.json no encontrado.")

        with open(constants_path, "r", encoding="utf-8") as f:
            self.constants = load(f)

        ok("Calculator inicializado correctamente.")



    # =======================================================
    #     PROCESO PRINCIPAL DEL CALCULATOR
    # =======================================================
    def process_facturas(self, df_facturas: pd.DataFrame):
        """
        Devuelve las facturas enriquecidas con:

            igv
            total_con_igv
            detraccion_monto
            neto_recibido
            fecha_limite_pago
            fecha_inicio_ventana
            fecha_fin_ventana

        Estas columnas son claves para el Matcher.
        """

        info("Aplicando cálculos financieros a facturas...")

        df = df_facturas.copy()

        IGV = float(self.env.get("IGV", 0.18))
        DTR = float(self.env.get("DETRACCION_PORCENTAJE", 0.04))
        TOL = int(self.env.get("DAYS_TOLERANCE_PAGO", 14))

        # =======================================================
        # IGV → subtotal * IGV
        # =======================================================
        df["igv"] = df["subtotal"] * IGV

        # =======================================================
        # Total con IGV
        # =======================================================
        df["total_con_igv"] = df["subtotal"] + df["igv"]

        # =======================================================
        # Detracción → total_con_igv * detracción%
        # =======================================================
        df["detraccion_monto"] = df["total_con_igv"] * DTR

        # =======================================================
        # Neto recibido (lo que llega a IBK)
        # =======================================================
        df["neto_recibido"] = df["total_con_igv"] - df["detraccion_monto"]

        # =======================================================
        # Fecha límite de pago → fecha_emision + forma_pago
        # =======================================================
        df["fecha_limite_pago"] = df["fecha_emision"] + pd.to_timedelta(df["forma_pago"], unit="D")

        # =======================================================
        # Ventana de pago válida → tolerancia ± días
        # =======================================================
        df["fecha_inicio_ventana"] = df["fecha_limite_pago"] - timedelta(days=TOL)
        df["fecha_fin_ventana"]    = df["fecha_limite_pago"] + timedelta(days=TOL)

        ok("Cálculos financieros aplicados con éxito.")
        return df


    # =======================================================
    #     PREPARAR MOVIMIENTOS BANCARIOS PARA MATCHER
    # =======================================================
    def process_bancos(self, df_bancos: pd.DataFrame):
        """
        Devuelve movimientos bancarios enriquecidos con:

            monto_variacion_min
            monto_variacion_max
            es_dolares (flag)
        """

        info("Preparando movimientos bancarios...")

        df = df_bancos.copy()

        VAR = float(self.env.get("MONTO_VARIACION", 0.50))

        # Variación permitida del monto para comparación
        df["monto_variacion_min"] = df["Monto"] - VAR
        df["monto_variacion_max"] = df["Monto"] + VAR

        # Bandera para pagos en dólares
        df["es_dolares"] = df["Moneda"].astype(str).str.upper().str.contains("USD")

        ok("Movimientos bancarios preparados correctamente.")
        return df



# =======================================================
#     TEST DIRECTO (opcional)
# =======================================================
if __name__ == "__main__":
    warn("Test directo del Calculator (solo para debug).")
    # Aquí no se prueban extractores para no duplicar procesos.
