# src/transformers/calculator.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from datetime import datetime, timedelta
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Calculator:
    """
    Calcula:
    - Subtotal limpio
    - IGV
    - Total
    - Detracción
    - Monto neto a cuenta empresa
    - Días de forma de pago
    - Fecha de vencimiento (emisión + días forma de pago)

    Trabaja sobre el DataFrame que viene de InvoicesExtractor.
    """

    def __init__(self):
        self.env = get_env()
        self.igv = self.env.IGV
        self.porc_detraccion = self.env.DETRACCION_PORCENTAJE

        # Cargar settings.json para saber cómo se llaman las columnas
        from json import load

        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("settings.json no encontrado")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        self.cols = self.settings["columnas_facturas"]

        ok("Calculator listo para operar.")


    # =======================================================
    #     UTILIDAD: convertir fecha a datetime
    # =======================================================
    @staticmethod
    def _parse_fecha(value):
        """Convierte fechas comunes a datetime, tolerante a varios formatos."""
        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None

            formatos = [
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%Y/%m/%d",
                "%d.%m.%Y",
            ]

            for f in formatos:
                try:
                    return datetime.strptime(value, f)
                except ValueError:
                    continue

        warn(f"Fecha no reconocida: {value}. Se asignará None.")
        return None


    # =======================================================
    #     UTILIDAD: convertir monto a float
    # =======================================================
    @staticmethod
    def _parse_monto(value):
        """Convierte un monto en string a float limpio."""
        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            limpio = (
                value.replace(",", "")
                     .replace("S/", "")
                     .replace("s/", "")
                     .replace("$", "")
                     .replace(" ", "")
                     .strip()
            )
            try:
                return float(limpio)
            except ValueError:
                warn(f"Monto ilegible: {value}")
                return None

        warn(f"Monto desconocido: {value}")
        return None


    # =======================================================
    #     UTILIDAD: extraer días de forma de pago
    # =======================================================
    @staticmethod
    def _parse_forma_pago(value):
        """
        Extrae el número de días desde la forma de pago.
        Ejemplos:
        - "30"        -> 30
        - "30 días"   -> 30
        - "Contado"   -> 0
        """
        if value is None:
            return 0

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            value = value.lower().strip()
            # Si dice contado, asumimos 0 días
            if "contado" in value:
                return 0

            # Buscar primer número en el string
            num = ""
            for ch in value:
                if ch.isdigit():
                    num += ch
                elif num:
                    break

            if num:
                return int(num)

        warn(f"No se pudo interpretar Forma de pago: {value}. Se usará 0 días.")
        return 0


    # =======================================================
    #     PROCESO PRINCIPAL
    # =======================================================
    def procesar_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recibe un DataFrame de facturas (de InvoicesExtractor)
        y devuelve un DataFrame con columnas calculadas:

        - Subtotal_calc
        - IGV_calc
        - Total_calc
        - Detraccion_calc
        - Monto_Neto_calc
        - Dias_Forma_Pago
        - Fecha_Emision_Parsed
        - Fecha_Vencimiento
        """
        info("Procesando cálculos contables de facturas...")

        df = df.copy()

        col_subtotal = self.cols["subtotal"]
        col_fecha = self.cols["fecha_emision"]
        col_forma = self.cols["forma_pago"]

        # Subtotal limpio
        df["Subtotal_calc"] = df[col_subtotal].apply(self._parse_monto)

        # IGV
        df["IGV_calc"] = df["Subtotal_calc"] * self.igv

        # Total
        df["Total_calc"] = df["Subtotal_calc"] + df["IGV_calc"]

        # Detracción
        df["Detraccion_calc"] = df["Total_calc"] * self.porc_detraccion

        # Monto neto que ingresa a cuenta empresa
        df["Monto_Neto_calc"] = df["Total_calc"] - df["Detraccion_calc"]

        # Forma de pago → días
        df["Dias_Forma_Pago"] = df[col_forma].apply(self._parse_forma_pago)

        # Parseo de fecha
        df["Fecha_Emision_Parsed"] = df[col_fecha].apply(self._parse_fecha)

        # Fecha de vencimiento: emisión + días forma de pago
        def _calc_venc(row):
            f = row["Fecha_Emision_Parsed"]
            d = row["Dias_Forma_Pago"]
            if f:
                return f + timedelta(days=d)
            return None

        df["Fecha_Vencimiento"] = df.apply(_calc_venc, axis=1)

        info("Vista previa de facturas con cálculos:")
        print(df.head())

        ok("Cálculos contables aplicados correctamente.")
        return df



# =======================================================
#   TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando Calculator con InvoicesExtractor...")
    from src.extractors.invoices_extractor import InvoicesExtractor

    extractor = InvoicesExtractor()
    df_facturas = extractor.load_invoices()

    calc = Calculator()
    df_resultado = calc.procesar_facturas(df_facturas)

    print("\n🔍 Vista previa final:")
    print(df_resultado.head())
