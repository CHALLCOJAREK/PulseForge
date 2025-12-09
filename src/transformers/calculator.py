# src/transformers/calculator.py
from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd
from typing import Optional, Dict, Any

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, PulseForgeConfig
from src.core.validations import validate_igv, validate_detraccion, validate_tipo_cambio


# ============================================================
#  CALCULATOR — MOTOR FINANCIERO 2025 (VERSIÓN FINAL)
# ============================================================
class Calculator:

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        self.cfg = cfg or get_config()

        # Validación financiera
        self.igv = float(validate_igv(self.cfg.igv))
        self.detraccion = float(validate_detraccion(self.cfg.detraccion))
        self.monto_variacion = float(self.cfg.variacion_monto)
        self.days_tolerance = int(self.cfg.tolerancia_dias)
        self.tipo_cambio = float(validate_tipo_cambio(self.cfg.tipo_cambio))

        self.modo_debug = bool(self.cfg.modo_debug)

        ok("Calculator cargado con parámetros financieros correctos.")

    # ------------------------------------------------------------
    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # ============================================================
    #   PROCESO FACTURAS
    # ============================================================
    def process_facturas(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando motor financiero a facturas…")

        df = self._normalize_df(df_facturas)

        # --------------------------------------------------------
        # VALIDACIÓN BÁSICA
        # --------------------------------------------------------
        if "subtotal" not in df.columns:
            raise ValueError("Falta columna 'subtotal' en facturas")

        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)

        # --------------------------------------------------------
        # FECHA EMISIÓN — SIEMPRE EXISTE
        # --------------------------------------------------------
        if "fecha_emision" not in df.columns:
            df["fecha_emision"] = pd.NaT

        df["fecha_emision"] = pd.to_datetime(df["fecha_emision"], errors="coerce")

        # --------------------------------------------------------
        # FORMA DE PAGO + DÍAS DE PAGO — SIEMPRE EXISTE
        # --------------------------------------------------------
        if "forma_pago" not in df.columns:
            df["forma_pago"] = ""

        df["forma_pago"] = df["forma_pago"].fillna("")
        df["dias_pago"] = df["forma_pago"].apply(self._parse_forma_pago)

        # --------------------------------------------------------
        # CÁLCULOS FINANCIEROS
        # --------------------------------------------------------
        df["igv"] = (df["subtotal"] * self.igv).round(2)
        df["total_con_igv"] = (df["subtotal"] + df["igv"]).round(2)
        df["detraccion_monto"] = (df["total_con_igv"] * self.detraccion).round(2)
        df["neto_recibido"] = (df["total_con_igv"] - df["detraccion_monto"]).round(2)

        df["tiene_detraccion"] = df["detraccion_monto"] > 0

        # --------------------------------------------------------
        # FECHA LIMITE PAGO — SIEMPRE EXISTE
        # --------------------------------------------------------
        df["fecha_limite_pago"] = df["fecha_emision"] + pd.to_timedelta(df["dias_pago"], unit="D")

        # --------------------------------------------------------
        # VENTANAS DE MATCH — SIEMPRE EXISTEN
        # --------------------------------------------------------
        df["fecha_inicio_ventana"] = df["fecha_limite_pago"] - timedelta(days=self.days_tolerance)
        df["fecha_fin_ventana"] = df["fecha_limite_pago"] + timedelta(days=self.days_tolerance)

        ok("Facturas procesadas correctamente.")
        return df

    # ------------------------------------------------------------
    @staticmethod
    def _parse_forma_pago(value) -> int:
        """Extrae días de crédito o retorna 0 si es contado."""
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)

        txt = str(value).lower()
        if "contado" in txt:
            return 0

        digits = "".join(filter(str.isdigit, txt))
        return int(digits) if digits else 0


# ============================================================
#   APIs PARA DATAMAPPER — INMUTABLES
# ============================================================
def preparar_factura_para_insert(subtotal: float, extra_campos: Dict[str, Any]):
    igv = round(subtotal * 0.18, 2)
    total_con_igv = round(subtotal + igv, 2)
    detraccion_monto = round(total_con_igv * 0.04, 2)
    neto_recibido = round(total_con_igv - detraccion_monto, 2)

    factura = {
        "subtotal": subtotal,
        "igv": igv,
        "total_con_igv": total_con_igv,
        "detraccion_monto": detraccion_monto,
        "neto_recibido": neto_recibido,
    }

    factura.update(extra_campos)
    return factura


def preparar_movimiento_bancario_para_insert(
        monto: float,
        moneda: str,
        codigo_banco: str,
        extra_campos: Dict[str, Any],
):
    movimiento = {
        "monto": monto,
        "moneda": moneda,
        "codigo_banco": codigo_banco,
    }
    movimiento.update(extra_campos)
    return movimiento
