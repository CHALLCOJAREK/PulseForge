# src/transformers/calculator.py
from __future__ import annotations

# ------------------------------------------------------------
#  BOOTSTRAP RUTAS
# ------------------------------------------------------------
import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ------------------------------------------------------------
import pandas as pd
from datetime import timedelta
from typing import Optional, Dict, Any

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, PulseForgeConfig
from src.core.validations import (
    validate_igv,
    validate_detraccion,
    validate_tipo_cambio,
)
from src.transformers.ai_helpers import ai_classify


# ============================================================
#  CALCULATOR · MOTOR FINANCIERO PULSEFORGE (OPTIMIZADO)
# ============================================================
class Calculator:

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        self.cfg = cfg or get_config()

        # Configuración validada
        self.igv = float(validate_igv(self.cfg.igv))
        self.detraccion = float(validate_detraccion(self.cfg.detraccion_porcentaje))
        self.days_tolerance = int(self.cfg.days_tolerance_pago)
        self.monto_variacion = float(self.cfg.monto_variacion)
        self.tipo_cambio_usd_pen = float(validate_tipo_cambio(self.cfg.tipo_cambio_usd_pen))

        self.activar_ia = bool(self.cfg.activar_ia)
        self.modo_debug = bool(self.cfg.modo_debug)

        ok("Calculator cargado con configuración centralizada.")

    # ============================================================
    # Helpers
    # ============================================================
    @staticmethod
    def _parse_forma_pago(value) -> int:
        """
        Traduce “contado”, “crédito”, “crédito 30 días”, etc → número de días.
        """
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)

        txt = str(value).lower()

        if "contado" in txt:
            return 0

        digits = "".join(filter(str.isdigit, txt))
        return int(digits) if digits else 0

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # ============================================================
    # FACTURAS
    # ============================================================
    def process_facturas(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando cálculos financieros a facturas…")

        df = self._normalize_df(df_facturas)

        # Validaciones rápidas
        faltantes = [col for col in ("subtotal", "fecha_emision") if col not in df.columns]
        if faltantes:
            error(f"Faltan columnas obligatorias: {faltantes}")
            raise ValueError(f"Columnas faltantes en facturas: {faltantes}")

        # Normalización
        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)
        df["fecha_emision"] = pd.to_datetime(df["fecha_emision"], errors="coerce")

        df["forma_pago"] = df.get("forma_pago", "").fillna("")
        df["dias_pago"] = df["forma_pago"].apply(self._parse_forma_pago)

        # Cálculos financieros
        df["igv"] = (df["subtotal"] * self.igv).round(2)
        df["total_con_igv"] = (df["subtotal"] + df["igv"]).round(2)
        df["detraccion_monto"] = (df["total_con_igv"] * self.detraccion).round(2)
        df["neto_recibido"] = (df["total_con_igv"] - df["detraccion_monto"]).round(2)

        df["tiene_detraccion"] = df["detraccion_monto"] > 0

        # Ventanas temporales exactas
        df["fecha_limite_pago"] = df["fecha_emision"] + df["dias_pago"].apply(lambda d: timedelta(days=d))
        df["fecha_inicio_ventana"] = df["fecha_limite_pago"] - timedelta(days=self.days_tolerance)
        df["fecha_fin_ventana"] = df["fecha_limite_pago"] + timedelta(days=self.days_tolerance)

        ok("Facturas enriquecidas correctamente.")
        return df

    # ============================================================
    # BANCOS
    # ============================================================
    def process_bancos(self, df_bancos: pd.DataFrame) -> pd.DataFrame:
        info("Preparando movimientos bancarios…")
        df = self._normalize_df(df_bancos)

        # Fecha
        fecha_cols = [c for c in df.columns if c.lower() in ("fecha", "fecha_mov", "fechaoperacion")]
        df["Fecha"] = pd.to_datetime(df[fecha_cols[0]], errors="coerce") if fecha_cols else pd.NaT

        # Monto
        monto_cols = [c for c in df.columns if c.lower() in ("monto", "importe", "montototal")]
        df["Monto"] = pd.to_numeric(df[monto_cols[0]], errors="coerce").fillna(0) if monto_cols else 0

        # Moneda
        moneda_cols = [c for c in df.columns if c.lower() == "moneda"]
        df["moneda"] = df[moneda_cols[0]].astype(str).upper().str.strip() if moneda_cols else "PEN"
        df["es_dolares"] = df["moneda"].str.contains("USD|DOL", regex=True)

        # Conversión PEN
        df["Monto_PEN"] = df.apply(
            lambda r: round(r["Monto"] * self.tipo_cambio_usd_pen, 2) if r["es_dolares"] else round(r["Monto"], 2),
            axis=1
        )

        df["monto_variacion_min"] = df["Monto_PEN"] - self.monto_variacion
        df["monto_variacion_max"] = df["Monto_PEN"] + self.monto_variacion

        # Descripción y operación
        desc_cols = [c for c in df.columns if c.lower() in ("descripcion", "detalle", "glosa", "concepto")]
        df["Descripcion"] = df[desc_cols[0]].astype(str) if desc_cols else ""

        oper_cols = [c for c in df.columns if c.lower() in ("operacion", "nro_operacion", "referencia")]
        df["Operacion"] = df[oper_cols[0]].astype(str) if oper_cols else ""

        # IA opcional
        if self.activar_ia:
            info("Clasificando movimientos con IA…")

            def safe_ai(desc):
                try:
                    return ai_classify(desc) if desc else {"tipo": "otro", "probabilidad": 0, "justificacion": "sin descripción"}
                except Exception as e:
                    warn(f"Error IA: {e}")
                    return {"tipo": "otro", "probabilidad": 0.2, "justificacion": "fallback"}

            temp = df["Descripcion"].apply(safe_ai)
            df["tipo_operacion_ia"] = temp.apply(lambda x: x.get("tipo"))
            df["tipo_operacion_ia_prob"] = temp.apply(lambda x: x.get("probabilidad"))
            df["tipo_operacion_ia_justificacion"] = temp.apply(lambda x: x.get("justificacion"))
        else:
            df["tipo_operacion_ia"] = "otro"
            df["tipo_operacion_ia_prob"] = 0.0
            df["tipo_operacion_ia_justificacion"] = "IA desactivada"

        ok("Movimientos bancarios listos.")
        return df


# ============================================================
#  API — COMPATIBILIDAD CON DATAMAPPER
# ============================================================
def preparar_factura_para_insert(subtotal: float, extra_campos: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recibe el subtotal y campos adicionales, y retorna
    una factura lista para insertar en la BD destino.
    """

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
) -> Dict[str, Any]:
    """
    Retorna un movimiento bancario estandarizado para
    la tabla master de bancos.
    """

    movimiento = {
        "monto": monto,
        "moneda": moneda,
        "codigo_banco": codigo_banco,
    }

    movimiento.update(extra_campos)
    return movimiento
