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
from typing import Optional

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, PulseForgeConfig
from src.core.validations import (
    validate_igv,
    validate_detraccion,
    validate_tipo_cambio,
)

from src.transformers.ai_helpers import ai_classify


# ============================================================
#  CALCULATOR · MOTOR FINANCIERO PULSEFORGE (FINAL)
# ============================================================
class Calculator:

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        self.cfg: PulseForgeConfig = cfg or get_config()

        # Carga segura de configuración
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
        Retorna días de crédito a partir de:
          - CONTADO → 0
          - CREDITO → 0
          - CREDITO 30 dias → 30
          - 30D, 15d → 30, 15
          - 0, 15, 30 (enteros)
        """
        if value is None:
            return 0

        if isinstance(value, (int, float)):
            return int(value)

        txt = str(value).strip().lower()
        if not txt:
            return 0

        if "contado" in txt:
            return 0

        if "credito" in txt and not any(ch.isdigit() for ch in txt):
            return 0

        digits = "".join(ch for ch in txt if ch.isdigit())
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

        # -----------------------------
        # Validaciones mínimas
        # -----------------------------
        if "subtotal" not in df.columns:
            error("Falta columna obligatoria 'subtotal'.")
            raise ValueError("No existe 'subtotal' en facturas.")

        if "fecha_emision" not in df.columns:
            error("Falta columna obligatoria 'fecha_emision'.")
            raise ValueError("No existe 'fecha_emision' en facturas.")

        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)
        df["fecha_emision"] = pd.to_datetime(df["fecha_emision"], errors="coerce")

        # Forma de pago
        if "forma_pago" not in df.columns:
            warn("Facturas sin 'forma_pago'. Se asume CONTADO.")
            df["forma_pago"] = ""

        df["dias_pago"] = df["forma_pago"].apply(self._parse_forma_pago)

        # -----------------------------
        # SIEMPRE procesamos TODAS las facturas
        # -----------------------------
        df["igv"] = (df["subtotal"] * self.igv).round(2)
        df["total_con_igv"] = (df["subtotal"] + df["igv"]).round(2)
        df["detraccion_monto"] = (df["total_con_igv"] * self.detraccion).round(2)
        df["neto_recibido"] = (df["total_con_igv"] - df["detraccion_monto"]).round(2)

        df["tiene_detraccion"] = df["detraccion_monto"] > 0

        # Ventanas temporales exactas que usa el Matcher
        df["fecha_limite_pago"] = df["fecha_emision"] + df["dias_pago"].apply(lambda d: timedelta(days=int(d)))
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

        # -----------------------------
        # Fecha
        # -----------------------------
        fecha_cols = [c for c in df.columns if c.lower() in ("fecha", "fecha_mov", "fechaoperacion")]
        if fecha_cols:
            df["Fecha"] = pd.to_datetime(df[fecha_cols[0]], errors="coerce")
        else:
            warn("Banco sin columna clara de fecha → Fecha = NaT")
            df["Fecha"] = pd.NaT

        # -----------------------------
        # Monto
        # -----------------------------
        monto_cols = [c for c in df.columns if c.lower() in ("monto", "importe", "montototal")]
        df["Monto"] = pd.to_numeric(df[monto_cols[0]], errors="coerce").fillna(0) if monto_cols else 0

        # -----------------------------
        # Moneda
        # -----------------------------
        moneda_cols = [c for c in df.columns if c.lower() == "moneda"]
        df["moneda"] = df[moneda_cols[0]].astype(str).upper().str.strip() if moneda_cols else "PEN"

        df["es_dolares"] = df["moneda"].str.contains("USD") | df["moneda"].str.contains("DOL")

        # Conversión
        df["Monto_PEN"] = df.apply(
            lambda r: round(float(r["Monto"]) * self.tipo_cambio_usd_pen, 2) if r["es_dolares"] else round(float(r["Monto"]), 2),
            axis=1
        )

        df["monto_variacion_min"] = df["Monto_PEN"] - self.monto_variacion
        df["monto_variacion_max"] = df["Monto_PEN"] + self.monto_variacion

        # -----------------------------
        # Descripción y operación
        # -----------------------------
        desc_cols = [c for c in df.columns if c.lower() in ("descripcion", "detalle", "glosa", "concepto")]
        df["Descripcion"] = df[desc_cols[0]].astype(str) if desc_cols else ""

        oper_cols = [c for c in df.columns if c.lower() in ("operacion", "nro_operacion", "referencia")]
        df["Operacion"] = df[oper_cols[0]].astype(str) if oper_cols else ""

        # -----------------------------
        # Clasificación IA segura
        # -----------------------------
        if self.activar_ia:
            info("Clasificando movimientos con IA…")

            def _safe_ai(desc: str):
                if not desc:
                    return {"tipo": "otro", "probabilidad": 0, "justificacion": "Sin descripción."}
                try:
                    return ai_classify(desc)
                except Exception as e:
                    warn(f"Error IA: {e}")
                    return {"tipo": "otro", "probabilidad": 0.3, "justificacion": "IA fallback"}

            temp = df["Descripcion"].apply(_safe_ai)
            df["tipo_operacion_ia"] = temp.apply(lambda r: r.get("tipo"))
            df["tipo_operacion_ia_prob"] = temp.apply(lambda r: r.get("probabilidad"))
            df["tipo_operacion_ia_justificacion"] = temp.apply(lambda r: r.get("justificacion"))
        else:
            df["tipo_operacion_ia"] = "otro"
            df["tipo_operacion_ia_prob"] = 0.0
            df["tipo_operacion_ia_justificacion"] = "IA desactivada"

        ok("Movimientos bancarios listos.")
        return df
