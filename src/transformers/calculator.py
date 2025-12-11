# src/transformers/calculator.py
from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd
from typing import Optional, Dict, Any

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ------------------------------------------------------------
# Core
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, PulseForgeConfig
from src.core.validations import validate_igv, validate_detraccion, validate_tipo_cambio


# ============================================================
#  CALCULATOR · MOTOR FINANCIERO PULSEFORGE 2025
# ============================================================
class Calculator:

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        self.cfg = cfg or get_config()

        # Parámetros contables (settings.json)
        self.igv = float(validate_igv(self.cfg.parametros.igv))
        self.detraccion = float(validate_detraccion(self.cfg.parametros.detraccion))
        self.tipo_cambio = float(validate_tipo_cambio(self.cfg.parametros.tipo_cambio_usd_pen))

        # Parámetros de ventana de tolerancia
        self.monto_variacion = float(self.cfg.parametros.monto_variacion)
        self.days_tolerance = int(self.cfg.parametros.dias_tolerancia_pago)

        ok("Calculator cargado con parámetros financieros oficiales.")

    # ------------------------------------------------------------
    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # ============================================================
    #   PROCESO FACTURAS – USANDO SOLO LA DATA DE LA BD DESTINO
    # ============================================================
    def process_facturas(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando motor financiero a facturas (BD destino)…")

        df = self._normalize_df(df_facturas)

        # --------------------------------------------------------
        # VALIDACIÓN BÁSICA
        # --------------------------------------------------------
        if "subtotal" not in df.columns:
            raise ValueError("Falta columna 'subtotal' en facturas_pf")

        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)

        # --------------------------------------------------------
        # FECHAS ORIGINALES DE LA BD DESTINO
        # --------------------------------------------------------
        df["fecha_emision"] = pd.to_datetime(df.get("fecha_emision"), errors="coerce")
        df["vencimiento"] = pd.to_datetime(df.get("vencimiento"), errors="coerce")

        # --------------------------------------------------------
        # CALCULAR DÍAS DE CRÉDITO A PARTIR DE (vencimiento - fecha_emision)
        # --------------------------------------------------------
        df["dias_credito"] = (df["vencimiento"] - df["fecha_emision"]).dt.days

        # Si la diferencia da negativa o no se puede calcular → dejar NULL
        df.loc[df["dias_credito"] < 0, "dias_credito"] = None

        # --------------------------------------------------------
        # FECHA DE PAGO = VENCIMIENTO
        # Si vencimiento es NULL → fecha_pago también será NULL
        # --------------------------------------------------------
        df["fecha_pago"] = df["vencimiento"]

        # --------------------------------------------------------
        # CÁLCULOS FINANCIEROS (A PARTIR DEL SUBTOTAL)
        # --------------------------------------------------------
        df["igv"] = (df["subtotal"] * self.igv).round(2)
        df["total_con_igv"] = (df["subtotal"] + df["igv"]).round(2)
        df["detraccion_monto"] = (df["total_con_igv"] * self.detraccion).round(2)
        df["neto_recibido"] = (df["total_con_igv"] - df["detraccion_monto"]).round(2)

        df["tiene_detraccion"] = df["detraccion_monto"] > 0

        # --------------------------------------------------------
        # VENTANAS DE MATCH (solo si existe fecha_pago)
        # --------------------------------------------------------
        df["ventana_inicio"] = df["fecha_pago"] - pd.to_timedelta(self.days_tolerance, unit="D")
        df["ventana_fin"] = df["fecha_pago"] + pd.to_timedelta(self.days_tolerance, unit="D")

        # Donde fecha_pago es NULL → ventanas deben ser NULL
        df.loc[df["fecha_pago"].isna(), ["ventana_inicio", "ventana_fin"]] = None

        ok("Facturas procesadas correctamente.")
        return df
        # ============================================================
    #   NUEVO: GUARDAR CÁLCULOS EN LA TABLA calculos_pf
    # ============================================================
    def save_calculos(self, df_facturas: pd.DataFrame):
        """
        Guarda resultados del cálculo financiero en la tabla calculos_pf.
        Una fila por factura. Modelo completamente persistente.
        """

        from src.core.db import PulseForgeDB, DatabaseError
        db = PulseForgeDB()

        info(f"Guardando cálculos → {len(df_facturas)} filas…")

        required_cols = [
            "source_hash",      # identificador único de factura
            "subtotal",
            "igv",
            "total_con_igv",
            "detraccion_monto",
            "neto_recibido",
            "dias_credito",
            "fecha_pago",
        ]

        # Validación
        for col in required_cols:
            if col not in df_facturas.columns:
                warn(f"⚠ No se encontró columna requerida: {col}. Se usará None.")
                df_facturas[col] = None

        registros = df_facturas.to_dict("records")

        for row in registros:
            data = {
                "factura_hash": row.get("source_hash"),
                "subtotal": row.get("subtotal"),
                "igv": row.get("igv"),
                "total_final": row.get("total_con_igv"),
                "detraccion": row.get("detraccion_monto"),
                "dias_credito": row.get("dias_credito"),
                "fecha_pago": row.get("fecha_pago").strftime("%Y-%m-%d") if row.get("fecha_pago") else None,
                "variacion": 0  # opcional, según reglas contables
            }

            try:
                db.insert("calculos_pf", data)
            except DatabaseError as e:
                warn(f"Fila omitida (posible duplicado) → {e}")

        ok("Cálculos guardados satisfactoriamente en calculos_pf.")

    # ============================================================
    #   PROCESO BANCOS — NORMALIZACIÓN ESTÁNDAR
    # ============================================================
    def process_bancos(self, df_bancos: pd.DataFrame) -> pd.DataFrame:
        """Normaliza movimientos bancarios para que el matcher los entienda."""
        info("Aplicando motor de normalización bancaria…")

        df = df_bancos.copy()
        df.columns = [str(c).strip() for c in df.columns]

        # --------------------------------------------------------
        # FECHA
        # --------------------------------------------------------
        fecha_col = next((c for c in df.columns if c.lower() in ("fecha", "f_operacion", "fecha_operacion",
                                                                 "fec_mov", "fecha movimiento")), None)

        if fecha_col:
            df["Fecha"] = pd.to_datetime(df[fecha_col], errors="coerce")
        else:
            warn("No se encontró columna de fecha en bancos. Se asignará NaT.")
            df["Fecha"] = pd.NaT

        # --------------------------------------------------------
        # DESCRIPCIÓN
        # --------------------------------------------------------
        desc_col = next((c for c in df.columns
                         if c.lower() in ("descripcion", "detalle", "glosa", "descripcion_mov", "concepto")), None)

        df["Descripcion"] = df[desc_col].astype(str) if desc_col else ""

        # --------------------------------------------------------
        # OPERACIÓN
        # --------------------------------------------------------
        oper_col = next((c for c in df.columns
                         if c.lower() in ("operacion", "nro_operacion", "referencia", "codigo_operacion")), None)

        df["Operacion"] = df[oper_col].astype(str) if oper_col else ""

        # --------------------------------------------------------
        # MONTO
        # --------------------------------------------------------
        monto_col = next((c for c in df.columns
                           if c.lower() in ("monto", "importe", "abono", "cargo", "valor")), None)

        if monto_col:
            df["Monto"] = pd.to_numeric(df[monto_col], errors="coerce").fillna(0)
        else:
            warn("No se encontró columna de monto. Se asignará 0.")
            df["Monto"] = 0

        # --------------------------------------------------------
        # MONEDA
        # --------------------------------------------------------
        mon_col = next((c for c in df.columns if c.lower() == "moneda"), None)
        if mon_col:
            df["moneda"] = df[mon_col].astype(str).str.upper().str.strip()
        else:
            df["moneda"] = "PEN"

        # --------------------------------------------------------
        # CONVERSIÓN A PEN SI CORRESPONDE
        # --------------------------------------------------------
        df["Monto_PEN"] = df["Monto"]

        try:
            mask_usd = df["moneda"].str.upper().isin(["USD", "$", "DOLARES"])
            df.loc[mask_usd, "Monto_PEN"] = df.loc[mask_usd, "Monto"] * self.tipo_cambio
        except Exception as e:
            warn(f"No se pudo convertir USD a PEN: {e}")

        ok("Movimientos bancarios normalizados correctamente.")
        return df


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
