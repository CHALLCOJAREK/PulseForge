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
#  CALCULATOR · MOTOR FINANCIERO PULSEFORGE 2025 — PREMIUM
# ============================================================
class Calculator:

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        self.cfg = cfg or get_config()

        # Parámetros contables
        self.igv = float(validate_igv(self.cfg.parametros.igv))
        self.detraccion = float(validate_detraccion(self.cfg.parametros.detraccion))
        self.tipo_cambio = float(validate_tipo_cambio(self.cfg.parametros.tipo_cambio_usd_pen))

        # Tolerancia
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
    #   PROCESO FACTURAS – CÁLCULO FINANCIERO COMPLETO
    # ============================================================
    def process_facturas(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando motor financiero a facturas…")

        df = self._normalize_df(df_facturas)

        # Validación
        if "subtotal" not in df.columns:
            raise ValueError("Falta columna 'subtotal' en facturas_pf")

        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)

        df["fecha_emision"] = pd.to_datetime(df.get("fecha_emision"), errors="coerce")
        df["vencimiento"] = pd.to_datetime(df.get("vencimiento"), errors="coerce")

        # Días crédito
        df["dias_credito"] = (df["vencimiento"] - df["fecha_emision"]).dt.days
        df.loc[df["dias_credito"] < 0, "dias_credito"] = None

        df["fecha_pago"] = df["vencimiento"]

        # Cálculo financiero
        df["igv"] = (df["subtotal"] * self.igv).round(2)
        df["total_con_igv"] = (df["subtotal"] + df["igv"]).round(2)
        df["detraccion_monto"] = (df["total_con_igv"] * self.detraccion).round(2)
        df["neto_recibido"] = (df["total_con_igv"] - df["detraccion_monto"]).round(2)

        df["tiene_detraccion"] = df["detraccion_monto"] > 0

        df["ventana_inicio"] = df["fecha_pago"] - pd.to_timedelta(self.days_tolerance, unit="D")
        df["ventana_fin"] = df["fecha_pago"] + pd.to_timedelta(self.days_tolerance, unit="D")

        df.loc[df["fecha_pago"].isna(), ["ventana_inicio", "ventana_fin"]] = None

        ok("Facturas procesadas correctamente.")
        return df

    # ============================================================
    #   PREMIUM: GUARDAR CÁLCULOS EN BD + ACTUALIZAR FACTURAS
    # ============================================================
    def save_calculos(self, df_facturas: pd.DataFrame):
        """
        Guarda cálculos en calculos_pf y sincroniza facturas_pf.
        Totalmente seguro contra NaT.
        """
        from src.core.db import PulseForgeDB, DatabaseError
        db = PulseForgeDB()

        if "source_hash" not in df_facturas.columns:
            warn("No existe columna 'source_hash'. No se pueden guardar cálculos.")
            return

        info(f"Guardando cálculos financieros → {len(df_facturas)} filas…")

        registros = df_facturas.to_dict("records")

        for row in registros:
            factura_hash = row.get("source_hash")

            # --- FIX PREMIUM (Evita el NaTType error) ---
            fecha_pago_val = row.get("fecha_pago")
            if pd.isna(fecha_pago_val):
                fecha_pago_str = None
            else:
                fecha_pago_str = fecha_pago_val.strftime("%Y-%m-%d")

            data_calc = {
                "factura_hash": factura_hash,
                "subtotal": row.get("subtotal"),
                "igv": row.get("igv"),
                "total_final": row.get("total_con_igv"),
                "detraccion": row.get("detraccion_monto"),
                "dias_credito": row.get("dias_credito"),
                "fecha_pago": fecha_pago_str,
                "variacion": 0
            }

            try:
                db.insert("calculos_pf", data_calc)
            except DatabaseError:
                pass  # evitar duplicados

            # --- Sincronizar factura (pero NO tocar match_id) ---
            data_factura = {
                "igv": row.get("igv"),
                "total": row.get("total_con_igv")
            }

            try:
                db.update(
                    "facturas_pf",
                    data_factura,
                    "source_hash = ?",
                    (factura_hash,)
                )
            except Exception as e:
                warn(f"No se pudo actualizar factura {factura_hash}: {e}")

        ok("Cálculos persistidos y facturas sincronizadas.")

    # ============================================================
    #   PROCESO BANCOS
    # ============================================================
    def process_bancos(self, df_bancos: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando normalización bancaria…")

        df = df_bancos.copy()
        df.columns = [str(c).strip() for c in df.columns]

        fecha_col = next((c for c in df.columns if "fecha" in c.lower()), None)
        df["Fecha"] = pd.to_datetime(df[fecha_col], errors="coerce") if fecha_col else pd.NaT

        desc_col = next((c for c in df.columns if "desc" in c.lower() or "glosa" in c.lower()), None)
        df["Descripcion"] = df[desc_col].astype(str) if desc_col else ""

        oper_col = next((c for c in df.columns if "oper" in c.lower()), None)
        df["Operacion"] = df[oper_col].astype(str) if oper_col else ""

        monto_col = next((c for c in df.columns if c.lower() in ("monto", "importe", "abono", "cargo")), None)
        df["Monto"] = pd.to_numeric(df[monto_col], errors="coerce").fillna(0) if monto_col else 0

        mon_col = next((c for c in df.columns if c.lower() == "moneda"), None)
        df["moneda"] = df[mon_col].astype(str).str.upper().str.strip() if mon_col else "PEN"

        df["Monto_PEN"] = df["Monto"]
        try:
            mask_usd = df["moneda"].str.contains("USD|DOL", case=False)
            df.loc[mask_usd, "Monto_PEN"] = df.loc[mask_usd, "Monto"] * self.tipo_cambio
        except Exception as e:
            warn(f"No se pudo convertir USD → PEN: {e}")

        ok("Movimientos bancarios normalizados.")
        return df


# ============================================================
#   APIs PARA DATAMAPPER
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
