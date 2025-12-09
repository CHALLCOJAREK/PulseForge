# src/transformers/data_mapper.py
from __future__ import annotations

import sys
import pandas as pd
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# CORE
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.utils import clean_amount

# CALCULATOR API (cálculos financieros)
from src.transformers.calculator import (
    preparar_factura_para_insert,
    preparar_movimiento_bancario_para_insert
)


# ============================================================
#   DATAMAPPER · TRANSFORMADOR CENTRAL
#   - No toca BD
#   - No lee settings.json directamente
#   - Usa get_config() como arquitectura PulseForge V2
# ============================================================
class DataMapper:

    def __init__(self):
        info("Inicializando DataMapper PulseForge…")

        cfg = get_config()

        # Se extraen SOLO las partes necesarias del config
        self.cols_fact = cfg.columnas_facturas or {}
        self.cols_bank = cfg.columnas_bancos or {}
        self.map_tablas_bancos = cfg.bancos or {}

        if not self.cols_fact:
            error("columnas_facturas no definidas en configuración.")
            raise KeyError("Falta columnas_facturas en config.")

        if not self.cols_bank:
            warn("columnas_bancos no definidas. Mapper bancario limitado.")

        ok("DataMapper cargado correctamente.")


    # ============================================================
    #   HASH ÚNICO (estable)
    # ============================================================
    def _make_hash(self, fila: dict) -> str:
        try:
            base = "|".join([str(fila.get(k, "")) for k in sorted(fila.keys())])
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception as e:
            warn(f"Error generando hash: {e}")
            return ""


    # ============================================================
    #   MAPEO CLIENTES
    # ============================================================
    def map_clientes(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando clientes…")

        clientes = []
        for _, row in df.iterrows():

            ruc = str(row.get("ruc", "")).strip()
            razon = str(row.get("razon_social", "")).strip()

            if not ruc:
                continue

            item = {
                "ruc": ruc,
                "razon_social": razon,
                "source_hash": self._make_hash({"ruc": ruc, "razon_social": razon})
            }

            clientes.append(item)

        ok(f"Clientes mapeados: {len(clientes)}")
        return clientes


    # ============================================================
    #   MAPEO FACTURAS
    # ============================================================
    def map_facturas(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando facturas…")

        facturas = []
        c = self.cols_fact

        for _, row in df.iterrows():
            try:
                subtotal = clean_amount(row.get(c.get("subtotal"), 0))

                factura = preparar_factura_para_insert(
                    subtotal=subtotal,
                    extra_campos={
                        "ruc": str(row.get(c.get("ruc"), "")).strip(),
                        "cliente_generador": str(row.get(c.get("cliente_generador"), "")).strip(),
                        "serie": str(row.get(c.get("serie"), "")).strip(),
                        "numero": str(row.get(c.get("numero"), "")).strip(),
                        "combinada": str(row.get(c.get("combinada"), "")).strip(),
                        "fecha_emision": str(row.get(c.get("fecha_emision"), "")),
                        "vencimiento": str(row.get(c.get("vencimiento"), "")),
                    }
                )

                factura["fue_cobrado"] = 0
                factura["match_id"] = None
                factura["source_hash"] = self._make_hash(factura)

                facturas.append(factura)

            except Exception as e:
                warn(f"Factura con error → {e}")

        ok(f"Facturas mapeadas: {len(facturas)}")
        return facturas


    # ============================================================
    #   MAPEO MOVIMIENTOS BANCARIOS
    # ============================================================
    def map_bancos(self, df: pd.DataFrame, nombre_tabla: str) -> list[dict]:
        info(f"Mapeando banco desde tabla '{nombre_tabla}'…")

        movimientos = []

        # Resolver alias → BN, BBVA, BCP...
        banco_codigo = next(
            (alias for alias, tabla in self.map_tablas_bancos.items() if tabla == nombre_tabla),
            None
        )

        if not banco_codigo:
            warn(f"No se identificó banco para {nombre_tabla}")
            return []

        c = self.cols_bank

        for _, row in df.iterrows():
            try:
                monto = clean_amount(row.get(c.get("monto")))
                moneda = str(row.get(c.get("moneda"), "")).upper().strip()

                # Operación dinámica
                oper_cols = c.get("operacion")
                oper = ""

                if isinstance(oper_cols, list):
                    for col in oper_cols:
                        if col in row and pd.notna(row[col]):
                            oper = str(row[col]).strip()
                            break
                else:
                    oper = str(row.get(oper_cols, "")).strip()

                mov = preparar_movimiento_bancario_para_insert(
                    monto=monto,
                    moneda=moneda,
                    codigo_banco=banco_codigo,
                    extra_campos={
                        "fecha": str(row.get(c.get("fecha"), "")),
                        "tipo_mov": str(row.get(c.get("tipo_mov"), "")).strip(),
                        "descripcion": str(row.get(c.get("descripcion"), "")).strip(),
                        "operacion": oper,
                        "destinatario": str(row.get(c.get("destinatario"), "")).strip(),
                        "tipo_documento": str(row.get(c.get("tipo_documento"), "")).strip(),
                    }
                )

                mov["source_hash"] = self._make_hash(mov)
                movimientos.append(mov)

            except Exception as e:
                warn(f"Error mapeando banco {banco_codigo}: {e}")

        ok(f"Movimientos mapeados: {len(movimientos)}")
        return movimientos
