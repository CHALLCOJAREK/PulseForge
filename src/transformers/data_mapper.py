# src/transformers/data_mapper.py
from __future__ import annotations

import sys
import pandas as pd
import hashlib
from pathlib import Path
from json import load

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.utils import clean_amount
from src.transformers.calculator import (
    preparar_factura_para_insert,
    preparar_movimiento_bancario_para_insert
)


class DataMapper:

    def __init__(self):
        info("Inicializando DataMapper PulseForge…")

        config_path = ROOT / "config" / "settings.json"
        if not config_path.exists():
            error("settings.json no encontrado — DataMapper no puede iniciar.")
            raise FileNotFoundError("settings.json no encontrado")

        with open(config_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        self.cols_fact = self.settings["columnas_facturas"]
        self.cols_bank = self.settings["columnas_bancos"]
        self.map_tablas_bancos = self.settings["tablas_bancos"]

        ok("DataMapper cargado correctamente.")

    # ================================================
    # HASH ÚNICO
    # ================================================
    def _make_hash(self, fila: dict) -> str:
        try:
            base = "|".join([str(fila.get(k, "")) for k in sorted(fila.keys())])
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception as e:
            warn(f"Error generando hash: {e}")
            return ""

    # ================================================
    # CLIENTES
    # ================================================
    def map_clientes(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando clientes…")

        clientes = []
        for _, row in df.iterrows():
            ruc = str(row.get("ruc", "")).strip()
            rs = str(row.get("razon_social", "")).strip()

            if not ruc:
                continue

            entry = {
                "ruc": ruc,
                "razon_social": rs,
                "source_hash": self._make_hash({"ruc": ruc, "razon_social": rs})
            }

            clientes.append(entry)

        ok(f"Clientes mapeados: {len(clientes)}")
        return clientes

    # ================================================
    # FACTURAS
    # ================================================
    def map_facturas(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando facturas…")

        facturas = []
        c = self.cols_fact

        for _, row in df.iterrows():
            try:
                subtotal = clean_amount(row.get(c["subtotal"]))

                factura = preparar_factura_para_insert(
                    subtotal=subtotal,
                    extra_campos={
                        "ruc": str(row.get(c["ruc"], "")).strip(),
                        "cliente_generador": str(row.get(c["cliente_generador"], "")).strip(),
                        "serie": str(row.get(c["serie"], "")).strip(),
                        "numero": str(row.get(c["numero"], "")).strip(),
                        "combinada": str(row.get(c["combinada"], "")).strip(),
                        "fecha_emision": str(row.get(c["fecha_emision"], "")),
                        "vencimiento": str(row.get(c["vencimiento"], "")),
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

    # ================================================
    # MOVIMIENTOS BANCARIOS
    # ================================================
    def map_bancos(self, df: pd.DataFrame, nombre_tabla: str) -> list[dict]:
        info(f"Mapeando banco desde tabla '{nombre_tabla}'…")

        movimientos = []

        banco_codigo = next(
            (cod for cod, tabla_real in self.map_tablas_bancos.items() if tabla_real == nombre_tabla),
            None
        )

        if not banco_codigo:
            error(f"No se pudo identificar banco para {nombre_tabla}")
            return []

        oper_cols = self.cols_bank["operacion"]

        for _, row in df.iterrows():
            try:
                monto = clean_amount(row.get(self.cols_bank["monto"]))
                moneda = str(row.get(self.cols_bank["moneda"], "")).upper().strip()

                # operación profesional multi-columna
                valor_operacion = ""
                if isinstance(oper_cols, list):
                    valor_operacion = next(
                        (str(row[col]).strip() for col in oper_cols if col in row and pd.notna(row[col])),
                        ""
                    )
                else:
                    valor_operacion = str(row.get(oper_cols, "")).strip()

                movimiento = preparar_movimiento_bancario_para_insert(
                    monto=monto,
                    moneda=moneda,
                    codigo_banco=banco_codigo,
                    extra_campos={
                        "fecha": str(row.get(self.cols_bank["fecha"], "")),
                        "tipo_mov": str(row.get(self.cols_bank["tipo_mov"], "")).strip(),
                        "descripcion": str(row.get(self.cols_bank["descripcion"], "")).strip(),
                        "operacion": valor_operacion,
                        "destinatario": str(row.get(self.cols_bank["destinatario"], "")).strip(),
                        "tipo_documento": str(row.get(self.cols_bank["tipo_documento"], "")).strip(),
                    }
                )

                movimiento["source_hash"] = self._make_hash(movimiento)
                movimientos.append(movimiento)

            except Exception as e:
                warn(f"Movimiento bancario con error → {e}")

        ok(f"Movimientos mapeados: {len(movimientos)}")
        return movimientos
