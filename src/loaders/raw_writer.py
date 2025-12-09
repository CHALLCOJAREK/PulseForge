# src/loaders/raw_writer.py
from __future__ import annotations

import sys
import sqlite3
from pathlib import Path
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


class RawWriter:
    """
    Escribe datos RAW (sin procesar) en la BD PulseForge.
    Inserta:
        - clientes_pf
        - facturas_pf
        - movimientos_pf
    """

    def __init__(self):
        cfg = get_config()
        self.db_path = Path(cfg.newdb_path)

        if not self.db_path.exists():
            error(f"BD destino no encontrada → {self.db_path}")
            raise FileNotFoundError(self.db_path)

        ok(f"RawWriter listo → {self.db_path}")

    # -----------------------------------------------------
    def _connect(self):
        return sqlite3.connect(self.db_path)

    # -----------------------------------------------------
    def insert_clientes(self, items: List[Dict[str, Any]]):
        conn = self._connect()
        cur = conn.cursor()

        for row in items:
            try:
                cur.execute("""
                    INSERT INTO clientes_pf (ruc, razon_social, source_hash)
                    VALUES (?, ?, ?)
                """, (
                    row.get("ruc"),
                    row.get("razon_social"),
                    row.get("source_hash"),
                ))
            except Exception as e:
                warn(f"No se pudo insertar cliente → {e}")

        conn.commit()
        conn.close()
        ok(f"Clientes insertados → {len(items)}")

    # -----------------------------------------------------
    def insert_facturas(self, items: List[Dict[str, Any]]):
        conn = self._connect()
        cur = conn.cursor()

        for row in items:
            try:
                cur.execute("""
                    INSERT INTO facturas_pf (
                        ruc, cliente_generador, serie, numero, combinada,
                        fecha_emision, vencimiento,
                        subtotal, igv, total, detraccion, total_neto_cobrado,
                        fue_cobrado, fecha_cobro, match_id, source_hash
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get("ruc"),
                    row.get("cliente_generador"),
                    row.get("serie"),
                    row.get("numero"),
                    row.get("combinada"),
                    row.get("fecha_emision"),
                    row.get("vencimiento"),
                    row.get("subtotal"),
                    row.get("igv"),
                    row.get("total_con_igv") or row.get("total"),
                    row.get("detraccion_monto"),
                    row.get("neto_recibido"),
                    0,
                    None,
                    None,
                    row.get("source_hash"),
                ))
            except Exception as e:
                warn(f"No se pudo insertar factura → {e}")

        conn.commit()
        conn.close()
        ok(f"Facturas insertadas → {len(items)}")

    # -----------------------------------------------------
    def insert_movimientos(self, items: List[Dict[str, Any]]):
        conn = self._connect()
        cur = conn.cursor()

        for row in items:
            try:
                cur.execute("""
                    INSERT INTO movimientos_pf (
                        banco_codigo, fecha, tipo_mov, descripcion,
                        monto_original, moneda_original, monto_pen,
                        operacion, destinatario, tipo_documento,
                        es_cuenta_empresa, es_cuenta_detraccion, es_cuenta_principal,
                        source_hash
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get("banco_codigo"),
                    row.get("fecha"),
                    row.get("tipo_mov"),
                    row.get("descripcion"),
                    row.get("monto_original"),
                    row.get("moneda_original"),
                    row.get("monto_pen"),
                    row.get("operacion"),
                    row.get("destinatario"),
                    row.get("tipo_documento"),
                    row.get("es_cuenta_empresa"),
                    row.get("es_cuenta_detraccion"),
                    row.get("es_cuenta_principal"),
                    row.get("source_hash"),
                ))
            except Exception as e:
                warn(f"No se pudo insertar movimiento → {e}")

        conn.commit()
        conn.close()
        ok(f"Movimientos insertados → {len(items)}")
