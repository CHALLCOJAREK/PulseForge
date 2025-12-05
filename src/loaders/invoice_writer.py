# src/loaders/invoice_writer.py
from __future__ import annotations

import sys
import sqlite3
import hashlib
from pathlib import Path
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


# ============================================================
# HELPERS
# ============================================================

def _get_newdb_path() -> Path:
    db_path = str(get_env("PULSEFORGE_NEWDB_PATH", default="")).strip()
    if not db_path:
        error("PULSEFORGE_NEWDB_PATH no configurado en .env")
        raise ValueError("Falta PULSEFORGE_NEWDB_PATH")

    file = Path(db_path)
    file.parent.mkdir(parents=True, exist_ok=True)
    return file


def _get_connection() -> sqlite3.Connection:
    db = _get_newdb_path()
    if not db.exists():
        warn(f"La BD destino no existe aún → {db}")
    return sqlite3.connect(db)


def _hash_factura(fac: dict) -> str:
    base = f"{fac.get('ruc','')}|{fac.get('combinada','')}|{fac.get('fecha_emision','')}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
#  INVOICE WRITER (compatible con list[dict])
# ============================================================

class InvoiceWriter:

    REQUIRED_FIELDS = [
        "ruc",
        "cliente_generador",
        "subtotal",
        "serie",
        "numero",
        "combinada",
        "estado_fs",
        "estado_cont",
        "fecha_emision",
        "fecha_limite_pago",
        "fecha_inicio_ventana",
        "fecha_fin_ventana",
        "neto_recibido",
        "total_con_igv",
        "detraccion_monto",
    ]

    def __init__(self):
        info("Inicializando InvoiceWriter…")
        self.db_path = _get_newdb_path()
        ok(f"BD destino: {self.db_path}")

    # --------------------------------------------------------
    def _ensure_table(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='facturas_pf'
        """)
        if not cur.fetchone():
            error("La tabla facturas_pf NO existe. Ejecuta newdb_builder.py")
            raise RuntimeError("Tabla facturas_pf no encontrada.")

    # --------------------------------------------------------
    def _normalize_record(self, fac: dict) -> dict:
        """
        Limpia 1 factura y asegura todas las columnas obligatorias.
        """
        rec = fac.copy()

        # Asegurar todas las columnas obligatorias
        for f in self.REQUIRED_FIELDS:
            if f not in rec:
                warn(f"[FACTURA] Falta '{f}' → se usa None.")
                rec[f] = None

        # Normalizar tipos numéricos
        for n in ["subtotal", "neto_recibido", "total_con_igv", "detraccion_monto"]:
            try:
                rec[n] = float(rec.get(n, 0) or 0)
            except:
                rec[n] = 0

        # Todas las fechas a texto YYYY-MM-DD
        for f in [
            "fecha_emision",
            "fecha_limite_pago",
            "fecha_inicio_ventana",
            "fecha_fin_ventana",
        ]:
            v = rec.get(f)
            rec[f] = str(v) if v not in (None, "", "NaT") else None

        # Estado pago
        rec["estado_pago"] = rec.get("estado_pago", "pendiente") or "pendiente"

        # Hash trazabilidad
        rec["source_hash"] = _hash_factura(rec)

        return rec

    # --------------------------------------------------------
    def save_facturas(self, facturas: List[Dict], reset=False) -> int:
        """
        Inserta facturas (list[dict]) en facturas_pf.
        """
        if not facturas:
            warn("Lista de facturas vacía. No se insertará nada.")
            return 0

        # Normalizar cada registro
        registros = [self._normalize_record(f) for f in facturas]

        conn = _get_connection()
        self._ensure_table(conn)
        cur = conn.cursor()

        if reset:
            warn("Reset=True → limpiando tabla facturas_pf")
            cur.execute("DELETE FROM facturas_pf")

        info("Insertando facturas…")

        cur.executemany("""
            INSERT INTO facturas_pf (
                ruc,
                cliente_generador,
                subtotal,
                serie,
                numero,
                combinada,
                estado_fs,
                estado_cont,
                fecha_emision,
                fecha_limite_pago,
                fecha_inicio_ventana,
                fecha_fin_ventana,
                neto_recibido,
                total_con_igv,
                detraccion_monto,
                estado_pago,
                source_hash
            )
            VALUES (
                :ruc,
                :cliente_generador,
                :subtotal,
                :serie,
                :numero,
                :combinada,
                :estado_fs,
                :estado_cont,
                :fecha_emision,
                :fecha_limite_pago,
                :fecha_inicio_ventana,
                :fecha_fin_ventana,
                :neto_recibido,
                :total_con_igv,
                :detraccion_monto,
                :estado_pago,
                :source_hash
            )
        """, registros)

        conn.commit()
        inserted = len(registros)
        ok(f"Facturas insertadas: {inserted}")
        conn.close()
        return inserted

    # Alias
    def save(self, facturas: List[Dict], reset=False) -> int:
        return self.save_facturas(facturas, reset)
