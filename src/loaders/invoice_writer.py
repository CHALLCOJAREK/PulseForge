# src/loaders/invoice_writer.py
from __future__ import annotations

import sqlite3
from pathlib import Path
import sys
import hashlib

# Bootstrap
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


TABLE_NAME = "facturas_pf"


# ============================================================
#                INVOICE WRITER · PULSEFORGE 2025
# ============================================================
class InvoiceWriter:

    def __init__(self):
        db_path = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()

        if not db_path:
            raise ValueError("[InvoiceWriter] ❌ Falta PULSEFORGE_NEWDB_PATH en .env")

        self.db_path = Path(db_path)
        info(f"[InvoiceWriter] BD destino → {self.db_path}")

        self._ensure_table()


    # ============================================================
    #              CREAR TABLA E ÍNDICES SI NO EXISTEN
    # ============================================================
    def _ensure_table(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info("[InvoiceWriter] Verificando tabla pf_invoices…")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_hash TEXT UNIQUE,
                ruc TEXT,
                cliente_generador TEXT,
                serie TEXT,
                numero TEXT,
                combinada TEXT,
                fecha_emision TEXT,
                vencimiento TEXT,
                subtotal REAL,
                igv REAL,
                total REAL,
                estado_fs TEXT,
                estado_cont TEXT,
                fue_cobrado INTEGER,
                match_id TEXT
            );
        """)

        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_inv_hash      ON {TABLE_NAME}(source_hash);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_inv_serie_num ON {TABLE_NAME}(serie, numero);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_inv_ruc       ON {TABLE_NAME}(ruc);")

        conn.commit()
        conn.close()

        ok("[InvoiceWriter] Tabla lista ✔")


    # ============================================================
    #                       GENERAR HASH
    # ============================================================
    def _make_hash(self, f: dict) -> str:
        """
        Genera un hash único en base a los campos fundamentales de una factura.
        Inmutable y consistente.
        """
        base = (
            f"{f.get('ruc','')}"
            f"|{f.get('serie','')}"
            f"|{f.get('numero','')}"
            f"|{f.get('subtotal','')}"
            f"|{f.get('total','')}"
        )
        return hashlib.sha256(base.encode("utf-8")).hexdigest()


    # ============================================================
    #                         VALIDACIÓN
    # ============================================================
    def _validate_factura(self, f: dict) -> bool:
        required = ["ruc", "subtotal", "igv", "total"]

        for key in required:
            if key not in f or f[key] in ["", None]:
                warn(f"[InvoiceWriter] Factura omitida → falta campo: {key}")
                return False

        return True


    # ============================================================
    #                     GUARDAR MUCHAS FACTURAS
    # ============================================================
    def save_many(self, facturas: list[dict]):
        if not facturas:
            warn("[InvoiceWriter] No hay facturas para guardar.")
            return

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info(f"[InvoiceWriter] Guardando {len(facturas)} facturas…")

        try:
            sql = f"""
                INSERT OR REPLACE INTO {TABLE_NAME} (
                    source_hash, ruc, cliente_generador, serie, numero, combinada,
                    fecha_emision, vencimiento, subtotal, igv, total,
                    estado_fs, estado_cont, fue_cobrado, match_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """

            validas = 0

            for f in facturas:

                if not self._validate_factura(f):
                    continue

                # 🚀 Generar hash automático si no existe
                if not f.get("source_hash"):
                    f["source_hash"] = self._make_hash(f)

                cur.execute(sql, (
                    f["source_hash"],
                    f["ruc"],
                    f.get("cliente_generador"),
                    f.get("serie"),
                    f.get("numero"),
                    f.get("combinada"),
                    str(f["fecha_emision"]) if f.get("fecha_emision") else None,
                    str(f["vencimiento"]) if f.get("vencimiento") else None,
                    f.get("subtotal"),
                    f.get("igv"),
                    f.get("total"),
                    f.get("estado_fs"),
                    f.get("estado_cont"),
                    f.get("fue_cobrado", 0),
                    f.get("match_id")
                ))

                validas += 1

            conn.commit()
            ok(f"[InvoiceWriter] ✔ Facturas guardadas: {validas}")

        except Exception as e:
            conn.rollback()
            error(f"[InvoiceWriter] ❌ Error guardando facturas → {e}")

        finally:
            conn.close()
