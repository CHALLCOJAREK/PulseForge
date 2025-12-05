# src/loaders/bank_writer.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · BANK WRITER (COMPATIBLE CON list[dict])
# ============================================================
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


# ------------------------------------------------------------
#  HELPERS
# ------------------------------------------------------------
def _get_newdb_path() -> Path:
    db_path = str(get_env("PULSEFORGE_NEWDB_PATH", default="")).strip()
    if not db_path:
        error("PULSEFORGE_NEWDB_PATH no configurado")
        raise ValueError("Falta PULSEFORGE_NEWDB_PATH")

    file = Path(db_path)
    file.parent.mkdir(parents=True, exist_ok=True)
    return file


def _get_connection() -> sqlite3.Connection:
    db = _get_newdb_path()
    if not db.exists():
        warn(f"⚠ BD destino no existe aún → {db}")
    return sqlite3.connect(db)


def _hash_mov(m: dict) -> str:
    base = f"{m.get('Banco','')}|{m.get('Operacion','')}|{m.get('Fecha','')}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
#  BANK WRITER (NUEVA ARQUITECTURA)
# ============================================================
class BankWriter:

    REQUIRED_FIELDS = [
        "Banco",
        "Fecha",
        "Descripcion",
        "Monto",
        "Moneda",
        "Operacion",
        "Tipo_Mov",
        "Destinatario",
        "Tipo_Documento",
    ]

    def __init__(self):
        info("Inicializando BankWriter…")
        self.db_path = _get_newdb_path()
        ok(f"BD destino: {self.db_path}")

    # --------------------------------------------------------
    def _ensure_table(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='movimientos_pf'
        """)
        if not cur.fetchone():
            error("Tabla movimientos_pf NO existe. Ejecuta newdb_builder.py")
            raise RuntimeError("movimientos_pf no encontrada.")

    # --------------------------------------------------------
    def _normalize_record(self, mov: dict) -> dict:
        """
        Recibe un diccionario y asegura que esté completo y limpio.
        """
        rec = mov.copy()

        # Asegurar columnas
        for col in self.REQUIRED_FIELDS:
            if col not in rec:
                warn(f"[BANK] Falta '{col}' → None")
                rec[col] = None

        # Normalizar monto
        try:
            rec["Monto"] = float(rec.get("Monto", 0) or 0)
        except:
            rec["Monto"] = 0

        # Normalizar fechas a string
        rec["Fecha"] = str(rec.get("Fecha") or "")

        # Convertir todo texto a str
        for t in [
            "Banco", "Descripcion", "Moneda", "Operacion",
            "Tipo_Mov", "Destinatario", "Tipo_Documento"
        ]:
            v = rec.get(t)
            rec[t] = str(v) if v not in (None, "") else ""

        # Hash trazabilidad
        rec["source_hash"] = _hash_mov(rec)

        return rec

    # --------------------------------------------------------
    def save_bancos(self, movimientos: List[Dict], reset=False) -> int:
        """
        Inserta movimientos bancarios (list[dict]) en movimientos_pf.
        """
        if not movimientos:
            warn("Lista de movimientos vacía.")
            return 0

        registros = [self._normalize_record(m) for m in movimientos]

        conn = _get_connection()
        self._ensure_table(conn)
        cur = conn.cursor()

        if reset:
            warn("Reset=True → limpiando movimientos_pf…")
            cur.execute("DELETE FROM movimientos_pf")

        info("Insertando movimientos bancarios…")

        cur.executemany("""
            INSERT INTO movimientos_pf (
                banco,
                fecha,
                descripcion,
                monto,
                moneda,
                operacion,
                tipo_mov,
                destinatario,
                tipo_documento,
                source_hash
            )
            VALUES (
                :Banco,
                :Fecha,
                :Descripcion,
                :Monto,
                :Moneda,
                :Operacion,
                :Tipo_Mov,
                :Destinatario,
                :Tipo_Documento,
                :source_hash
            )
        """, registros)

        conn.commit()
        inserted = len(registros)
        ok(f"Movimientos insertados: {inserted}")
        conn.close()
        return inserted

    # Alias
    def save(self, movimientos: List[Dict], reset=False) -> int:
        return self.save_bancos(movimientos, reset)
