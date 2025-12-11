# src/loaders/bank_writer.py
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


TABLE_NAME = "bancos_pf"


# ============================================================
#              BANK WRITER · PULSEFORGE 2025
# ============================================================
class BankWriter:

    def __init__(self):
        """Inicializa el writer y prepara la BD destino."""
        db_path = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()

        if not db_path:
            raise ValueError("[BankWriter] ❌ Falta PULSEFORGE_NEWDB_PATH en .env")

        self.db_path = Path(db_path)
        info(f"[BankWriter] BD destino → {self.db_path}")

        self._ensure_table()

    # ============================================================
    #               GENERADOR DE HASH
    # ============================================================
    def _make_hash(self, data: dict) -> str:
        """
        Crea un hash único basado en los valores importantes del movimiento.
        """
        base = "|".join([
            str(data.get("fecha", "")),
            str(data.get("tipo_mov", "")),
            str(data.get("descripcion", "")),
            str(data.get("operacion", "")),
            str(data.get("monto", "")),
            str(data.get("moneda", "")),
            str(data.get("banco_codigo", "")),
        ])

        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    # ============================================================
    #            CREAR TABLA (si no existe)
    # ============================================================
    def _ensure_table(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info("[BankWriter] Verificando tabla pf_bank_movs…")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_hash TEXT UNIQUE,
                fecha TEXT,
                tipo_mov TEXT,
                descripcion TEXT,
                operacion TEXT,
                destinatario TEXT,
                tipo_documento TEXT,
                monto REAL,
                moneda TEXT,
                banco_codigo TEXT
            );
        """)

        # Índices críticos para performance del match
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_bank_hash  ON {TABLE_NAME}(source_hash);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_bank_oper  ON {TABLE_NAME}(operacion);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_bank_banco ON {TABLE_NAME}(banco_codigo);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_bank_fecha ON {TABLE_NAME}(fecha);")

        conn.commit()
        conn.close()

        ok("[BankWriter] Tabla lista ✔")

    # ============================================================
    #               VALIDACIÓN FLEXIBLE
    # ============================================================
    def _validate_mov(self, m: dict) -> bool:
        """
        Valida campos esenciales sin exigir source_hash,
        porque ahora se genera automáticamente.
        """
        required = ["monto", "banco_codigo"]

        for key in required:
            if key not in m or m[key] in ("", None):
                warn(f"[BankWriter] Movimiento omitido → falta campo '{key}'")
                return False

        return True

    # ============================================================
    #                GUARDAR LISTA DE MOVIMIENTOS
    # ============================================================
    def save_many(self, movimientos: list[dict]):
        if not movimientos:
            warn("[BankWriter] No hay movimientos para guardar.")
            return

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info(f"[BankWriter] Guardando {len(movimientos)} movimientos…")

        try:
            sql = f"""
                INSERT OR REPLACE INTO {TABLE_NAME} (
                    source_hash, fecha, tipo_mov, descripcion, operacion,
                    destinatario, tipo_documento, monto, moneda, banco_codigo
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """

            validos = 0

            for m in movimientos:

                if not self._validate_mov(m):
                    continue

                # 🚀 Generar hash si no viene del extractor
                if not m.get("source_hash"):
                    m["source_hash"] = self._make_hash(m)

                cur.execute(sql, (
                    m["source_hash"],
                    m.get("fecha"),
                    m.get("tipo_mov"),
                    m.get("descripcion"),
                    m.get("operacion"),
                    m.get("destinatario"),
                    m.get("tipo_documento"),
                    m.get("monto", 0.0),
                    m.get("moneda"),
                    m.get("banco_codigo"),
                ))

                validos += 1

            conn.commit()
            ok(f"[BankWriter] ✔ Movimientos insertados: {validos}")

        except Exception as e:
            conn.rollback()
            error(f"[BankWriter] ❌ Error guardando movimientos → {e}")

        finally:
            conn.close()
