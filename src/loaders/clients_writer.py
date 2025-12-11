# src/loaders/clients_writer.py
from __future__ import annotations

import sqlite3
from pathlib import Path
import sys
import hashlib

# Bootstrap dinámico
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


TABLE_NAME = "clientes_pf"


# ============================================================
#              CLIENT WRITER · PULSEFORGE 2025
# ============================================================
class ClientsWriter:

    def __init__(self):
        """Inicializa el writer y prepara la BD destino."""
        db_path = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()

        if not db_path:
            raise ValueError("❌ Falta PULSEFORGE_NEWDB_PATH en .env")

        self.db_path = Path(db_path)
        info(f"[ClientsWriter] BD destino → {self.db_path}")

        self._ensure_table()

    # ============================================================
    #               CREAR TABLA BASE
    # ============================================================
    def _ensure_table(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info("[ClientsWriter] Verificando tabla pf_clients…")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_hash TEXT UNIQUE,
                ruc TEXT,
                razon_social TEXT
            );
        """)

        # Índices críticos para búsquedas por RUC
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_cli_hash ON {TABLE_NAME}(source_hash);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_cli_ruc  ON {TABLE_NAME}(ruc);")

        conn.commit()
        conn.close()

        ok("[ClientsWriter] Tabla lista ✔")

    # ============================================================
    #               GENERADOR DE HASH
    # ============================================================
    def _make_hash(self, c: dict) -> str:
        """
        Genera un hash único basado en datos estables del cliente.
        """
        base = f"{c.get('ruc','')}|{c.get('razon_social','')}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    # ============================================================
    #               VALIDACIÓN FLEXIBLE
    # ============================================================
    def _validate_cliente(self, c: dict) -> bool:
        required = ["ruc", "razon_social"]

        for k in required:
            if k not in c or c[k] in ["", None]:
                warn(f"[ClientsWriter] Cliente omitido, falta campo: {k}")
                return False

        return True

    # ============================================================
    #               GUARDADO MASIVO
    # ============================================================
    def save_many(self, clientes: list[dict]):
        if not clientes:
            warn("[ClientsWriter] No hay clientes para guardar.")
            return

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info(f"[ClientsWriter] Guardando {len(clientes)} clientes…")

        try:
            sql = f"""
                INSERT OR REPLACE INTO {TABLE_NAME} (
                    source_hash, ruc, razon_social
                ) VALUES (?, ?, ?);
            """

            validos = 0

            for c in clientes:

                if not self._validate_cliente(c):
                    continue

                # 🚀 Generar hash si no existe
                if not c.get("source_hash"):
                    c["source_hash"] = self._make_hash(c)

                cur.execute(sql, (
                    c["source_hash"],
                    c["ruc"],
                    c["razon_social"],
                ))

                validos += 1

            conn.commit()
            ok(f"[ClientsWriter] ✔ Clientes guardados: {validos}")

        except Exception as e:
            conn.rollback()
            error(f"[ClientsWriter] ❌ Error guardando clientes → {e}")

        finally:
            conn.close()
