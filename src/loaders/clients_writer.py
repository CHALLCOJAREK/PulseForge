# src/loaders/clients_writer.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · CLIENTS WRITER
#  Inserta clientes normalizados en clientes_pf
# ============================================================
import sys
import sqlite3
import hashlib
from pathlib import Path

import pandas as pd

# ------------------------------------------------------------
#  BOOTSTRAP
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.extractors.clients_extractor import ClientsExtractor


# ============================================================
#  HELPERS
# ============================================================

def _get_newdb_path() -> Path:
    db = get_env("PULSEFORGE_NEWDB_PATH")
    if not db:
        error("PULSEFORGE_NEWDB_PATH faltante en .env")
        raise ValueError("Falta configuración DB destino.")

    return Path(db)


def _get_connection() -> sqlite3.Connection:
    db_file = _get_newdb_path()

    if not db_file.exists():
        error(f"La base de datos destino no existe → {db_file}")
        raise FileNotFoundError(db_file)

    info(f"Conectando a BD PulseForge → {db_file}")
    return sqlite3.connect(db_file)


def _compute_hash(row: pd.Series) -> str:
    """
    Hash único: RUC + Razón Social.
    Garantiza unicidad y auditoría.
    """
    base = f"{row.get('RUC','')}|{row.get('Razon_Social','')}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
#  CLIENTS WRITER
# ============================================================

class ClientsWriter:

    EXPECTED_COLS = ["RUC", "Razon_Social"]

    def __init__(self) -> None:
        info("Inicializando ClientsWriter…")
        self.db_path = _get_newdb_path()
        ok(f"ClientsWriter listo. BD destino = {self.db_path}")

    # --------------------------------------------------------
    def _ensure_table(self, conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='clientes_pf'
        """)
        if not cur.fetchone():
            error("Tabla 'clientes_pf' NO existe. Ejecuta newdb_builder.py primero.")
            raise RuntimeError("clientes_pf no encontrada.")

    # --------------------------------------------------------
    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida columnas, limpia strings y genera hash.
        """
        if df is None or df.empty:
            warn("DF de clientes vacío. No se insertará nada.")
            return pd.DataFrame(columns=self.EXPECTED_COLS + ["source_hash"])

        df_norm = df.copy()

        # Asegurar columnas mínimas
        for c in self.EXPECTED_COLS:
            if c not in df_norm.columns:
                warn(f"[CLIENT_WRITER] Columna faltante '{c}', se crea vacía.")
                df_norm[c] = ""

        # Limpieza mínima
        df_norm["RUC"] = df_norm["RUC"].astype(str).str.strip()
        df_norm["Razon_Social"] = df_norm["Razon_Social"].astype(str).str.strip()

        # Hash único
        df_norm["source_hash"] = df_norm.apply(_compute_hash, axis=1)

        # Orden final
        return df_norm[["RUC", "Razon_Social", "source_hash"]]

    # --------------------------------------------------------
    def save_clientes(self, df: pd.DataFrame, reset=False) -> int:
        """
        Inserta clientes normalizados en clientes_pf.
        """
        df_norm = self._normalize(df)

        conn = _get_connection()
        try:
            self._ensure_table(conn)

            cur = conn.cursor()

            if reset:
                warn("Reset=True → limpiando tabla clientes_pf")
                cur.execute("DELETE FROM clientes_pf")

            info("Insertando clientes en clientes_pf…")

            rows = df_norm.to_dict(orient="records")

            cur.executemany("""
                INSERT INTO clientes_pf (
                    ruc,
                    razon_social,
                    source_hash
                )
                VALUES (
                    :RUC,
                    :Razon_Social,
                    :source_hash
                )
            """, rows)

            conn.commit()

            ok(f"Clientes insertados en clientes_pf: {len(rows)}")
            return len(rows)

        except Exception as e:
            error(f"Error insertando clientes: {e}")
            raise

        finally:
            conn.close()

    # --------------------------------------------------------
    # 🚀 NUEVO: alias estándar requerido por main.py
    # --------------------------------------------------------
    def save(self, df: pd.DataFrame, reset=False) -> int:
        """
        Alias universal para mantener compatibilidad con PulseForge:
        main.py llama → ClientsWriter().save(df)
        """
        return self.save_clientes(df, reset=reset)


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de ClientsWriter…")

        ce = ClientsExtractor()
        df = ce.get_clientes_mapeados()
        ok(f"Clientes extraídos normalizados: {len(df)}")

        writer = ClientsWriter()
        inserted = writer.save_clientes(df, reset=True)

        ok(f"Test de ClientsWriter completado. Filas: {inserted}")

    except Exception as e:
        error(f"Fallo en test de ClientsWriter: {e}")
