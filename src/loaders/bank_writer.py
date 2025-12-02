# src/loaders/bank_writer.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · BANK WRITER
#  Inserta movimientos bancarios en movimientos_pf
# ============================================================
import sys
import sqlite3
import hashlib
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


# ------------------------------------------------------------
def _get_newdb_path() -> Path:
    db_path = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()
    if not db_path:
        error("PULSEFORGE_NEWDB_PATH no configurado en .env")
        raise ValueError("Falta PULSEFORGE_NEWDB_PATH")

    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    return db_file


def _get_connection() -> sqlite3.Connection:
    db_file = _get_newdb_path()
    if not db_file.exists():
        warn(f"La BD destino aún no existe: {db_file}. Ejecuta newdb_builder.py.")
    info(f"Conectando a BD PulseForge → {db_file}")
    return sqlite3.connect(db_file)


def _compute_hash(row: pd.Series) -> str:
    base = f"{row.get('Banco','')}|{row.get('Operacion','')}|{row.get('Fecha','')}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
class BankWriter:
    """
    Inserta movimientos bancarios ya mapeados en movimientos_pf.
    """

    EXPECTED_COLS = [
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

    def __init__(self) -> None:
        info("Inicializando BankWriter…")
        self.db_path = _get_newdb_path()
        ok(f"BankWriter listo. BD destino: {self.db_path}")

    # --------------------------------------------------------
    def _ensure_table_exists(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='movimientos_pf'
            """
        )
        if not cur.fetchone():
            error("movimientos_pf no existe. Ejecuta newdb_builder.py primero.")
            raise RuntimeError("Tabla movimientos_pf no encontrada.")

    # --------------------------------------------------------
    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        - Asegura columnas esperadas.
        - Convierte tipos a algo que SQLite soporte (str, float, int).
        """
        if df is None or df.empty:
            warn("DF bancario vacío. Nada que insertar.")
            return pd.DataFrame(columns=self.EXPECTED_COLS + ["source_hash"])

        df2 = df.copy()

        # Asegurar columnas
        for col in self.EXPECTED_COLS:
            if col not in df2.columns:
                warn(f"[BANK_WRITER] Columna faltante '{col}'. Se crea vacía.")
                df2[col] = None

        # ---- Tipos seguros para SQLite ----

        # Fecha → string
        if "Fecha" in df2.columns:
            if pd.api.types.is_datetime64_any_dtype(df2["Fecha"]):
                df2["Fecha"] = df2["Fecha"].dt.strftime("%Y-%m-%d")
            else:
                df2["Fecha"] = df2["Fecha"].astype(str)

        # Monto → numérico
        if "Monto" in df2.columns:
            df2["Monto"] = pd.to_numeric(df2["Monto"], errors="coerce")

        # Campos de texto → str
        for col_text in [
            "Banco",
            "Descripcion",
            "Moneda",
            "Operacion",
            "Tipo_Mov",
            "Destinatario",
            "Tipo_Documento",
        ]:
            if col_text in df2.columns:
                df2[col_text] = df2[col_text].astype(str)

        # Generar hash
        df2["source_hash"] = df2.apply(_compute_hash, axis=1)

        return df2

    # --------------------------------------------------------
    def save_bancos(self, df_bancos: pd.DataFrame, reset: bool = False) -> int:
        """
        Inserta movimientos bancarios en movimientos_pf.
        """
        df_norm = self._normalize_df(df_bancos)
        if df_norm.empty:
            warn("DF bancario normalizado vacío. No se insertará nada.")
            return 0

        conn = _get_connection()
        try:
            self._ensure_table_exists(conn)

            cur = conn.cursor()

            if reset:
                warn("Reset=True → limpiando movimientos_pf")
                cur.execute("DELETE FROM movimientos_pf")

            info("Insertando movimientos bancarios en movimientos_pf…")

            rows = df_norm.to_dict(orient="records")

            cur.executemany(
                """
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
                ) VALUES (
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
                """,
                rows,
            )

            conn.commit()
            inserted = len(rows)
            ok(f"Movimientos insertados en movimientos_pf: {inserted}")
            return inserted

        except Exception as e:
            error(f"Error insertando movimientos bancarios: {e}")
            raise

        finally:
            conn.close()

    # --------------------------------------------------------
    # 🔥 ALIAS PARA COMPATIBILIDAD CON EL PIPELINE INCREMENTAL 🔥
    # --------------------------------------------------------
    def save_movimientos(self, df: pd.DataFrame, reset: bool = False) -> int:
        """
        Alias corporativo para compatibilidad.
        Internamente delega al método oficial save_bancos().
        """
        return self.save_bancos(df, reset=reset)


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        from src.extractors.bank_extractor import BankExtractor

        info("Test local de BankWriter…")
        be = BankExtractor()
        df = be.get_bancos_mapeados()

        bw = BankWriter()
        inserted = bw.save_bancos(df, reset=True)

        ok(f"Test BankWriter completo. Insertados: {inserted}")

    except Exception as e:
        error(f"Fallo en test de BankWriter: {e}")
