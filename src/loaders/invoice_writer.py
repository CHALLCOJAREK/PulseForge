# src/loaders/invoice_writer.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · INVOICE WRITER
#  Escribe facturas procesadas en pulseforge.sqlite (facturas_pf)
# ============================================================
import sys
import sqlite3
import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd

# ------------------------------------------------------------
#  BOOTSTRAP RUTAS
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.extractors.invoices_extractor import InvoicesExtractor


# ============================================================
#  HELPERS
# ============================================================
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
        warn(f"La BD destino aún no existe: {db_file}. "
             f"¿Ejecutaste newdb_builder.py?")
    info(f"Conectando a BD PulseForge → {db_file}")
    return sqlite3.connect(db_file)


def _compute_source_hash(row: pd.Series) -> str:
    """
    Genera un hash estable por factura para trazabilidad.
    Usa RUC + COMBINADA + FECHA_EMISION como clave lógica.
    """
    base = f"{row.get('ruc','')}|{row.get('combinada','')}|{row.get('fecha_emision','')}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
#  INVOICE WRITER
# ============================================================
class InvoiceWriter:
    """
    Escribe las facturas procesadas (DataFrame de InvoicesExtractor /
    DataMapper.map_facturas) en la tabla facturas_pf de pulseforge.sqlite.
    """

    # Columnas esperadas desde el pipeline (post-DataMapper)
    EXPECTED_COLS = [
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

    NUMERIC_COLS = [
        "subtotal",
        "neto_recibido",
        "total_con_igv",
        "detraccion_monto",
    ]

    DATE_COLS = [
        "fecha_emision",
        "fecha_limite_pago",
        "fecha_inicio_ventana",
        "fecha_fin_ventana",
    ]

    def __init__(self) -> None:
        info("Inicializando InvoiceWriter…")
        self.db_path = _get_newdb_path()
        ok(f"InvoiceWriter listo. BD destino: {self.db_path}")

    # --------------------------------------------------------
    def _ensure_table_exists(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='facturas_pf'
        """)
        row = cur.fetchone()
        if not row:
            error("La tabla 'facturas_pf' no existe en PulseForge. "
                  "Ejecuta primero src/loaders/newdb_builder.py")
            raise RuntimeError("Tabla facturas_pf no encontrada.")

    # --------------------------------------------------------
    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura que el DataFrame tenga todas las columnas estándar,
        tipos numéricos y fechas como texto ISO.
        """
        if df is None or df.empty:
            warn("DataFrame de facturas vacío. No se insertará nada.")
            return pd.DataFrame(columns=self.EXPECTED_COLS + ["estado_pago", "source_hash"])

        df_norm = df.copy()

        # Asegurar columnas esperadas
        for col in self.EXPECTED_COLS:
            if col not in df_norm.columns:
                warn(f"[INVOICE_WRITER] Columna '{col}' faltante en DF. Se crea vacía.")
                df_norm[col] = None

        # Cast numéricas
        for col in self.NUMERIC_COLS:
            try:
                df_norm[col] = pd.to_numeric(df_norm[col], errors="coerce")
            except Exception:
                warn(f"No se pudo convertir '{col}' a numérico. Se mantiene como está.")

        # Cast fechas → string ISO (YYYY-MM-DD) o texto simple
        for col in self.DATE_COLS:
            if col not in df_norm.columns:
                continue

            if pd.api.types.is_datetime64_any_dtype(df_norm[col]):
                df_norm[col] = df_norm[col].dt.strftime("%Y-%m-%d")
            else:
                # Convertir a string pero respetando None / NaN
                df_norm[col] = df_norm[col].where(df_norm[col].notna(), None)
                df_norm[col] = df_norm[col].astype(str)

        # Estado de pago por defecto: pendiente (aún sin match)
        if "estado_pago" not in df_norm.columns:
            df_norm["estado_pago"] = "pendiente"
        else:
            df_norm["estado_pago"] = df_norm["estado_pago"].fillna("pendiente")

        # Hash de origen para trazabilidad
        df_norm["source_hash"] = df_norm.apply(_compute_source_hash, axis=1)

        # Devolver solo columnas en el orden esperado por facturas_pf
        final_cols = [
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
            "estado_pago",
            "source_hash",
        ]

        return df_norm[final_cols]

    # --------------------------------------------------------
    def save_facturas(self, df_facturas: pd.DataFrame, reset: bool = False) -> int:
        """
        Inserta facturas en facturas_pf.

        - df_facturas: DataFrame ya procesado por DataMapper.map_facturas()
        - reset=True → hace TRUNCATE lógico (DELETE) antes de insertar.
        """
        df_norm = self._normalize_df(df_facturas)
        if df_norm.empty:
            warn("No hay facturas normalizadas para insertar.")
            return 0

        conn = _get_connection()
        try:
            self._ensure_table_exists(conn)

            cur = conn.cursor()

            if reset:
                warn("Reset=True → limpiando tabla facturas_pf antes de insertar.")
                cur.execute("DELETE FROM facturas_pf")

            info("Insertando facturas en facturas_pf…")

            rows = df_norm.to_dict(orient="records")

            cur.executemany(
                """
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
                """,
                rows,
            )

            conn.commit()
            inserted = len(rows)
            ok(f"Facturas insertadas en facturas_pf: {inserted}")
            return inserted

        except Exception as e:
            error(f"Error insertando facturas en facturas_pf: {e}")
            raise
        finally:
            conn.close()


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("Iniciando test local de InvoiceWriter…")

        # 1) Extraer facturas desde DataPulse (SQLite origen)
        extractor = InvoicesExtractor()
        df_fact = extractor.get_facturas_mapeadas()
        ok(f"Facturas extraídas y mapeadas: {len(df_fact)}")

        # 2) Escribirlas en pulseforge.sqlite
        writer = InvoiceWriter()
        inserted = writer.save_facturas(df_facturas=df_fact, reset=True)

        ok(f"Test de InvoiceWriter completado. Filas insertadas: {inserted}")

    except Exception as e:
        error(f"Fallo en test de InvoiceWriter: {e}")
