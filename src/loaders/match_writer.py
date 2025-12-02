# src/loaders/match_writer.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · MATCH WRITER
#  Escribe resultados del Matcher en matches_pf
#  Actualiza estados en facturas_pf
# ============================================================
import sys
import sqlite3
import hashlib
from pathlib import Path

import pandas as pd

# ------------------------------------------------------------
#  RUTAS
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


# ============================================================
#  HELPERS
# ============================================================

def _get_newdb_path() -> Path:
    db = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()
    if not db:
        error("PULSEFORGE_NEWDB_PATH vacío en .env")
        raise ValueError("PULSEFORGE_NEWDB_PATH requerido.")
    return Path(db)


def _get_connection() -> sqlite3.Connection:
    db = _get_newdb_path()
    if not db.exists():
        error(f"La BD destino no existe → {db}")
        raise FileNotFoundError(db)
    return sqlite3.connect(db)


def _compute_match_hash(row: pd.Series) -> str:
    base = (
        f"{row.get('factura_id', '')}|"
        f"{row.get('movimiento_id', '')}|"
        f"{row.get('monto_aplicado', '')}|"
        f"{row.get('fecha_pago', '')}|"
        f"{row.get('banco', '')}"
    )
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()



# ============================================================
#  MATCH WRITER
# ============================================================

class MatchWriter:

    EXPECTED_COLS = [
        "factura_id",
        "movimiento_id",
        "monto_aplicado",
        "monto_detraccion",
        "variacion_monto",
        "fecha_pago",
        "banco",
        "score_similitud",
        "razon_ia",
        "match_tipo",
    ]

    def __init__(self):
        info("Inicializando MatchWriter…")
        self.db = _get_newdb_path()
        ok(f"MatchWriter listo. BD destino → {self.db}")

    # --------------------------------------------------------
    def _ensure_tables(self, conn: sqlite3.Connection):
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE name='matches_pf'")
        if not cur.fetchone():
            error("La tabla 'matches_pf' NO existe. Ejecuta newdb_builder.py")
            raise RuntimeError("matches_pf no existe")

        cur.execute("SELECT name FROM sqlite_master WHERE name='facturas_pf'")
        if not cur.fetchone():
            error("La tabla 'facturas_pf' NO existe. Ejecuta newdb_builder.py")
            raise RuntimeError("facturas_pf no existe")

    # --------------------------------------------------------
    #  🔥 NUEVA NORMALIZACIÓN ANTI-None (FIX CRÍTICO)
    # --------------------------------------------------------
    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Asegura columnas, limpia None y tipos antes de insertar."""
        df_norm = df.copy()

        # Asegurar columnas faltantes
        for col in self.EXPECTED_COLS:
            if col not in df_norm.columns:
                warn(f"[MATCH_WRITER] Columna faltante '{col}', se crea vacía.")
                df_norm[col] = None

        # Convertir NUMÉRICOS → nunca dejar None
        numeric_cols = [
            "monto_aplicado",
            "monto_detraccion",
            "variacion_monto",
            "score_similitud",
        ]
        for col in numeric_cols:
            df_norm[col] = pd.to_numeric(df_norm[col], errors="coerce").fillna(0.0)

        # Convertir FECHAS → string seguro o vacío
        df_norm["fecha_pago"] = (
            pd.to_datetime(df_norm["fecha_pago"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .fillna("")
        )

        # Convertir STRING columns → sin None
        text_cols = ["razon_ia", "match_tipo", "banco"]
        for col in text_cols:
            df_norm[col] = (
                df_norm[col]
                .astype(str)
                .replace("None", "")
                .replace("nan", "")
                .fillna("")
            )

        # Hash único por match
        df_norm["source_hash"] = df_norm.apply(_compute_match_hash, axis=1)

        return df_norm

    # --------------------------------------------------------
    def save_matches(self, df: pd.DataFrame, reset=False) -> int:
        df_norm = self._normalize(df)

        conn = _get_connection()
        self._ensure_tables(conn)
        cur = conn.cursor()

        if reset:
            warn("Reset=True → limpiando tabla matches_pf")
            cur.execute("DELETE FROM matches_pf")

        info("Insertando registros de match en matches_pf…")

        rows = df_norm.to_dict(orient="records")

        cur.executemany("""
            INSERT INTO matches_pf (
                factura_id,
                movimiento_id,
                monto_aplicado,
                monto_detraccion,
                variacion,
                fecha_pago,
                banco,
                score,
                razon_ia,
                match_tipo,
                source_hash
            )
            VALUES (
                :factura_id,
                :movimiento_id,
                :monto_aplicado,
                :monto_detraccion,
                :variacion_monto,
                :fecha_pago,
                :banco,
                :score_similitud,
                :razon_ia,
                :match_tipo,
                :source_hash
            )
        """, rows)

        conn.commit()
        ok(f"Matches insertados: {len(rows)}")

        # Después de insertar → actualizar facturas_pf
        self._update_facturas_status(conn)

        conn.commit()
        conn.close()
        return len(rows)

    # --------------------------------------------------------
    def _update_facturas_status(self, conn: sqlite3.Connection):
        """Actualiza facturas_pf según montos cobrados."""

        info("Actualizando estados en facturas_pf…")
        cur = conn.cursor()

        # Obtener totales de matches por factura
        cur.execute("""
            SELECT factura_id, SUM(monto_aplicado) AS cobrado
            FROM matches_pf
            GROUP BY factura_id
        """)

        for factura_id, cobrado_total in cur.fetchall():

            # Monto original
            cur.execute("""
                SELECT subtotal FROM facturas_pf
                WHERE id = ?
            """, (factura_id,))
            monto_factura = cur.fetchone()

            if not monto_factura:
                warn(f"Factura {factura_id} no encontrada en facturas_pf")
                continue

            monto_factura = float(monto_factura[0])
            cobrado_total = float(cobrado_total or 0)

            if cobrado_total >= monto_factura - 0.01:
                estado = "COBRADO"
            elif 0 < cobrado_total < monto_factura:
                estado = "PARCIAL"
            else:
                estado = "PENDIENTE"

            cur.execute("""
                UPDATE facturas_pf
                SET estado_pago = ?,
                    monto_cobrado = ?
                WHERE id = ?
            """, (estado, cobrado_total, factura_id))

        ok("Estados de facturas actualizados correctamente.")


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de MatchWriter…")

        # Aquí simulamos matches (en tu pipeline vendrán del matcher real)
        df_test = pd.DataFrame([
            {
                "factura_id": 1,
                "movimiento_id": 10,
                "monto_aplicado": 500,
                "monto_detraccion": 0,
                "variacion_monto": 0.02,
                "fecha_pago": "2025-01-20",
                "banco": "BBVA",
                "score_similitud": 0.92,
                "razon_ia": "Coincidencia semántica de cliente",
                "match_tipo": "semantico"
            }
        ])

        writer = MatchWriter()
        writer.save_matches(df_test, reset=True)

        ok("Test de MatchWriter completado correctamente.")

    except Exception as e:
        error(f"Fallo en test de MatchWriter: {e}")
