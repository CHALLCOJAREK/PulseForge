# src/loaders/match_writer.py
from __future__ import annotations
import sys
import sqlite3
import hashlib
from pathlib import Path
from typing import List
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


# ============================================================
# HELPERS
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


def _compute_hash(row: pd.Series) -> str:
    """
    Hash único por combinación factura-movimiento-monto_banco.
    Sirve para deduplicar matches.
    """
    base = (
        f"{row.get('factura_id','')}|"
        f"{row.get('movimiento_id','')}|"
        f"{row.get('monto_banco','')}"
    )
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ============================================================
# MATCH WRITER ENTERPRISE — ALINEADO A newdb_builder
# ============================================================

class MatchWriter:

    def __init__(self):
        info("Inicializando MatchWriter Enterprise…")
        self.db = _get_newdb_path()
        ok(f"BD destino → {self.db}")

    # ============================================================
    def _ensure_tables(self, conn: sqlite3.Connection):
        """
        Crea tablas si no existen. Esquema ALINEADO a newdb_builder.py
        para evitar incompatibilidades.
        """
        cur = conn.cursor()

        # matches_pf — resumen
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches_pf (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id      INTEGER,
                movimiento_id   INTEGER,
                monto_factura   REAL,
                monto_banco     REAL,
                diferencia      REAL,
                match_tipo      TEXT,
                score           REAL,
                razon_ia        TEXT,
                source_hash     TEXT UNIQUE,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # match_detalles_pf — trazabilidad
        cur.execute("""
            CREATE TABLE IF NOT EXISTS match_detalles_pf (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id          INTEGER,
                movimiento_id       INTEGER,
                serie               TEXT,
                numero              TEXT,
                combinada           TEXT,
                ruc                 TEXT,
                cliente             TEXT,
                fecha_mov           TEXT,
                banco_codigo        TEXT,
                descripcion_banco   TEXT,
                tipo_comparacion    TEXT,
                monto_ref           REAL,
                monto_banco         REAL,
                diff_monto          REAL,
                es_detraccion_bn    INTEGER,
                coincide_fecha      INTEGER,
                coincide_monto      INTEGER,
                coincide_nombre     INTEGER,
                dias_diff_fecha     INTEGER,
                score_final         REAL,
                resultado_final     TEXT,
                created_at          TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()

    # ============================================================
    # NORMALIZACIÓN DE MATCHES → matches_pf
    # ============================================================
    def _normalize_matches(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            warn("df_match vacío en MatchWriter._normalize_matches()")
            return pd.DataFrame(columns=[
                "factura_id", "movimiento_id",
                "monto_factura", "monto_banco",
                "diferencia", "match_tipo",
                "score", "razon_ia", "source_hash"
            ])

        df = df.copy()

        # Asegurar columnas mínimas para cálculo
        needed = [
            "factura_id", "movimiento_id",
            "subtotal", "igv", "total_con_igv", "detraccion_monto",
            "neto_recibido",
            "monto_banco", "monto_banco_equivalente",
            "variacion_monto",
            "match_tipo", "score_similitud", "razon_ia",
        ]
        for col in needed:
            if col not in df.columns:
                df[col] = None

        # Numéricos
        num_cols = [
            "subtotal", "igv", "total_con_igv", "detraccion_monto",
            "neto_recibido", "monto_banco", "monto_banco_equivalente",
            "variacion_monto", "score_similitud"
        ]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        # -----------------------------------------
        # Monto factura (prioridad):
        #  1) neto_recibido
        #  2) total_con_igv
        #  3) subtotal
        # -----------------------------------------
        df["monto_factura"] = df["neto_recibido"]
        mask = df["monto_factura"] == 0
        df.loc[mask, "monto_factura"] = df["total_con_igv"]
        mask = df["monto_factura"] == 0
        df.loc[mask, "monto_factura"] = df["subtotal"]

        # -----------------------------------------
        # Monto banco:
        #  1) monto_banco_equivalente (PEN)
        #  2) monto_banco
        # -----------------------------------------
        df["monto_banco_final"] = df["monto_banco_equivalente"]
        mask = df["monto_banco_final"] == 0
        df.loc[mask, "monto_banco_final"] = df["monto_banco"]
        df["monto_banco"] = df["monto_banco_final"]

        # -----------------------------------------
        # Diferencia
        # -----------------------------------------
        df["diferencia"] = df["variacion_monto"]
        mask_diff = df["diferencia"].isna() | (df["diferencia"] == 0)
        df.loc[mask_diff, "diferencia"] = df["monto_banco"] - df["monto_factura"]

        # -----------------------------------------
        # Campos finales
        # -----------------------------------------
        df["match_tipo"] = df["match_tipo"].fillna("")
        df["score"] = df["score_similitud"].fillna(0.0)
        df["razon_ia"] = df["razon_ia"].fillna("")

        # Hash único
        df["source_hash"] = df.apply(_compute_hash, axis=1)

        cols_out = [
            "factura_id",
            "movimiento_id",
            "monto_factura",
            "monto_banco",
            "diferencia",
            "match_tipo",
            "score",
            "razon_ia",
            "source_hash",
        ]
        df_out = df[cols_out].copy()

        ok(f"Matches normalizados → {len(df_out)} registros.")
        return df_out

    # ============================================================
    # NORMALIZACIÓN DE DETALLES → match_detalles_pf
    # ============================================================
    def _normalize_detalles(self, df_det: pd.DataFrame) -> pd.DataFrame:
        if df_det is None or df_det.empty:
            warn("df_detalles vacío en MatchWriter._normalize_detalles()")
            return pd.DataFrame(columns=[
                "factura_id", "movimiento_id", "serie", "numero",
                "combinada", "ruc", "cliente", "fecha_mov",
                "banco_codigo", "descripcion_banco", "tipo_comparacion",
                "monto_ref", "monto_banco", "diff_monto",
                "es_detraccion_bn", "coincide_fecha", "coincide_monto",
                "coincide_nombre", "dias_diff_fecha",
                "score_final", "resultado_final"
            ])

        df = df_det.copy()

        # Renombrar posibles nombres antiguos a los nuevos
        rename_map = {
            "banco": "banco_codigo",
            "banco_pago": "banco_codigo",
            "tipo_monto_ref": "tipo_comparacion",
            "monto_banco_equivalente": "monto_banco",
            "match_por_monto": "coincide_monto",
            "sim_nombre_regla": "coincide_nombre",
            "dias_diferencia_fecha": "dias_diff_fecha",
        }
        df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns},
            inplace=True,
        )

        needed = [
            "factura_id", "movimiento_id",
            "serie", "numero", "combinada",
            "ruc", "cliente",
            "fecha_mov", "banco_codigo",
            "descripcion_banco",
            "tipo_comparacion",
            "monto_ref", "monto_banco", "diff_monto",
            "es_detraccion_bn",
            "coincide_fecha", "coincide_monto", "coincide_nombre",
            "dias_diff_fecha",
            "score_final", "resultado_final",
        ]

        for col in needed:
            if col not in df.columns:
                df[col] = None

        # Numéricos
        num_cols = [
            "monto_ref", "monto_banco", "diff_monto",
            "es_detraccion_bn",
            "coincide_fecha", "coincide_monto", "coincide_nombre",
            "dias_diff_fecha",
            "score_final",
        ]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Flags 0/1
        for c in ["es_detraccion_bn", "coincide_fecha", "coincide_monto", "coincide_nombre"]:
            df[c] = df[c].fillna(0).astype(int)

        df_out = df[needed].copy()
        ok(f"Detalles normalizados → {len(df_out)} registros.")
        return df_out

    # ============================================================
    # API PÚBLICA
    # ============================================================
    def save(self, df_match: pd.DataFrame, df_detalles: pd.DataFrame, reset: bool = False) -> int:
        """
        Inserta resultados en:
          - matches_pf         (resumen)
          - match_detalles_pf  (trazabilidad)
        Retorna cantidad de matches guardados.
        """
        conn = _get_connection()
        self._ensure_tables(conn)
        cur = conn.cursor()

        if reset:
            warn("Reset=True → limpiando tablas matches_pf y match_detalles_pf")
            cur.execute("DELETE FROM matches_pf")
            cur.execute("DELETE FROM match_detalles_pf")

        # -------------------------
        # 1. Matches (resumen)
        # -------------------------
        df_norm = self._normalize_matches(df_match)
        if not df_norm.empty:
            cur.executemany("""
                INSERT OR REPLACE INTO matches_pf (
                    factura_id,
                    movimiento_id,
                    monto_factura,
                    monto_banco,
                    diferencia,
                    match_tipo,
                    score,
                    razon_ia,
                    source_hash
                )
                VALUES (
                    :factura_id,
                    :movimiento_id,
                    :monto_factura,
                    :monto_banco,
                    :diferencia,
                    :match_tipo,
                    :score,
                    :razon_ia,
                    :source_hash
                )
            """, df_norm.to_dict(orient="records"))

            ok(f"Matches insertados: {len(df_norm)}")

        # -------------------------
        # 2. Detalles
        # -------------------------
        if df_detalles is not None and not df_detalles.empty:
            df_det_norm = self._normalize_detalles(df_detalles)

            cur.executemany("""
                INSERT INTO match_detalles_pf (
                    factura_id,
                    movimiento_id,
                    serie,
                    numero,
                    combinada,
                    ruc,
                    cliente,
                    fecha_mov,
                    banco_codigo,
                    descripcion_banco,
                    tipo_comparacion,
                    monto_ref,
                    monto_banco,
                    diff_monto,
                    es_detraccion_bn,
                    coincide_fecha,
                    coincide_monto,
                    coincide_nombre,
                    dias_diff_fecha,
                    score_final,
                    resultado_final
                )
                VALUES (
                    :factura_id,
                    :movimiento_id,
                    :serie,
                    :numero,
                    :combinada,
                    :ruc,
                    :cliente,
                    :fecha_mov,
                    :banco_codigo,
                    :descripcion_banco,
                    :tipo_comparacion,
                    :monto_ref,
                    :monto_banco,
                    :diff_monto,
                    :es_detraccion_bn,
                    :coincide_fecha,
                    :coincide_monto,
                    :coincide_nombre,
                    :dias_diff_fecha,
                    :score_final,
                    :resultado_final
                )
            """, df_det_norm.to_dict(orient="records"))

            ok(f"Detalles insertados: {len(df_det_norm)}")

        conn.commit()
        conn.close()

        return len(df_norm)
