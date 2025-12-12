# src/loaders/match_writer.py
from __future__ import annotations

import sys
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

# ------------------------------------------------------------
#  Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


MATCH_TABLE = "match_pf"
FACT_TABLE = "facturas_pf"
BANK_TABLE = "bancos_pf"


# ============================================================
#  HELPERS DE CONEXIÓN
# ============================================================
def _get_db_path() -> Path:
    cfg = get_config()
    db_path = getattr(cfg, "db_pulseforge", None) or getattr(cfg, "db_destino", None)
    if not db_path:
        error("MatchWriter: ruta de BD PulseForge vacía en configuración.")
        raise ValueError("DB PulseForge no configurada.")
    return Path(db_path)


def _get_connection() -> sqlite3.Connection:
    db_path = _get_db_path()
    if not db_path.exists():
        error(f"MatchWriter: BD destino no existe → {db_path}")
        raise FileNotFoundError(db_path)
    info(f"[MatchWriter] Conectando SQLite → {db_path}")
    return sqlite3.connect(db_path)


# ============================================================
#  MATCH WRITER · VERSIÓN PRO
# ============================================================
class MatchWriter:

    def __init__(self):
        info("Inicializando MatchWriter…")
        self.cfg = get_config()
        self.db_path = _get_db_path()
        info(f"[MatchWriter] BD PulseForge configurada: {self.db_path}")
        self._fact_hash_map: Dict[Any, str] = {}
        self._bank_hash_map: Dict[Any, str] = {}

    # ------------------------------------------------------
    #  Creación de tabla (segura)
    # ------------------------------------------------------
    def _ensure_table(self, conn: sqlite3.Connection) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {MATCH_TABLE} (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_hash     TEXT,
            banco_hash       TEXT,
            cliente_hash     TEXT,
            tipo_monto_match TEXT,
            monto_factura    REAL,
            monto_banco      REAL,
            diferencia       REAL,
            porcentaje_match REAL,
            estado           TEXT,
            fecha_match      TEXT
        );
        """
        conn.execute(sql)
        conn.commit()
        ok(f"[MatchWriter] Tabla {MATCH_TABLE} verificada/creada.")

    # ------------------------------------------------------
    #  Cargar hash maps
    # ------------------------------------------------------
    def _load_hash_maps(self, conn: sqlite3.Connection) -> None:

        # Facturas
        try:
            df_fact = pd.read_sql_query(
                f"SELECT id, source_hash FROM {FACT_TABLE}", conn
            )
            self._fact_hash_map = (
                df_fact.dropna(subset=["id"])
                .set_index("id")["source_hash"]
                .dropna()
                .astype(str)
                .to_dict()
            )
            ok(f"[MatchWriter] Hashes facturas cargados ({len(self._fact_hash_map)})")
        except Exception as e:
            warn(f"[MatchWriter] No se pudieron cargar hashes de facturas: {e}")
            self._fact_hash_map = {}

        # Bancos
        try:
            df_bank = pd.read_sql_query(
                f"SELECT id, source_hash FROM {BANK_TABLE}", conn
            )
            self._bank_hash_map = (
                df_bank.dropna(subset=["id"])
                .set_index("id")["source_hash"]
                .dropna()
                .astype(str)
                .to_dict()
            )
            ok(f"[MatchWriter] Hashes bancos cargados ({len(self._bank_hash_map)})")
        except Exception as e:
            warn(f"[MatchWriter] No se pudieron cargar hashes de bancos: {e}")
            self._bank_hash_map = {}

    # ------------------------------------------------------
    @staticmethod
    def _safe_float(v) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    # =======================================================
    #  🔥 API PRINCIPAL — save_matches(df)
    # =======================================================
    def save_matches(self, df_match: pd.DataFrame) -> None:

        if df_match is None or df_match.empty:
            warn("[MatchWriter] df_match vacío, no hay nada que guardar.")
            return

        info("Guardando matches reales en BD…")
        info(f"[MatchWriter] Guardando {len(df_match)} matches…")

        conn = _get_connection()
        try:
            self._ensure_table(conn)
            self._load_hash_maps(conn)

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows_to_insert = []

            for _, row in df_match.iterrows():
                factura_id = row.get("factura_id") or row.get("id")
                mov_id = row.get("movimiento_id")

                # ----------------------------
                # HASH FACTURA
                # ----------------------------
                if pd.notna(row.get("factura_hash")):
                    factura_hash = str(row.get("factura_hash"))
                elif factura_id in self._fact_hash_map:
                    factura_hash = self._fact_hash_map[factura_id]
                else:
                    factura_hash = f"FAC:{factura_id}"

                # ----------------------------
                # HASH BANCO
                # ----------------------------
                if pd.notna(row.get("banco_hash")):
                    banco_hash = str(row.get("banco_hash"))
                elif mov_id in self._bank_hash_map:
                    banco_hash = self._bank_hash_map[mov_id]
                else:
                    banco_hash = f"BANK:{mov_id}"

                cliente_hash = row.get("cliente_hash")
                tipo_monto_match = row.get("tipo_monto_match")

                monto_factura = self._safe_float(
                    row.get("monto_factura")
                    or row.get("total_final")
                    or row.get("total_con_igv")
                )

                monto_banco = self._safe_float(
                    row.get("monto_banco_equivalente")
                    or row.get("monto_banco")
                )

                diferencia = self._safe_float(
                    row.get("variacion_monto") or (monto_factura - monto_banco)
                )

                porcentaje_match = self._safe_float(row.get("score_similitud"))
                estado = str(row.get("match_tipo") or "NO_MATCH").upper()

                rows_to_insert.append(
                    (
                        factura_hash,
                        banco_hash,
                        cliente_hash,
                        tipo_monto_match,
                        monto_factura,
                        monto_banco,
                        diferencia,
                        porcentaje_match,
                        estado,
                        now_str,
                    )
                )

            sql_insert = f"""
            INSERT INTO {MATCH_TABLE} (
                factura_hash,
                banco_hash,
                cliente_hash,
                tipo_monto_match,
                monto_factura,
                monto_banco,
                diferencia,
                porcentaje_match,
                estado,
                fecha_match
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            conn.executemany(sql_insert, rows_to_insert)
            conn.commit()

            ok(f"[MatchWriter] Insertados {len(rows_to_insert)} registros en {MATCH_TABLE}.")

        except Exception as e:
            conn.rollback()
            error(f"[MatchWriter] Error guardando matches: {e}")
            raise
        finally:
            conn.close()

    # =======================================================
    #  🔄 RETROCOMPATIBILIDAD — save_many(records)
    # =======================================================
    def save_many(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            warn("[MatchWriter] save_many() recibió lista vacía.")
            return

        try:
            df = pd.DataFrame.from_records(records)
        except Exception as e:
            error(f"[MatchWriter] Error convirtiendo records a DataFrame en save_many(): {e}")
            raise

        self.save_matches(df)
