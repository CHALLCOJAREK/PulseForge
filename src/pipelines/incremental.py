# src/pipelines/incremental.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE INCREMENTAL (VERSIÓN CORPORATIVA)
#  Procesa SOLO nuevos registros en clientes, facturas, bancos
#  y ejecuta el matcher evitando duplicados vía source_hash.
# ============================================================

import sys
import hashlib
import sqlite3
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

from src.pipelines.pipeline_clients import PipelineClients
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_matcher import PipelineMatcher


# ============================================================
#  HELPERS — DB
# ============================================================
def _get_conn():
    db = Path(str(get_env("PULSEFORGE_NEWDB_PATH")).strip())
    if not db.exists():
        error(f"BD destino no existe: {db}")
        raise FileNotFoundError(db)
    return sqlite3.connect(db)


# ============================================================
#  HELPERS — AGREGAR HASH SEGÚN TIPO
# ============================================================
def _add_hash_if_missing(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    """
    Genera source_hash para clientes, facturas y bancos
    si no existe.
    """
    if df.empty:
        return df

    if "source_hash" in df.columns:
        return df  # nada que hacer

    # --------------------------
    # HASH CLIENTES
    # --------------------------
    if tipo == "clientes":
        def _h(row):
            base = f"{row.get('RUC','')}|{row.get('Razon_Social','')}"
            return hashlib.sha1(base.encode()).hexdigest()
        df["source_hash"] = df.apply(_h, axis=1)
        ok("source_hash generado para clientes.")
        return df

    # --------------------------
    # HASH FACTURAS
    # --------------------------
    if tipo == "facturas":
        def _h(row):
            base = (
                f"{row.get('ruc','')}|"
                f"{row.get('combinada','')}|"
                f"{row.get('fecha_emision','')}"
            )
            return hashlib.sha1(base.encode()).hexdigest()
        df["source_hash"] = df.apply(_h, axis=1)
        ok("source_hash generado para facturas.")
        return df

    # --------------------------
    # HASH BANCOS
    # --------------------------
    if tipo == "bancos":
        def _h(row):
            base = (
                f"{row.get('Banco','')}|"
                f"{row.get('Fecha','')}|"
                f"{row.get('Monto','')}|"
                f"{row.get('Operacion','')}|"
                f"{row.get('Descripcion','')}"
            )
            return hashlib.sha1(base.encode()).hexdigest()
        df["source_hash"] = df.apply(_h, axis=1)
        ok("source_hash generado para movimientos bancarios.")
        return df

    warn(f"No se definió hash para tipo='{tipo}', se crea columna vacía.")
    df["source_hash"] = ""
    return df


# ============================================================
#  HELPERS — FILTRAR REGISTROS NUEVOS POR HASH
# ============================================================
def _filter_new(df: pd.DataFrame, table: str, conn) -> pd.DataFrame:
    """
    Filtra solo registros cuyo source_hash NO existe en la tabla destino.
    """
    if df.empty:
        return df

    hashes = [str(h) for h in df["source_hash"].fillna("").tolist()]
    placeholders = ",".join("?" for _ in hashes)

    try:
        existing = pd.read_sql_query(
            f"SELECT source_hash FROM {table} WHERE source_hash IN ({placeholders})",
            conn,
            params=hashes
        )
    except Exception:
        warn(f"No se pudo leer source_hash de {table}. Se insertará TODO.")
        return df

    existing_set = set(existing["source_hash"].tolist())
    df_new = df[~df["source_hash"].isin(existing_set)]

    info(f"[INCREMENTAL] {table}: total={len(df)}, nuevos={len(df_new)}")
    return df_new


# ============================================================
#  PIPELINE INCREMENTAL
# ============================================================
class PipelineIncremental:

    def __init__(self):
        info("Inicializando PipelineIncremental…")

        # Nunca usar reset=True aquí
        self.p_clients = PipelineClients()
        self.p_fact = PipelineFacturas()
        self.p_bank = PipelineBancos()
        self.p_match = PipelineMatcher()

        ok("PipelineIncremental inicializado correctamente.")

    # --------------------------------------------------------
    def run(self):
        info("🚀 Ejecutando PipelineIncremental…")
        conn = _get_conn()

        # ====================================================
        # 1) CLIENTES
        # ====================================================
        info("📂 [1/4] Clientes – incremental")
        df_clientes = self.p_clients.extractor.get_clientes_mapeados()
        df_clientes = _add_hash_if_missing(df_clientes, "clientes")
        df_clientes_new = _filter_new(df_clientes, "clientes_pf", conn)

        if not df_clientes_new.empty:
            self.p_clients.writer.save_clientes(df_clientes_new)
        else:
            warn("No hay nuevos clientes para insertar.")

        # ====================================================
        # 2) FACTURAS
        # ====================================================
        info("📄 [2/4] Facturas – incremental")
        df_fact = self.p_fact.extractor.get_facturas_mapeadas()
        df_fact = _add_hash_if_missing(df_fact, "facturas")
        df_fact_new = _filter_new(df_fact, "facturas_pf", conn)

        if not df_fact_new.empty:
            self.p_fact.writer.save_facturas(df_fact_new)
        else:
            warn("No hay nuevas facturas para insertar.")

        # ====================================================
        # 3) BANCOS
        # ====================================================
        info("🏦 [3/4] Bancos – incremental")
        df_bancos = self.p_bank.extractor.get_bancos_mapeados()
        df_bancos = _add_hash_if_missing(df_bancos, "bancos")
        df_bancos_new = _filter_new(df_bancos, "movimientos_pf", conn)

        if not df_bancos_new.empty:
            self.p_bank.writer.save_movimientos(df_bancos_new)
        else:
            warn("No hay nuevos movimientos bancarios para insertar.")

        # ====================================================
        # 4) MATCHER
        # ====================================================
        info("🤖 [4/4] Matcher – incremental (evita duplicados por hash)")
        self.p_match.run()

        ok("✅ PipelineIncremental ejecutado correctamente.")


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        p = PipelineIncremental()
        p.run()
        ok("Test de PipelineIncremental OK.")
    except Exception as e:
        error(f"Fallo en PipelineIncremental: {e}")
