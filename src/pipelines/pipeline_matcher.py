# src/pipelines/pipeline_matcher.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE MATCHER (VERSIÓN CORPORATIVA)
#  Ejecuta el motor de matching y escribe resultados en matches_pf
# ============================================================

import sys
import sqlite3
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
#  BOOTSTRAP RUTAS
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env

# Motor y Writer oficiales
from src.matchers.matcher_engine import MatcherEngine
from src.loaders.match_writer import MatchWriter


# ------------------------------------------------------------
#  Conexión con pulseforge.sqlite
# ------------------------------------------------------------
def _get_conn_pf() -> sqlite3.Connection:
    db_path = str(get_env("PULSEFORGE_NEWDB_PATH")).strip()
    if not db_path:
        error("PULSEFORGE_NEWDB_PATH no configurado en .env")
        raise ValueError("Falta PULSEFORGE_NEWDB_PATH")

    file = Path(db_path)
    if not file.exists():
        error(f"La base pulseforge.sqlite no existe: {file}")
        raise RuntimeError("Ejecuta primero newdb_builder.py y los demás pipelines.")

    info(f"Conectando a BD PulseForge → {file}")
    return sqlite3.connect(file)


# ============================================================
#  PIPELINE MATCHER — MOTOR PRINCIPAL
# ============================================================
class PipelineMatcher:

    def __init__(self) -> None:
        info("Inicializando PipelineMatcher…")

        self.conn = _get_conn_pf()
        self.writer = MatchWriter()
        self.matcher = MatcherEngine()

        ok("PipelineMatcher listo.")

    # --------------------------------------------------------
    def _load_facturas_pf(self) -> pd.DataFrame:
        info("📄 Cargando facturas desde facturas_pf…")
        df = pd.read_sql_query("SELECT * FROM facturas_pf", self.conn)

        if df.empty:
            warn("No existen facturas en pulseforge.sqlite.")
        else:
            ok(f"Facturas cargadas: {len(df)}")

        return df

    # --------------------------------------------------------
    def _load_movimientos_pf(self) -> pd.DataFrame:
        info("🏦 Cargando movimientos desde movimientos_pf…")
        df = pd.read_sql_query("SELECT * FROM movimientos_pf", self.conn)

        if df.empty:
            warn("No existen movimientos bancarios en pulseforge.sqlite.")
        else:
            ok(f"Movimientos cargados: {len(df)}")

        return df

    # --------------------------------------------------------
    def run(self, reset: bool = False) -> int:
        """
        Ejecuta matching completo:
        - Lee facturas + movimientos
        - Llama al motor inteligente
        - Inserta matches + detalles
        """
        info("🚀 Ejecutando PipelineMatcher…")

        df_fact = self._load_facturas_pf()
        df_mov = self._load_movimientos_pf()

        if df_fact.empty or df_mov.empty:
            error("No se puede ejecutar el matcher: faltan datos.")
            return 0

        # Ejecutar motor de matching
        info("🤖 Ejecutando motor de Matching IA/Reglas…")

        try:
            df_matches, df_detalles = self.matcher.run(df_fact, df_mov)
        except Exception as e:
            error(f"Error ejecutando matcher: {e}")
            raise

        ok(f"Matches generados: {len(df_matches)}")

        # Guardar en BD
        info("💾 Guardando matches y detalles en matches_pf…")

        inserted = self.writer.save(
            df_match=df_matches,
            df_detalles=df_detalles,
            reset=reset
        )

        ok(f"Pipeline de Matching completado → {inserted} registros.")
        return inserted


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de PipelineMatcher…")
        pipeline = PipelineMatcher()
        inserted = pipeline.run(reset=True)
        ok(f"Test PipelineMatcher OK → {inserted} matches insertados.")
    except Exception as e:
        error(f"Fallo en test de PipelineMatcher: {e}")
