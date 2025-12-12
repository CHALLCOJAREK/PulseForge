# src/pipelines/pipeline_matcher.py
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports corporativos
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import PulseForgeDB

from src.matchers.matcher_engine import MatcherEngine
from src.loaders.match_writer import MatchWriter


# ============================================================
#        PIPELINE MATCHER · EJECUCIÓN REAL EMPRESARIAL
# ============================================================
class PipelineMatcher:

    def __init__(self):
        info("Inicializando PipelineMatcher…")

        self.cfg = get_config()
        self.db = PulseForgeDB()
        self.writer = MatchWriter()
        self.engine = MatcherEngine()

        ok(f"PipelineMatcher listo. BD → {self.cfg.db_destino}")

    # --------------------------------------------------------
    # CARGA DE DATOS DESDE BD
    # --------------------------------------------------------
    def _load_data(self):
        try:
            conn = self.db.connect()

            df_fact = pd.read_sql_query(
                """
                SELECT *
                FROM facturas_pf
                WHERE IFNULL(fue_cobrado, 0) = 0
                """,
                conn
            )

            df_bank = pd.read_sql_query(
                "SELECT * FROM bancos_pf",
                conn
            )

            ok(f"Facturas pendientes de cobro: {len(df_fact)}")
            ok(f"Movimientos cargados: {len(df_bank)}")

            return df_fact, df_bank

        except Exception as e:
            error(f"Error cargando data desde BD: {e}")
            return pd.DataFrame(), pd.DataFrame()

    # --------------------------------------------------------
    # AUDITORÍA
    # --------------------------------------------------------
    def _audit(self, evento: str, detalle: str):
        try:
            conn = self.db.connect()
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                "INSERT INTO auditoria_pf (evento, detalle, fecha) VALUES (?, ?, ?)",
                (evento, detalle, fecha)
            )
            conn.commit()
        except Exception as e:
            warn(f"No se pudo registrar auditoría: {e}")

    # --------------------------------------------------------
    # PROCESO PRINCIPAL
    # --------------------------------------------------------
    def run(self) -> dict:
        info("=== PIPELINE MATCHER · EJECUCIÓN COMPLETA ===")

        df_fact, df_bank = self._load_data()

        if df_fact.empty or df_bank.empty:
            warn("No hay data suficiente para ejecutar matching.")
            return {}

        info("Ejecutando motor MatcherEngine…")
        df_match, df_detalles = self.engine.run(df_fact, df_bank)

        ok(f"Matches generados: {len(df_match)}")
        ok(f"Detalles generados: {len(df_detalles)}")

        # ----------------------------------------------------
        # GUARDAR MATCHES
        # ----------------------------------------------------
        if not df_match.empty:
            self.writer.save_many(df_match.to_dict("records"))
        else:
            warn("No se generaron matches válidos.")

        # ----------------------------------------------------
        # MARCAR FACTURAS COBRADAS (🔥 CAMBIO CLAVE 🔥)
        # ----------------------------------------------------
        try:
            conn = self.db.connect()

            df_ok = df_match[df_match["match_tipo"] == "MATCH"]

            for _, row in df_ok.iterrows():
                factura_id = row.get("factura_id")
                if not factura_id:
                    continue

                conn.execute(
                    """
                    UPDATE facturas_pf
                    SET fue_cobrado = 1,
                        match_id = ?
                    WHERE id = ?
                    """,
                    (row.get("factura_hash"), factura_id)
                )

            conn.commit()
            ok(f"Facturas marcadas como cobradas: {len(df_ok)}")

        except Exception as e:
            warn(f"No se pudieron actualizar facturas cobradas: {e}")

        # ----------------------------------------------------
        # GUARDAR DETALLES TEMPORALES
        # ----------------------------------------------------
        if not df_detalles.empty:
            try:
                df_detalles.to_sql(
                    "match_detalles_tmp",
                    self.db.connect(),
                    if_exists="replace",
                    index=False
                )
                ok("Detalles guardados en match_detalles_tmp.")
            except Exception as e:
                warn(f"No se pudieron guardar detalles: {e}")

        # Auditoría
        self._audit("MATCH_RUN", f"Matches generados: {len(df_match)}")

        ok("=== PIPELINE MATCHER COMPLETADO ===")

        return {
            "matches": df_match,
            "detalles": df_detalles
        }


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    info("=== TEST LOCAL · PIPELINE MATCHER ===")
    PipelineMatcher().run()
    ok("=== FIN TEST MATCHER ===")
