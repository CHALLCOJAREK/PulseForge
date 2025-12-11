# src/pipelines/incremental.py
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

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
from src.core.db import get_connection

from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes

from src.matchers.matcher_engine import MatcherEngine


# ============================================================
#  INCREMENTAL RUN — SOLO PROCESAR LO NUEVO EN BD
# ============================================================
class IncrementalRunner:

    def __init__(self):
        info("Inicializando IncrementalRunner PulseForge…")
        self.cfg = get_config()
        self.conn = get_connection()

    # --------------------------------------------------------
    # Leer hashes ya existentes en destino
    # --------------------------------------------------------
    def _load_existing_hashes(self) -> dict:
        info("Leyendo hashes existentes desde BD destino…")

        try:
            df_f = pd.read_sql_query("SELECT source_hash FROM facturas_pf", self.conn)
            df_b = pd.read_sql_query("SELECT source_hash FROM bancos_pf", self.conn)
            df_c = pd.read_sql_query("SELECT source_hash FROM clientes_pf", self.conn)

            ok("Hashes cargados correctamente.")
            return {
                "facturas": set(df_f["source_hash"].astype(str)),
                "bancos": set(df_b["source_hash"].astype(str)),
                "clientes": set(df_c["source_hash"].astype(str))
            }

        except Exception as e:
            error(f"Error cargando hashes de destino: {e}")
            return {"facturas": set(), "bancos": set(), "clientes": set()}

    # --------------------------------------------------------
    # Filtrar solo registros nuevos
    # --------------------------------------------------------
    @staticmethod
    def _filter_new(lista: list[dict], existing: set):
        return [item for item in lista if item.get("source_hash") not in existing]

    # --------------------------------------------------------
    # Guardar en BD destino
    # --------------------------------------------------------
    def _save_facturas(self, rows: list[dict]):
        if not rows:
            warn("No hay facturas nuevas para insertar.")
            return

        df = pd.DataFrame(rows)
        df.to_sql("facturas_pf", self.conn, if_exists="append", index=False)
        ok(f"Facturas nuevas insertadas: {len(df)}")

    def _save_bancos(self, rows: list[dict]):
        if not rows:
            warn("No hay movimientos nuevos para insertar.")
            return

        df = pd.DataFrame(rows)
        df.to_sql("bancos_pf", self.conn, if_exists="append", index=False)
        ok(f"Movimientos nuevos insertados: {len(df)}")

    def _save_clientes(self, rows: list[dict]):
        if not rows:
            warn("No hay clientes nuevos para insertar.")
            return

        df = pd.DataFrame(rows)
        df.to_sql("clientes_pf", self.conn, if_exists="append", index=False)
        ok(f"Clientes nuevos insertados: {len(df)}")

    # --------------------------------------------------------
    #  EJECUCIÓN PRINCIPAL INCREMENTAL
    # --------------------------------------------------------
    def run(self) -> dict:
        info("=== INCREMENTAL RUN · PULSEFORGE ===")

        hashes = self._load_existing_hashes()

        # -----------------------------
        #  FACTURAS
        # -----------------------------
        pf = PipelineFacturas()
        df_fact = pf.process()

        nuevas_fact = self._filter_new(df_fact.to_dict("records"), hashes["facturas"])
        ok(f"Facturas nuevas detectadas: {len(nuevas_fact)}")
        self._save_facturas(nuevas_fact)

        # -----------------------------
        #  BANCOS
        # -----------------------------
        pb = PipelineBancos()
        df_bank = pb.process()

        nuevas_bank = self._filter_new(df_bank.to_dict("records"), hashes["bancos"])
        ok(f"Movimientos nuevos detectados: {len(nuevas_bank)}")
        self._save_bancos(nuevas_bank)

        # -----------------------------
        #  CLIENTES
        # -----------------------------
        pc = PipelineClientes()
        lista_clientes = pc.process()

        nuevas_cli = self._filter_new(lista_clientes, hashes["clientes"])
        ok(f"Clientes nuevos detectados: {len(nuevas_cli)}")
        self._save_clientes(nuevas_cli)

        # -----------------------------
        # MATCHING SOLO SOBRE LO NUEVO
        # -----------------------------
        info("Ejecutando matching incremental…")

        df_match = pd.DataFrame()
        df_det = pd.DataFrame()

        if nuevas_fact or nuevas_bank:
            matcher = MatcherEngine()

            # matching incremental = match SOLO nuevos con TODO el banco
            df_match, df_det = matcher.run(
                pd.DataFrame(nuevas_fact),
                pd.concat([df_bank], ignore_index=True)
            )

            ok(f"Matches generados (incremental): {len(df_match)}")
        else:
            warn("No hay datos nuevos. Matching no ejecutado.")

        return {
            "facturas_nuevas": nuevas_fact,
            "bancos_nuevos": nuevas_bank,
            "clientes_nuevos": nuevas_cli,
            "matches": df_match,
            "detalles": df_det
        }


# ============================================================
# TEST CONTROLADO – SOLO SI SE EJECUTA DIRECTAMENTE
# ============================================================
if __name__ == "__main__":
    info("=== TEST E2E · INCREMENTAL RUN ===")

    runner = IncrementalRunner()
    result = runner.run()

    ok("=== INCREMENTAL COMPLETADO ===")
