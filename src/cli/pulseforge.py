from __future__ import annotations
import sys
import argparse
from pathlib import Path

# ======================================================
# Bootstrap rutas
# ======================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ======================================================
# Imports corporativos
# ======================================================
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config

from src.loaders.newdb_builder import NewDBBuilder

# --------- FASE 1: EXTRACTORES + LOADERS ---------
from src.extractors.invoices_extractor import FacturasExtractor
from src.extractors.bank_extractor import BancosExtractor
from src.extractors.clients_extractor import ClientesExtractor

from src.loaders.facturas_loader import FacturasLoader
from src.loaders.bancos_loader import BancosLoader
from src.loaders.clientes_loader import ClientesLoader

# --------- FASE 2: PIPELINES + MATCH ---------
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes
from src.pipelines.pipeline_matcher import PipelineMatcher

from src.pipelines.incremental import IncrementalRunner


# ======================================================
#   FULL PIPELINE (F1 + F2 COMPLETO)
# ======================================================
def cmd_full(args):
    info("=== FULL RUN · PULSEFORGE ===")
    
    # 1) Cargar entorno + settings
    cfg = get_config()
    ok(f"DB origen: {cfg.db_source}")
    ok(f"DB destino: {cfg.db_destino}")

    # 2) Reconstruir BD destino
    warn("Reiniciando BD destino…")
    NewDBBuilder()   # crea tablas RAW

    # ==================================================
    #   FASE 1 — EXTRACCIÓN + LOAD
    # ==================================================

    info("=== FASE 1 · EXTRACCIÓN Y CARGA ===")

    # --- FACTURAS ---
    info("Extrayendo facturas…")
    ef = FacturasExtractor()
    df_fact_raw = ef.extract()

    info("Cargando facturas en PulseForge…")
    lf = FacturasLoader()
    lf.load(df_fact_raw)

    # --- BANCOS ---
    info("Extrayendo bancos…")
    eb = BancosExtractor()
    df_bank_raw = eb.extract()

    info("Cargando bancos en PulseForge…")
    lb = BancosLoader()
    lb.load(df_bank_raw)

    # --- CLIENTES ---
    info("Extrayendo clientes…")
    ec = ClientesExtractor()
    df_client_raw = ec.extract()

    info("Cargando clientes en PulseForge…")
    lc = ClientesLoader()
    lc.load(df_client_raw)

    ok("FASE 1 completada ✔ (BD destino llena con RAW)")

    # ==================================================
    #   FASE 2 — PIPELINES (NORMALIZACIÓN + CÁLCULO)
    # ==================================================
    info("=== FASE 2 · PIPELINES ===")

    # FACTURAS
    pf = PipelineFacturas()
    df_facturas_proc = pf.process()

    # BANCOS
    pb = PipelineBancos()
    df_bancos_proc = pb.process()

    # CLIENTES
    pc = PipelineClientes()
    clientes_list = pc.process()

    ok("Fase 2 completada ✔")

    # ==================================================
    #   MATCHING
    # ==================================================
    info("=== FASE 3 · MATCHER ===")

    pm = PipelineMatcher()
    pm.run()

    ok("FULL RUN finalizado correctamente 😎")


# ======================================================
#   INCREMENTAL
# ======================================================
def cmd_incremental(args):
    info("=== INCREMENTAL RUN ===")
    cfg = get_config()

    runner = IncrementalRunner()
    runner.run()

    ok("Incremental terminado.")


# ======================================================
#   SOLO MATCH
# ======================================================
def cmd_match(args):
    info("=== MATCH RUN ===")

    cfg = get_config()
    
    pm = PipelineMatcher()
    pm.run()

    ok("Matching actualizado.")


# ======================================================
#   REBUILD
# ======================================================
def cmd_rebuild(args):
    info("=== REBUILD BD ===")
    cfg = get_config()
    
    NewDBBuilder()
    ok("BD reconstruida.")


# ======================================================
#   STATUS
# ======================================================
def cmd_status(args):
    cfg = get_config()
    info("=== ESTADO DEL SISTEMA ===")
    ok(f"DB origen:   {cfg.db_source}")
    ok(f"DB destino:  {cfg.db_destino}")
    ok(f"Run Mode:    {cfg.run_mode}")
    ok("PulseForge operativo ✔")


# ======================================================
#   REGISTRO DEL CLI
# ======================================================
def build_cli():
    parser = argparse.ArgumentParser(
        prog="pulseforge",
        description="PulseForge CLI — ETL + Matching Engine"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("full", help="Ejecuta el proceso completo").set_defaults(func=cmd_full)
    sub.add_parser("incremental", help="Incremental ETL").set_defaults(func=cmd_incremental)
    sub.add_parser("match", help="Ejecuta solo matching").set_defaults(func=cmd_match)
    sub.add_parser("rebuild", help="Reconstruye BD").set_defaults(func=cmd_rebuild)
    sub.add_parser("status", help="Estado del sistema").set_defaults(func=cmd_status)

    return parser


def main():
    parser = build_cli()
    args = parser.parse_args()

    try:
        args.func(args)
    except Exception as e:
        error(f"Error en ejecución: {e}")
        raise


if __name__ == "__main__":
    main()
