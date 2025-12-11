# src/cli/pulseforge.py
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

# ---------------- FASE 1: EXTRACTORES -----------------
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.bank_extractor import BankExtractor
from src.extractors.clients_extractor import ClientsExtractor

from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.bank_writer import BankWriter
from src.loaders.clients_writer import ClientsWriter

# ---------------- FASE 2: PIPELINES -------------------
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes
from src.pipelines.pipeline_matcher import PipelineMatcher

from src.pipelines.incremental import IncrementalRunner


# ======================================================
#                FULL · ETL COMPLETO
# ======================================================
def cmd_full(args):
    info("=== FULL RUN · PULSEFORGE ===")

    # 1) Configuración
    cfg = get_config()
    ok(f"DB origen:  {cfg.db_source}")
    ok(f"DB destino: {cfg.db_destino}")

    # 2) Crear BD destino / reset
    warn("Reiniciando BD destino…")
    NewDBBuilder()

    # ==================================================
    #               FASE 1 — EXTRACCIÓN
    # ==================================================
    info("=== FASE 1 · EXTRACCIÓN Y CARGA ===")

    # -------- Facturas --------
    info("Extrayendo facturas desde BD origen…")
    fact_ex = InvoicesExtractor()
    facturas = fact_ex.run()  # → list[dict]

    info("Guardando facturas RAW en PulseForge…")
    fact_writer = InvoiceWriter()
    fact_writer.save_many(facturas)

    # -------- Bancos --------
    info("Extrayendo movimientos bancarios…")
    bank_ex = BankExtractor()
    movimientos = bank_ex.run()  # list[dict]

    info("Guardando movimientos RAW en PulseForge…")
    bank_writer = BankWriter()
    bank_writer.save_many(movimientos)

    # -------- Clientes --------
    info("Extrayendo clientes…")
    cli_ex = ClientsExtractor()
    clientes = cli_ex.run()  # list[dict]

    info("Guardando clientes RAW en PulseForge…")
    cli_writer = ClientsWriter()
    cli_writer.save_many(clientes)

    ok("FASE 1 completada ✔ (BD destino llena con datos RAW)")

    # ==================================================
    #           FASE 2 — PIPELINES / TRANSFORMERS
    # ==================================================
    info("=== FASE 2 · PROCESAMIENTO ===")

    pf = PipelineFacturas()
    df_f = pf.process()
    ok(f"Facturas procesadas: {len(df_f)}")

    pb = PipelineBancos()
    df_b = pb.process()
    ok(f"Movimientos procesados: {len(df_b)}")

    pc = PipelineClientes()
    df_c = pc.process()
    ok(f"Clientes procesados: {len(df_c)}")

    # ==================================================
    #              FASE 3 — MATCH
    # ==================================================
    info("=== FASE 3 · MATCHING ===")

    pm = PipelineMatcher()
    pm.run()

    ok("FULL RUN completado con éxito 😎")


# ======================================================
#                INCREMENTAL
# ======================================================
def cmd_incremental(args):
    info("=== INCREMENTAL RUN ===")
    cfg = get_config()

    runner = IncrementalRunner()
    runner.run()

    ok("Incremental ejecutado.")


# ======================================================
#                SOLO MATCH
# ======================================================
def cmd_match(args):
    info("=== MATCH RUN ===")
    cfg = get_config()

    pm = PipelineMatcher()
    pm.run()

    ok("Matching actualizado correctamente.")


# ======================================================
#                  REBUILD
# ======================================================
def cmd_rebuild(args):
    info("=== REBUILD BD ===")
    cfg = get_config()

    NewDBBuilder()
    ok("BD reconstruida.")


# ======================================================
#                  STATUS
# ======================================================
def cmd_status(args):
    cfg = get_config()

    info("=== ESTADO DEL SISTEMA ===")
    ok(f"DB origen:    {cfg.db_source}")
    ok(f"DB destino:   {cfg.db_destino}")
    ok(f"Run Mode:     {cfg.run_mode}")
    ok("PulseForge operativo ✔")


# ======================================================
#                   CLI SETUP
# ======================================================
def build_cli():
    parser = argparse.ArgumentParser(
        prog="pulseforge",
        description="PulseForge CLI — ETL + Matching Engine"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("full", help="Ejecuta ETL completo (extract + load + pipelines + match)").set_defaults(func=cmd_full)
    sub.add_parser("incremental", help="Ejecuta incremental").set_defaults(func=cmd_incremental)
    sub.add_parser("match", help="Ejecuta solo matching").set_defaults(func=cmd_match)
    sub.add_parser("rebuild", help="Reconstruye BD destino").set_defaults(func=cmd_rebuild)
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
