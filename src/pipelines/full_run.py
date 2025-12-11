# src/pipelines/full_run.py
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

from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes
from src.matchers.matcher_engine import MatcherEngine


# ============================================================
#  FULL RUN – EJECUCIÓN COMPLETA DEL SISTEMA PULSEFORGE
# ============================================================
def run_full_pipeline() -> dict:
    info("=== FULL RUN · PULSEFORGE 2025 ===")

    cfg = get_config()

    # -----------------------------
    # 1) FACTURAS
    # -----------------------------
    info("→ Ejecutando PipelineFacturas…")
    pf = PipelineFacturas()
    df_fact = pf.process()

    if df_fact is None or df_fact.empty:
        warn("PipelineFacturas devolvió cero filas.")
    else:
        ok(f"Facturas procesadas: {len(df_fact)}")

    # -----------------------------
    # 2) BANCOS
    # -----------------------------
    info("→ Ejecutando PipelineBancos…")
    pb = PipelineBancos()
    df_bank = pb.process()

    if df_bank is None or df_bank.empty:
        warn("PipelineBancos devolvió cero filas.")
    else:
        ok(f"Movimientos procesados: {len(df_bank)}")

    # -----------------------------
    # 3) CLIENTES
    # -----------------------------
    info("→ Ejecutando PipelineClientes…")
    pc = PipelineClientes()
    clientes = pc.process()

    if not clientes:
        warn("PipelineClientes devolvió cero registros.")
    else:
        ok(f"Clientes procesados: {len(clientes)}")

    # -----------------------------
    # 4) MATCHING COMPLETO
    # -----------------------------
    info("→ Ejecutando MatcherEngine (facturas vs bancos)…")
    matcher = MatcherEngine()

    df_match, df_det = matcher.run(df_fact, df_bank)

    ok(f"Matches generados: {len(df_match)}")
    ok(f"Detalles generados: {len(df_det)}")

    return {
        "facturas": df_fact,
        "bancos": df_bank,
        "clientes": clientes,
        "matches": df_match,
        "detalles": df_det,
    }


# ============================================================
#  TEST CONTROLADO – SOLO SI SE EJECUTA DIRECTAMENTE
# ============================================================
if __name__ == "__main__":
    info("=== TEST E2E · FULL RUN PULSEFORGE ===")

    resultados = run_full_pipeline()

    df_f = resultados.get("facturas")
    df_b = resultados.get("bancos")
    df_m = resultados.get("matches")
    df_d = resultados.get("detalles")

    # Vista previa controlada por logger (sin prints)
    if df_f is not None and not df_f.empty:
        info("Vista previa FACTURAS (5 filas):")
        info(df_f.head().to_string())
    else:
        warn("Sin facturas para mostrar.")

    if df_b is not None and not df_b.empty:
        info("Vista previa BANCOS (5 filas):")
        info(df_b.head().to_string())
    else:
        warn("Sin movimientos bancarios para mostrar.")

    if df_m is not None and not df_m.empty:
        info("Vista previa MATCHES (5 filas):")
        info(df_m.head().to_string())
    else:
        warn("Sin matches para mostrar.")

    if df_d is not None and not df_d.empty:
        info("Vista previa DETALLES (5 filas):")
        info(df_d.head().to_string())
    else:
        warn("Sin detalles para mostrar.")

    ok("=== FULL RUN COMPLETADO CON ÉXITO ===")
