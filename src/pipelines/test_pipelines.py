#  src/pipelines/test_pipelines.py
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

# Pipelines
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes


# ============================================================
#   TEST PIPELINE FACTURAS
# ============================================================
def test_facturas():
    info("=== TEST · PIPELINE FACTURAS ===")

    pf = PipelineFacturas()
    df = pf.process()

    if df is None or df.empty:
        warn("Resultado vacío en pipeline_facturas.")
    else:
        ok(f"Facturas procesadas: {len(df)}")
        print(df.head())

    return df


# ============================================================
#   TEST PIPELINE BANCOS
# ============================================================
def test_bancos():
    info("=== TEST · PIPELINE BANCOS ===")

    pb = PipelineBancos()
    df = pb.process()

    if df is None or df.empty:
        warn("Resultado vacío en pipeline_bancos.")
    else:
        ok(f"Movimientos procesados: {len(df)}")
        print(df.head())

    return df


# ============================================================
#   TEST PIPELINE CLIENTES
# ============================================================
def test_clientes():
    info("=== TEST · PIPELINE CLIENTES ===")

    pc = PipelineClientes()
    lista = pc.process()

    if not lista:
        warn("Resultado vacío en pipeline_clients.")
    else:
        ok(f"Clientes procesados: {len(lista)}")
        print(lista[:5])

    return lista


# ============================================================
# EJECUCIÓN DIRECTA DEL TEST SUITE
# ============================================================
if __name__ == "__main__":
    info("=== TEST SUITE PULSEFORGE · PIPELINES BASE ===")

    df_f = test_facturas()
    df_b = test_bancos()
    df_c = test_clientes()

    ok("=== TEST PIPELINES COMPLETADO ===")
