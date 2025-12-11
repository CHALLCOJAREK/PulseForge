# src/main.py
from __future__ import annotations

import sys
import traceback
from pathlib import Path

# ============================================================
#  BOOTSTRAP GLOBAL
# ============================================================
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports corporativos
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import PulseForgeDB
from src.loaders.newdb_builder import NewDBBuilder

# PIPELINES
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_clients import PipelineClientes
from src.pipelines.pipeline_matcher import PipelineMatcher

# WRITER MATCH
from src.loaders.match_writer import MatchWriter


# ============================================================
#  MOTOR PRINCIPAL DE PULSEFORGE
# ============================================================
def main(full_reset: bool = False):
    info("=== 🚀 INICIANDO PULSEFORGE (MAIN ENGINE) ===")

    cfg = get_config()
    db_path = cfg.db_destino

    # ========================================================
    # 0. CREACIÓN O RESETEO DE LA BD
    # ========================================================
    if full_reset:
        warn("Reseteo completo activado: reconstruyendo pulseforge.sqlite…")
        NewDBBuilder().build(reset=True)
    else:
        if not Path(db_path).exists():
            warn("BD no encontrada → generando nueva automáticamente.")
            NewDBBuilder().build(reset=True)
        else:
            ok("BD destino encontrada. Se usará modo incremental.")

    db = PulseForgeDB()  # apertura segura

    # ========================================================
    # 1. PIPELINE CLIENTES
    # ========================================================
    info("📌 PIPELINE CLIENTES — Extrayendo, mapeando y cargando…")
    clientes_list = PipelineClientes().process()

    if clientes_list:
        ok(f"Clientes procesados: {len(clientes_list)}")
    else:
        warn("No se detectaron clientes nuevos.")

    # ========================================================
    # 2. PIPELINE FACTURAS
    # ========================================================
    info("📌 PIPELINE FACTURAS — Extrayendo, mapeando, calculando…")
    df_fact = PipelineFacturas().process()

    if df_fact is not None and not df_fact.empty:
        ok(f"Facturas procesadas: {len(df_fact)}")
    else:
        warn("No se detectaron facturas.")

    # ========================================================
    # 3. PIPELINE BANCOS
    # ========================================================
    info("📌 PIPELINE BANCOS — Extrayendo, normalizando y cargando…")
    df_bank = PipelineBancos().process()

    if df_bank is not None and not df_bank.empty:
        ok(f"Movimientos bancarios procesados: {len(df_bank)}")
    else:
        warn("No se detectaron movimientos.")

    # ========================================================
    # 4. MATCHER PIPELINE COMPLETO
    # ========================================================
    info("🔍 Ejecutando MATCH COMPLETO…")

    pm = PipelineMatcher()
    df_matches, df_detalles = pm.process()

    ok(f"Matches generados: {len(df_matches)}")
    ok(f"Detalles generados: {len(df_detalles)}")

    # ========================================================
    # 5. GUARDAR RESULTADOS DEL MATCH
    # ========================================================
    info("💾 Guardando resultados del MATCH…")

    mw = MatchWriter()
    mw.save(df_matches, df_detalles, reset=full_reset)

    ok("Match guardado correctamente en BD destino.")
    ok("✨ PulseForge finalizado sin errores.")


# ============================================================
# ENTRYPOINT DIRECTO
# ============================================================
if __name__ == "__main__":
    try:
        # Modo full-reset para reconstruir todo desde cero.
        main(full_reset=True)
    except Exception as e:
        error("❌ Error crítico en PulseForge:")
        error(str(e))
        print(traceback.format_exc())
