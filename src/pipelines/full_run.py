# src/pipelines/full_run.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · FULL RUN PIPELINE (EJECUCIÓN COMPLETA)
#  Corre TODOS los pipelines en la secuencia correcta:
#     1) Clientes
#     2) Facturas
#     3) Bancos
#     4) Matcher
# ============================================================

import sys
from pathlib import Path

# Bootstrap rutas
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env

# Pipelines individuales
from src.pipelines.pipeline_clients import PipelineClients
from src.pipelines.pipeline_facturas import PipelineFacturas
from src.pipelines.pipeline_bancos import PipelineBancos
from src.pipelines.pipeline_matcher import PipelineMatcher


# ============================================================
#  FULL RUN PIPELINE
# ============================================================
class FullRun:

    def __init__(self):
        info("Inicializando FullRun Pipeline…")

        # Inicializar sub-pipelines
        self.p_clients = PipelineClients()
        self.p_facturas = PipelineFacturas()
        self.p_bancos = PipelineBancos()
        self.p_matcher = PipelineMatcher()

        ok("FullRun inicializado correctamente.")

    # --------------------------------------------------------
    def run(self, reset: bool = False) -> None:
        """
        Ejecuta TODO PulseForge en orden ideal.
        Si reset=True → limpia cada tabla antes de cargar.
        """

        info("🚀 Iniciando ejecución completa de PulseForge…")
        if reset:
            warn("RESET GLOBAL ACTIVADO → todas las tablas se limpiarán antes de cargar.")

        # 1) CLIENTES
        info("📂 [1/4] Procesando clientes…")
        try:
            n1 = self.p_clients.run(reset=reset)
            ok(f"[FULL_RUN] Clientes procesados: {n1}")
        except Exception as e:
            error(f"[FULL_RUN] Error procesando clientes: {e}")
            return

        # 2) FACTURAS
        info("📄 [2/4] Procesando facturas…")
        try:
            n2 = self.p_facturas.run(reset=reset)
            ok(f"[FULL_RUN] Facturas procesadas: {n2}")
        except Exception as e:
            error(f"[FULL_RUN] Error procesando facturas: {e}")
            return

        # 3) BANCOS
        info("🏦 [3/4] Procesando movimientos bancarios…")
        try:
            n3 = self.p_bancos.run(reset=reset)
            ok(f"[FULL_RUN] Movimientos bancarios procesados: {n3}")
        except Exception as e:
            error(f"[FULL_RUN] Error procesando bancos: {e}")
            return

        # 4) MATCHER
        info("🤖 [4/4] Ejecutando matcher…")
        try:
            n4 = self.p_matcher.run()
            ok(f"[FULL_RUN] Matches generados: {n4}")
        except Exception as e:
            error(f"[FULL_RUN] Error ejecutando matcher: {e}")
            return

        ok("🔥 FULL RUN COMPLETADO EXITOSAMENTE 🔥")


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de FullRun Pipeline…")
        fr = FullRun()
        fr.run(reset=True)
        ok("Test de FullRun OK.")
    except Exception as e:
        error(f"Fallo en test de FullRun: {e}")
