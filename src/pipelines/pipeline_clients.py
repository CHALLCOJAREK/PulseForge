# src/pipelines/pipeline_clients.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE CLIENTS
#  Orquesta: Extract → Transform → Load para CLIENTES
# ============================================================
import sys
from pathlib import Path

# Bootstrap rutas
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.extractors.clients_extractor import ClientsExtractor
from src.loaders.clients_writer import ClientsWriter


class PipelineClients:
    """
    Pipeline corporativo para cargar clientes desde DataPulse → PulseForge.
    """

    def __init__(self):
        info("Inicializando PipelineClients…")
        self.extractor = ClientsExtractor()
        self.writer = ClientsWriter()
        ok("PipelineClients inicializado correctamente.")

    # --------------------------------------------------------
    def run(self, reset: bool = False) -> int:
        """
        Ejecuta el pipeline de clientes completo.
        """
        try:
            info("Iniciando extracción de clientes…")
            df = self.extractor.get_clientes_mapeados()
            ok(f"Clientes extraídos: {len(df)}")

            info("Iniciando carga de clientes en PulseForge…")
            inserted = self.writer.save_clientes(df=df, reset=reset)
            ok(f"Pipeline de clientes completado. Registros insertados: {inserted}")

            return inserted

        except Exception as e:
            error(f"Error en PipelineClients: {e}")
            raise


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de PipelineClients…")
        pipeline = PipelineClients()
        inserted = pipeline.run(reset=True)
        ok(f"Test de PipelineClients finalizado. Registros insertados: {inserted}")

    except Exception as e:
        error(f"Fallo en test de PipelineClients: {e}")
