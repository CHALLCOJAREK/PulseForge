# src/pipelines/pipeline_facturas.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE FACTURAS
#  Extract → Transform → Load para FACTURAS
# ============================================================
import sys
from pathlib import Path

# Bootstrap rutas
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.extractors.invoices_extractor import InvoicesExtractor
from src.loaders.invoice_writer import InvoiceWriter


class PipelineFacturas:
    """
    Pipeline empresarial para cargar facturas desde DataPulse → PulseForge.
    - Extrae desde SQLite
    - Normaliza y mapea (DataMapper)
    - Calcula campos de ventana de pago (ya lo hace DataMapper + Calculator)
    - Inserta en facturas_pf
    """

    def __init__(self):
        info("Inicializando PipelineFacturas…")
        self.extractor = InvoicesExtractor()
        self.writer = InvoiceWriter()
        ok("PipelineFacturas inicializado correctamente.")

    # --------------------------------------------------------
    def run(self, reset: bool = False) -> int:
        """
        Ejecuta el pipeline completo.
        reset=True → limpia facturas_pf antes de insertar.
        """
        try:
            info("Iniciando extracción de facturas…")
            df = self.extractor.get_facturas_mapeadas()
            ok(f"Facturas extraídas y mapeadas: {len(df)}")

            info("Iniciando carga de facturas en PulseForge…")
            inserted = self.writer.save_facturas(df_facturas=df, reset=reset)
            ok(f"Pipeline de facturas completado. Registros insertados: {inserted}")

            return inserted

        except Exception as e:
            error(f"Error en PipelineFacturas: {e}")
            raise


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de PipelineFacturas…")
        pipeline = PipelineFacturas()
        inserted = pipeline.run(reset=True)
        ok(f"Test de PipelineFacturas finalizado. Filas insertadas: {inserted}")

    except Exception as e:
        error(f"Fallo en test de PipelineFacturas: {e}")
