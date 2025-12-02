# src/pipelines/pipeline_bancos.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE BANCOS
#  Extrae → Mapea → Inserta en pulseforge.sqlite
# ============================================================
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.extractors.bank_extractor import BankExtractor
from src.loaders.bank_writer import BankWriter


class PipelineBancos:

    def __init__(self) -> None:
        info("Inicializando PipelineBancos…")

        self.extractor = BankExtractor()
        self.writer = BankWriter()

        ok("PipelineBancos inicializado correctamente.")

    # --------------------------------------------------------
    def run(self, reset: bool = False) -> int:
        """
        Corre el flujo completo:
        1) Extraer movimientos desde DataPulse
        2) Normalizar (BankExtractor + DataMapper)
        3) Insertar en movimientos_pf (BankWriter)
        """
        try:
            info("🔎 Extrayendo movimientos de todos los bancos…")
            df_bancos = self.extractor.get_bancos_mapeados()

            if df_bancos is None or df_bancos.empty:
                warn("No se encontraron movimientos bancarios. Pipeline finalizado sin inserciones.")
                return 0

            ok(f"Total movimientos extraídos: {len(df_bancos)}")

            info("💾 Insertando movimientos en PulseForge…")
            inserted = self.writer.save_bancos(df_bancos, reset=reset)

            ok(f"Pipeline de bancos completado. Registros insertados: {inserted}")
            return inserted

        except Exception as e:
            error(f"Error en PipelineBancos: {e}")
            raise


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local de PipelineBancos…")

        pipeline = PipelineBancos()
        inserted = pipeline.run(reset=True)

        ok(f"Test de PipelineBancos finalizado. Registros insertados: {inserted}")

    except Exception as e:
        error(f"Fallo en test de PipelineBancos: {e}")
