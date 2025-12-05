# src/pipelines/pipeline_bancos.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · PIPELINE BANCOS (COMPATIBLE CON list[dict])
# ============================================================
import sys
from pathlib import Path

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
        Flujo completo:
        1) Extraer (BankExtractor → list[dict])
        2) Insertar (BankWriter → movimientos_pf)
        """
        try:
            info("🔎 Extrayendo movimientos de todos los bancos…")
            movimientos = self.extractor.extract()  # <-- LIST[DICT]

            if not movimientos:
                warn("No hay movimientos bancarios para procesar.")
                return 0

            ok(f"Total movimientos extraídos: {len(movimientos)}")

            info("💾 Insertando movimientos en PulseForge…")
            inserted = self.writer.save_bancos(movimientos, reset=reset)

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
        ok(f"Test finalizado. Registros insertados: {inserted}")

    except Exception as e:
        error(f"Fallo en test de PipelineBancos: {e}")
    