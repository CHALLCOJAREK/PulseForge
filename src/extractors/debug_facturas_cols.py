from __future__ import annotations

# ------------------------------------------------------------
# Bootstrap rutas (igual que todos los extractores)
# ------------------------------------------------------------
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports principales
# ------------------------------------------------------------
from src.core.db import SourceDB
from src.core.logger import info, ok, warn, error

# ------------------------------------------------------------
# Debug columnas de facturas
# ------------------------------------------------------------
if __name__ == "__main__":
    info("=== DEBUG COLUMNAS FACTURAS ===")

    try:
        db = SourceDB()
        db.connect()

        df = db.read_query('SELECT * FROM "excel_6_control_servicios" LIMIT 1')

        columnas = df.columns.tolist()
        info("Columnas detectadas en Pandas:")
        for col in columnas:
            print(f" - {col}")

        ok("Listo. Copia estos nombres y te preparo el mapeo exacto.")

    except Exception as e:
        error(f"Error al leer columnas → {e}")
