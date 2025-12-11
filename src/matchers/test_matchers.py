#  src/matchers/test_matchers.py
from __future__ import annotations
import sys
import sqlite3
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
from src.matchers.matcher_engine import MatcherEngine


# ============================================================
#      TEST MATCHERS — MOTOR COMPLETO DE MATCH (E2E)
# ============================================================
def main():

    info("=== TEST MATCHERS · MATCH ENGINE (E2E CONTROLADO) ===")

    # --------------------------------------------------------
    # 1. CARGAR CONFIGURACIÓN
    # --------------------------------------------------------
    cfg = get_config()
    db_path = cfg.db_pulseforge
    ok(f"BD destino: {db_path}")

    # --------------------------------------------------------
    # 2. LEER FACTURAS + BANCOS DIRECTO DESDE BD DESTINO
    # --------------------------------------------------------
    info("Leyendo facturas y bancos desde la BD destino…")

    try:
        conn = sqlite3.connect(db_path)
        df_fact = pd.read_sql_query("SELECT * FROM facturas_pf;", conn)
        df_bank = pd.read_sql_query("SELECT * FROM bancos_pf;", conn)
        conn.close()
    except Exception as e:
        error(f"❌ Error leyendo BD: {e}")
        return

    if df_fact.empty:
        warn("❌ No se encontraron facturas en facturas_pf.")
        return

    if df_bank.empty:
        warn("❌ No se encontraron movimientos en bancos_pf.")
        return

    ok(f"Facturas cargadas: {len(df_fact)}")
    ok(f"Movimientos cargados: {len(df_bank)}")

    # --------------------------------------------------------
    # 3. EJECUTAR MATCHER ENGINE
    # --------------------------------------------------------
    info("Ejecutando MatcherEngine.run()…")

    engine = MatcherEngine()
    matches_df, detalles_df = engine.run(df_fact, df_bank)

    # --------------------------------------------------------
    # 4. VALIDACIÓN DE RESULTADOS
    # --------------------------------------------------------
    ok(f"Matches generados: {len(matches_df)}")
    ok(f"Detalles generados: {len(detalles_df)}")

    # Vista previa
    if not matches_df.empty:
        info("Vista previa MATCHES (primeras 5 filas):")
        print(matches_df.head())
    else:
        warn("⚠ No se generaron matches.")

    if not detalles_df.empty:
        info("Vista previa DETALLES (primeras 5 filas):")
        print(detalles_df.head())
    else:
        warn("⚠ No se generaron detalles.")

    ok("=== TEST MATCHERS COMPLETADO ===")


# ============================================================
# EJECUCIÓN DIRECTA DEL TEST
# ============================================================
if __name__ == "__main__":
    main()
