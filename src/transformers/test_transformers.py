# src/transformers/test_transformers.py
from __future__ import annotations
import sys
import sqlite3
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
# Bootstrap
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Core
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, get_env

# ------------------------------------------------------------
# Transformers
# ------------------------------------------------------------
from src.transformers.data_mapper import DataMapper
from src.transformers.calculator import Calculator


# =====================================================================
#                  TEST TRANSFORMERS · E2E
# =====================================================================
def main():

    info("=== TEST TRANSFORMERS · DATA MAPPER + CALCULATOR (E2E) ===")

    # ------------------------------------------------------------
    # 1. Configuración base
    # ------------------------------------------------------------
    cfg = get_config()
    ok("Configuración cargada correctamente.")

    db_path = cfg.db_destino or get_env("PULSEFORGE_DB_PATH")

    if not db_path:
        error("No se encontró PULSEFORGE_DB_PATH en .env")
        return

    db_path = Path(db_path)
    if not db_path.exists():
        error(f"BD destino no existe → {db_path}")
        return

    ok(f"Usando BD destino: {db_path}")

    # ------------------------------------------------------------
    # 2. Leer facturas desde BD destino
    # ------------------------------------------------------------
    info("Leyendo facturas desde facturas_pf…")

    conn = sqlite3.connect(db_path)
    df_facturas = pd.read_sql_query("SELECT * FROM facturas_pf;", conn)
    conn.close()

    if df_facturas.empty:
        warn("No hay facturas en BD — test finaliza.")
        return

    ok(f"Facturas cargadas: {len(df_facturas)}")

    # ------------------------------------------------------------
    # 3. Aplicar Calculator
    # ------------------------------------------------------------
    info("Aplicando Calculator a facturas…")

    calc = Calculator(cfg)
    df_calc = calc.process_facturas(df_facturas)

    ok("Cálculo financiero aplicado correctamente.")

    # ------------------------------------------------------------
    # 4. Verificación de columnas clave
    # ------------------------------------------------------------
    columnas_requeridas = [
        "subtotal", "igv", "total_con_igv",
        "detraccion_monto", "neto_recibido",
        "fecha_emision", "vencimiento",
        "fecha_pago",
        "ventana_inicio", "ventana_fin"
    ]


    faltantes = [c for c in columnas_requeridas if c not in df_calc.columns]
    if faltantes:
        error(f"Faltan columnas requeridas después del cálculo: {faltantes}")
    else:
        ok("Todas las columnas financieras requeridas están presentes.")

    # ------------------------------------------------------------
    # 5. Vista previa
    # ------------------------------------------------------------
    info("Vista previa (primeras 5 filas):")
    print(df_calc[columnas_requeridas].head())

    ok("=== TEST TRANSFORMERS COMPLETADO EXITOSAMENTE ===")


# =====================================================================
# Ejecución directa
# =====================================================================
if __name__ == "__main__":
    main()
