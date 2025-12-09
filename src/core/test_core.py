# src/core/test_core.py
from __future__ import annotations

# -------------------------
# Bootstrap
# -------------------------
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# -------------------------
# Imports core
# -------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB, PulseForgeDB, NewDB
from src.core.utils import (
    normalize_text, clean_amount, parse_date,
    format_date_yyyymmdd, date_diff_days, clean_ruc
)
from src.core.validations import (
    validate_igv, validate_detraccion, validate_tipo_cambio,
    validate_required, validate_positive, validate_date, validate_ruc
)


# -------------------------
# Test configuración
# -------------------------
def test_config():
    info("Probando carga de configuración...")
    try:
        cfg = get_config()
        ok(f"Config cargada → DB Origen: {cfg.db_source}")

        ok("Tablas dinámicas detectadas:")
        for k, v in cfg.tablas.items():
            print(f"   - {k} → {v}")

    except Exception as e:
        error(f"Error en test_config: {e}")


# -------------------------
# Test conexión real
# -------------------------
def test_db_connections():
    info("Probando conexiones reales a bases de datos...")

    def real_check(db, nombre: str):
        try:
            db.connect()
            ok(f"{nombre} → Conexión abierta.")

            cur = db.connection.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]

            ok(f"{nombre} → Tablas detectadas:")
            for t in tables:
                print(f"   - {t}")

            if tables:
                first = tables[0]
                cur.execute(f"SELECT COUNT(*) FROM {first}")
                count = cur.fetchone()[0]
                ok(f"{nombre} → Tabla '{first}' tiene {count} filas.")
            else:
                warn(f"{nombre} no tiene tablas.")

            db.close()

        except Exception as e:
            error(f"{nombre} ERROR REAL: {e}")

    real_check(SourceDB(), "BD ORIGEN")
    real_check(PulseForgeDB(), "BD PULSEFORGE")
    real_check(NewDB(), "BD NUEVA")


# -------------------------
# Test lectura real de datos
# -------------------------
def test_real_data():
    info("Probando lectura real de datos desde BD Origen...")

    try:
        cfg = get_config()
        db = SourceDB()
        db.connect()

        tabla_facturas = cfg.tablas.get("facturas")

        if not tabla_facturas:
            warn("No se encontró la clave 'facturas' en settings.json.")
            return

        cur = db.connection.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablas_existentes = [x[0] for x in cur.fetchall()]

        if tabla_facturas not in tablas_existentes:
            warn(f"La tabla '{tabla_facturas}' no existe en la base origen.")
            return

        import pandas as pd
        df = pd.read_sql_query(f"SELECT * FROM {tabla_facturas} LIMIT 5", db.connection)

        ok(f"Tabla '{tabla_facturas}' → {len(df)} filas mostradas (vista previa):")

        for _, row in df.iterrows():
            print("   -", dict(row))

        db.close()

    except Exception as e:
        error(f"Lectura real ERROR: {e}")


# -------------------------
# Test utils
# -------------------------
def test_utils():
    info("Probando utils...")

    try:
        ok("normalize_text → " + normalize_text("ÁbC DéF & Co."))
        ok("clean_amount → " + str(clean_amount("S/ 1,234.56")))
        ok("parse_date → " + str(parse_date("2024-01-20")))
        ok("format_date_yyyymmdd → " + format_date_yyyymmdd(parse_date("2024-01-20")))
        ok("date_diff_days → " + str(date_diff_days(parse_date("2024-01-01"), parse_date("2024-01-10"))))
        ok("clean_ruc → " + clean_ruc("20-12345678"))
    except Exception as e:
        error(f"Utils ERROR: {e}")


# -------------------------
# Test validations
# -------------------------
def test_validations():
    info("Probando validaciones...")

    try:
        ok("IGV → " + str(validate_igv(0.18)))
        ok("Detracción → " + str(validate_detraccion(0.12)))
        ok("TC → " + str(validate_tipo_cambio(3.8)))

        ok("validate_required → " + str(validate_required("A")))
        ok("validate_positive → " + str(validate_positive(10)))
        ok("validate_date → " + str(validate_date("2024-01-02")))
        ok("validate_ruc → " + str(validate_ruc("20123456789")))
    except Exception as e:
        error(f"Validations ERROR: {e}")


# -------------------------
# Runner
# -------------------------
if __name__ == "__main__":
    info("=== INICIANDO TEST CORE PULSEFORGE ===")
    test_config()
    test_db_connections()
    test_real_data()          # ← NUEVA PRUEBA REAL
    test_utils()
    test_validations()
    ok("=== TEST CORE COMPLETADO ===")
