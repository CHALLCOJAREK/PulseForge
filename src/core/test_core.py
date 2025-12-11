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
# Imports Core
# -------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.utils import (
    normalize_text, clean_amount, parse_date,
    format_date_yyyymmdd, date_diff_days, clean_ruc
)
from src.core.validations import (
    validate_system_config,
    validate_igv, validate_detraccion, validate_tipo_cambio,
    validate_required, validate_positive, validate_date,
    validate_ruc, validate_amount, validate_text
)
from src.core.db import SourceDB, PulseForgeDB, NewDB

import pandas as pd


# =====================================================
#   TEST CONFIGURACIÓN UNIVERSAL
# =====================================================
def test_config():
    info("🔍 Probando carga de configuración (env + settings + constants)...")

    try:
        cfg = get_config()
        ok(f"Config cargada correctamente → Origen: {cfg.db_source}")

        # Validación completa del sistema
        validate_system_config(cfg, settings={
            "tablas": cfg.tablas,
            "tablas_bancos": cfg.tablas_bancos,
            "tabla_movimientos_unica": cfg.tabla_movimientos_unica,
            "columnas_facturas": cfg.columnas_facturas,
            "columnas_bancos": cfg.columnas_bancos
        })

        ok("Validación global del sistema → OK")

        ok("Tablas dinámicas configuradas:")
        for k, v in cfg.tablas.items():
            print(f"   - {k}: {v}")

        ok("Tablas de bancos configuradas:")
        for k, v in cfg.tablas_bancos.items():
            print(f"   - {k}: {v}")

    except Exception as e:
        error(f"ERROR en test_config: {e}")


# =====================================================
#   TEST CONEXIONES A BASES
# =====================================================
def test_db_connections():
    info("🔍 Probando conexiones a las bases de datos...")

    def try_connect(db, nombre: str):
        try:
            conn = db.connect()
            ok(f"{nombre} → Conexión OK")

            tablas = db.get_tables()
            if tablas:
                ok(f"{nombre} → {len(tablas)} tablas encontradas:")
                for t in tablas:
                    print(f"   - {t}")
            else:
                warn(f"{nombre} → Sin tablas registradas.")

            db.close()
        except Exception as e:
            error(f"{nombre} ERROR: {e}")

    try_connect(SourceDB(), "BD ORIGEN")
    try_connect(PulseForgeDB(), "BD PULSEFORGE")
    try_connect(NewDB(), "BD NUEVA")


# =====================================================
#   TEST LECTURAS REALES
# =====================================================
def test_real_data():
    info("🔍 Probando lectura real de tablas dinámicas...")

    try:
        cfg = get_config()
        db = SourceDB()
        db.connect()

        for alias, tabla in cfg.tablas.items():
            print("")
            ok(f"Revisando tabla: {tabla} (alias: {alias})")

            try:
                preview = pd.read_sql_query(f"SELECT * FROM {tabla} LIMIT 5", db.connection)
                ok(f"Vista previa OK → {len(preview)} filas")

                for _, row in preview.iterrows():
                    print("   →", dict(row))

            except Exception as e:
                warn(f"No se pudo leer tabla '{tabla}': {e}")

        db.close()

    except Exception as e:
        error(f"ERROR en test_real_data: {e}")


# =====================================================
#   TEST UTILS
# =====================================================
def test_utils():
    info("🔍 Probando funciones del módulo utils...")

    try:
        ok("normalize_text → " + normalize_text("ÁB;C DéF / Co.&123"))
        ok("clean_amount → " + str(clean_amount("S/ 1,234.56")))
        ok("parse_date → " + str(parse_date("20 Ene 2024")))
        ok("format_date_yyyymmdd → " + str(format_date_yyyymmdd(parse_date("2024-01-20"))))
        ok("date_diff_days → " + str(date_diff_days(parse_date("2024-01-01"), parse_date("2024-01-10"))))
        ok("clean_ruc → " + clean_ruc("20-12345678"))
    except Exception as e:
        error(f"Utils ERROR: {e}")


# =====================================================
#   TEST VALIDATIONS
# =====================================================
def test_validations():
    info("🔍 Probando validaciones...")

    try:
        ok("IGV → OK: " + str(validate_igv(0.18)))
        ok("Detracción → OK: " + str(validate_detraccion(0.12)))
        ok("Tipo Cambio → OK: " + str(validate_tipo_cambio(3.80)))

        ok("validate_required → " + str(validate_required("A")))
        ok("validate_positive → " + str(validate_positive(10)))
        ok("validate_date → " + str(validate_date("2024-01-02")))
        ok("validate_ruc → " + str(validate_ruc("20123456789")))
        ok("validate_amount → " + str(validate_amount("1,234.56")))
        ok("validate_text → " + str(validate_text("ÁB CD123")))

    except Exception as e:
        error(f"Validations ERROR: {e}")


# =====================================================
#   RUNNER
# =====================================================
if __name__ == "__main__":
    info("=== INICIANDO TEST CORE PULSEFORGE ===")

    test_config()
    test_db_connections()
    test_real_data()
    test_utils()
    test_validations()

    ok("=== TEST CORE COMPLETADO ===")
