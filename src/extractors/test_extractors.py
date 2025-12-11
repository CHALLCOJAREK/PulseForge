# src/extractors/test_extractors.py
from __future__ import annotations

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports principales
# ------------------------------------------------------------
import pandas as pd
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config

from src.extractors.bank_extractor import BankExtractor
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.invoices_extractor import InvoicesExtractor


# ============================================================
#   UTILIDADES DE ANÁLISIS PROFUNDO
# ============================================================
def _df_quality_report(df: pd.DataFrame, nombre: str):
    if df.empty:
        warn(f"[{nombre}] No hay análisis: DataFrame vacío.")
        return

    info(f"📊 === ANALISIS PROFUNDO: {nombre} ===")

    # Tamaño
    info(f"Filas: {len(df)} | Columnas: {len(df.columns)}")

    # Tipos de datos
    info("Tipos de datos:")
    info(str(df.dtypes))

    # Nulos
    info("Nulos por columna:")
    info(str(df.isnull().sum()))

    # Valores únicos
    info("Valores únicos (top 5 por columna):")
    for col in df.columns:
        vals = df[col].dropna().unique()
        muestra = vals[:5]
        info(f"  {col}: {muestra} (total uniques={len(vals)})")

    # Columnas vacías
    vacias = [c for c in df.columns if df[c].dropna().astype(str).str.len().sum() == 0]
    if vacias:
        warn(f"Columnas completamente vacías: {vacias}")

    # Columnas duplicadas (99% mismo valor)
    repetidas = []
    for col in df.columns:
        serie = df[col].astype(str)
        top = serie.value_counts(normalize=True, dropna=False).iloc[0]
        if top > 0.98:
            repetidas.append(col)
    if repetidas:
        warn(f"Columnas con 98%+ del mismo valor: {repetidas}")

    # Duplicados
    dups = df.duplicated().sum()
    if dups > 0:
        warn(f"Duplicados detectados: {dups}")

    # Export preview
    preview_path = Path(ROOT) / "data" / "temp" / f"preview_{nombre.lower()}.csv"
    try:
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        df.head(50).to_csv(preview_path, index=False, encoding="utf-8")
        ok(f"Preview exportada → {preview_path}")
    except Exception as e:
        warn(f"No se pudo exportar preview: {e}")


# ============================================================
#   TABLA BONITA
# ============================================================
def _pretty_table(df, max_rows=15):
    if df.empty:
        return "\n  [DF vacío]\n"

    df_show = df.head(max_rows).astype(str)
    tabla = df_show.to_string(index=False)
    return f"\n{tabla}\n"


# ============================================================
#   PRETTY LIST
# ============================================================
def _pretty_list(lst, max_items=15):
    if not lst:
        return "\n  [Lista vacía]\n"

    salida = []
    for i, item in enumerate(lst[:max_items], start=1):
        salida.append(f"{i:02d}. {item}")

    return "\n" + "\n".join(salida) + "\n"


# ============================================================
#   VALIDADORES
# ============================================================
def _validate_df(df, nombre: str):
    if df is None:
        error(f"[{nombre}] devolvió None.")
        return False

    if not isinstance(df, pd.DataFrame):
        error(f"[{nombre}] devolvió tipo incorrecto → {type(df)}")
        return False

    filas = len(df)
    columnas = len(df.columns)

    if filas == 0:
        warn(f"[{nombre}] DataFrame vacío")
    else:
        ok(f"[{nombre}] OK → {filas} filas y {columnas} columnas")

    tabla = _pretty_table(df)
    info(f"[{nombre}] Vista previa:{tabla}")

    _df_quality_report(df, nombre)

    return True


def _validate_list(lst, nombre: str):
    if not isinstance(lst, list):
        error(f"[{nombre}] devolvió tipo incorrecto → {type(lst)}")
        return False

    cantidad = len(lst)
    if cantidad == 0:
        warn(f"[{nombre}] Lista vacía")
    else:
        ok(f"[{nombre}] OK → {cantidad} elementos")

    vista = _pretty_list(lst)
    info(f"[{nombre}] Vista previa:{vista}")

    return True


# ============================================================
#   TESTS INDIVIDUALES
# ============================================================
def test_banks():
    info("=== TEST: BankExtractor ===")
    try:
        df = BankExtractor().extract()
        return _validate_df(df, "BankExtractor")
    except Exception as e:
        error(f"BankExtractor falló → {e}")
        return False


def test_clients():
    info("=== TEST: ClientsExtractor ===")
    try:
        df = ClientsExtractor().extract()
        return _validate_df(df, "ClientsExtractor")
    except Exception as e:
        error(f"ClientsExtractor falló → {e}")
        return False


def test_invoices():
    info("=== TEST: InvoicesExtractor ===")
    try:
        registros = InvoicesExtractor().extract()
        return _validate_list(registros, "InvoicesExtractor")
    except Exception as e:
        error(f"InvoicesExtractor falló → {e}")
        return False


# ============================================================
#   MAIN
# ============================================================
if __name__ == "__main__":
    info("=== INICIANDO TEST DE EXTRACTORES (MODO ANALISIS PROFUNDO) ===")

    cfg = get_config()
    info(f"Modo: {cfg.env} | Run Mode: {cfg.run_mode}")

    r1 = test_banks()
    r2 = test_clients()
    r3 = test_invoices()

    ok("=== RESULTADOS ===")
    print(f"BankExtractor ………… {'OK' if r1 else 'FAIL'}")
    print(f"ClientsExtractor …… {'OK' if r2 else 'FAIL'}")
    print(f"InvoicesExtractor … {'OK' if r3 else 'FAIL'}")

    info("=== FIN TEST EXTRACTORES PULSEFORGE ===")
