# src/transformers/data_mapper.py
from __future__ import annotations

import sys
import pandas as pd
import hashlib
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# CORE
from src.core.logger import info, ok, warn
from src.core.env_loader import get_config
from src.core.utils import clean_amount


# ============================================================
#   HELPERS INTERNOS – NORMALIZACIÓN DE NOMBRES DE COLUMNAS
# ============================================================
def normalize_colname(name: str) -> str:
    """
    Normaliza nombres de columna para evitar errores:
    - Espacios invisibles (NBSP, tabs, unicode)
    - Mayúsculas/minúsculas
    - Trailing/leading spaces
    """

    if not name:
        return ""

    # Normalizar unicode (quita acentos invisibles)
    name = unicodedata.normalize("NFKD", name)

    # Reemplazar cualquier espacio raro por un espacio normal
    name = re.sub(r"\s+", " ", name)

    return name.strip().lower()


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve un DF con columnas estandarizadas SIN perder nombre original."""
    mapping = {}
    for col in df.columns:
        mapping[col] = normalize_colname(col)
    df_ren = df.copy()
    df_ren.columns = [normalize_colname(c) for c in df.columns]
    return df_ren, mapping


# ============================================================
#   DATAMAPPER • TRANSFORMADOR CENTRAL (RAW / SIN CÁLCULOS)
# ============================================================
class DataMapper:

    def __init__(self):
        info("Inicializando DataMapper PulseForge (RAW)…")

        cfg = get_config()

        # Config general
        self.cols_fact = cfg.columnas_facturas or {}
        self.cols_bank = cfg.columnas_bancos or {}
        self.map_tablas_bancos = cfg.tablas_bancos or {}

        if not self.cols_fact:
            raise KeyError("Falta columnas_facturas en config.")

        ok("DataMapper cargado correctamente (sin cálculos financieros).")


    # ============================================================
    #   HASH
    # ============================================================
    def _make_hash(self, fila: dict) -> str:
        try:
            base = "|".join([str(fila.get(k, "")) for k in sorted(fila.keys())])
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception:
            return ""


    # ============================================================
    #   PARSEO SERIE / NUMERO
    # ============================================================
    def _parse_combinada(self, combinada: str):
        """Ej: FE01-534 → (FE01, 534)"""
        if not combinada or not isinstance(combinada, str):
            return None, None

        m = re.match(r"([A-Za-z0-9]+)-([0-9]+)", combinada.strip())
        return (m.group(1), m.group(2)) if m else (None, None)

    def _is_invalid_series(self, serie: str) -> bool:
        if not serie:
            return True
        return bool(re.match(r"^[0-9]{1,3}$", serie))  # Ej: 1, 02, 15

    # ============================================================
    #   EXTRACTOR MULTICOLUMNA
    # ============================================================
    def _extract_multi(self, row, posibles, df_normalized_cols):
        """
        Busca una columna en versión normalizada.
        posibles → lista de alias definidos en settings.json
        """
        if not posibles:
            return None

        if not isinstance(posibles, list):
            posibles = [posibles]

        for alias in posibles:
            alias_norm = normalize_colname(alias)
            if alias_norm in df_normalized_cols:
                original_col = df_normalized_cols[alias_norm]
                val = row.get(original_col)
                if pd.notna(val) and str(val).strip() not in ["", "nan", "None"]:
                    return val
        return None

    # ============================================================
    #   MAPEO CLIENTES
    # ============================================================
    def map_clientes(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando clientes…")

        df_norm, colmap = normalize_dataframe_columns(df)

        clientes = []
        for _, row in df_norm.iterrows():
            ruc = str(row.get("ruc", "")).strip()
            razon = str(row.get("razon_social", "")).strip()

            if not ruc:
                continue

            item = {
                "ruc": ruc,
                "razon_social": razon,
                "source_hash": self._make_hash({"ruc": ruc, "razon_social": razon})
            }

            clientes.append(item)

        ok(f"Clientes mapeados: {len(clientes)}")
        return clientes

    # ============================================================
    #   MAPEO FACTURAS
    # ============================================================
    def map_facturas(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando facturas (RAW, sin cálculos)…")

        df_norm, colmap = normalize_dataframe_columns(df)
        facturas = []
        c = self.cols_fact

        for idx, row in df_norm.iterrows():
            try:

                # ----------------------------------------------------
                # SUBTOTAL / IGV / TOTAL
                # Siempre limpiar a número con clean_amount()
                # ----------------------------------------------------
                subtotal = clean_amount(
                    self._extract_multi(row, c.get("subtotal"), colmap)
                )
                igv = clean_amount(
                    self._extract_multi(row, c.get("igv"), colmap)
                )
                total = clean_amount(
                    self._extract_multi(row, c.get("total"), colmap)
                )

                # ----------------------------------------------------
                # CAMPOS IDENTIFICACIÓN
                # ----------------------------------------------------
                ruc = str(self._extract_multi(row, c.get("ruc"), colmap) or "").strip()
                cliente = str(self._extract_multi(row, c.get("cliente_generador"), colmap) or "").strip()

                # ----------------------------------------------------
                # COMBINADA
                # ----------------------------------------------------
                combinada_val = str(self._extract_multi(row, c.get("combinada"), colmap) or "").strip()
                serie, numero = self._parse_combinada(combinada_val)

                # ----------------------------------------------------
                # FALLBACK MANUAL SERIE / NUMERO
                # ----------------------------------------------------
                if not serie:
                    sr = str(self._extract_multi(row, c.get("serie"), colmap) or "").strip()
                    if sr and not self._is_invalid_series(sr):
                        serie = sr

                if not numero:
                    nr = str(self._extract_multi(row, c.get("numero"), colmap) or "").strip()
                    if nr.isdigit():
                        numero = nr

                factura = {
                    "subtotal": subtotal,
                    "igv": igv,
                    "total": total,
                    "ruc": ruc,
                    "cliente_generador": cliente,
                    "serie": serie or "",
                    "numero": numero or "",
                    "combinada": combinada_val,
                    "fecha_emision": self._extract_multi(row, c.get("fecha_emision"), colmap),
                    "vencimiento": self._extract_multi(row, c.get("vencimiento"), colmap),
                    "estado_fs": str(self._extract_multi(row, c.get("estado_fs"), colmap) or "").strip(),
                    "estado_cont": str(self._extract_multi(row, c.get("estado_cont"), colmap) or "").strip(),
                    "fue_cobrado": 0,
                    "match_id": None,
                }

                factura["source_hash"] = self._make_hash(factura)
                facturas.append(factura)

            except Exception as e:
                warn(f"[Factura fila {idx}] Error → {e}")

        ok(f"Facturas mapeadas: {len(facturas)}")
        return facturas

    # ============================================================
    #   MAPEO BANCOS
    # ============================================================
    def map_bancos(self, df: pd.DataFrame, nombre_tabla: str) -> list[dict]:
        info(f"Mapeando banco desde tabla '{nombre_tabla}'…")

        df_norm, colmap = normalize_dataframe_columns(df)
        movimientos = []

        banco_codigo = next(
            (alias for alias, tabla in self.map_tablas_bancos.items() if tabla == nombre_tabla),
            None
        )

        if not banco_codigo:
            warn(f"No se identificó banco para {nombre_tabla}")
            return []

        c = self.cols_bank

        for _, row in df_norm.iterrows():
            try:
                monto = clean_amount(self._extract_multi(row, c.get("monto"), colmap))
                moneda = str(self._extract_multi(row, c.get("moneda"), colmap) or "").upper().strip()
                oper = str(self._extract_multi(row, c.get("operacion"), colmap) or "").strip()

                mov = {
                    "fecha": self._extract_multi(row, c.get("fecha"), colmap),
                    "tipo_mov": str(self._extract_multi(row, c.get("tipo_mov"), colmap) or "").strip(),
                    "descripcion": str(self._extract_multi(row, c.get("descripcion"), colmap) or "").strip(),
                    "operacion": oper,
                    "destinatario": str(self._extract_multi(row, c.get("destinatario"), colmap) or "").strip(),
                    "tipo_documento": str(self._extract_multi(row, c.get("tipo_documento"), colmap) or "").strip(),
                    "monto": monto,
                    "moneda": moneda,
                    "banco_codigo": banco_codigo,
                }

                mov["source_hash"] = self._make_hash(mov)
                movimientos.append(mov)

            except Exception as e:
                warn(f"Error mapeando banco {banco_codigo}: {e}")

        ok(f"Movimientos mapeados: {len(movimientos)}")
        return movimientos
