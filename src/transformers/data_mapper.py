#  src/transformers/data_mapper.py
from __future__ import annotations

import sys
import re
import hashlib
import unicodedata
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional

# ------------------------------------------------------------
#  Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
#  Core
# ------------------------------------------------------------
from src.core.logger import info, ok, warn
from src.core.env_loader import get_config
from src.core.utils import clean_amount


# ============================================================
#  NORMALIZACIÓN DE NOMBRES
# ============================================================
def normalize_colname(name: str) -> str:
    """
    Estándar corporativo:
    - Minúsculas
    - Sin acentos invisibles
    - Sin espacios raros
    """
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip().lower()


def normalize_dataframe_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, str]]:
    """
    Retorna:
    - df con columnas normalizadas
    - mapping { columna_normalizada : columna_original }
    """
    mapping = {}
    new_cols = []
    for col in df.columns:
        norm = normalize_colname(col)
        mapping[norm] = col
        new_cols.append(norm)
    df_norm = df.copy()
    df_norm.columns = new_cols
    return df_norm, mapping


# ============================================================
#  DATAMAPPER · TRANSFORMADOR RAW (SIN CÁLCULOS)
# ============================================================
class DataMapper:

    def __init__(self):
        info("Inicializando DataMapper PulseForge…")

        cfg = get_config()
        self.cols_fact = cfg.columnas_facturas or {}
        self.cols_bank = cfg.columnas_bancos or {}
        self.tablas_bancos = cfg.tablas_bancos or {}

        if not self.cols_fact:
            raise KeyError("columnas_facturas no definidas en settings.json")

        ok("DataMapper cargado correctamente (modo RAW).")

    # ------------------------------------------------------------
    #  HASH ESTABLE
    # ------------------------------------------------------------
    @staticmethod
    def _make_hash(fields: Dict[str, Any]) -> str:
        try:
            base = "|".join(str(fields.get(k, "")) for k in sorted(fields.keys()))
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception:
            return ""

    # ------------------------------------------------------------
    #  PARSEO DE COMBINADA (FE01-534)
    # ------------------------------------------------------------
    @staticmethod
    def _parse_combinada(val: str) -> tuple[Optional[str], Optional[str]]:
        if not val or not isinstance(val, str):
            return None, None
        m = re.match(r"([A-Za-z0-9]+)-([0-9]+)", val)
        return (m.group(1), m.group(2)) if m else (None, None)

    @staticmethod
    def _serie_invalida(serie: str) -> bool:
        if not serie:
            return True
        return bool(re.match(r"^[0-9]{1,3}$", serie))

    # ------------------------------------------------------------
    #  EXTRACTOR MULTICOLUMNA
    # ------------------------------------------------------------
    def _pick(self, row: pd.Series, posibles: List[str], colmap: Dict[str, str]):
        """
        Busca una columna usando los alias normalizados de settings.json.
        """
        if not posibles:
            return None

        for alias in posibles:
            alias_norm = normalize_colname(alias)
            if alias_norm in colmap:
                orig = colmap[alias_norm]
                val = row.get(orig)
                if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                    return val
        return None

    # ============================================================
    #  MAPEO CLIENTES
    # ============================================================
    def map_clientes(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando clientes (RAW)…")

        df_norm, colmap = normalize_dataframe_columns(df)
        clientes: list[dict] = []

        for _, row in df_norm.iterrows():
            ruc = str(row.get("ruc") or "").strip()
            rz = str(row.get("razon_social") or "").strip()

            if not ruc:
                continue

            cli = {
                "ruc": ruc,
                "razon_social": rz,
            }
            cli["source_hash"] = self._make_hash(cli)
            clientes.append(cli)

        ok(f"Clientes mapeados: {len(clientes)}")
        return clientes

    # ============================================================
    #  MAPEO FACTURAS
    # ============================================================
    def map_facturas(self, df: pd.DataFrame) -> list[dict]:
        info("Mapeando facturas (RAW)…")

        df_norm, colmap = normalize_dataframe_columns(df)
        facturas: list[dict] = []
        c = self.cols_fact

        for idx, row in df_norm.iterrows():
            try:
                # -----------------------------
                # SUBTOTAL / IGV / TOTAL
                # -----------------------------
                subtotal = clean_amount(self._pick(row, c.get("subtotal"), colmap))
                igv = clean_amount(self._pick(row, c.get("igv"), colmap))
                total = clean_amount(self._pick(row, c.get("total"), colmap))

                # -----------------------------
                # IDENTIFICACIÓN
                # -----------------------------
                ruc = str(self._pick(row, c.get("ruc"), colmap) or "").strip()
                cliente = str(self._pick(row, c.get("cliente_generador"), colmap) or "").strip()

                # -----------------------------
                # COMBINADA → (serie-numero)
                # -----------------------------
                combinada = str(self._pick(row, c.get("combinada"), colmap) or "").strip()
                serie, numero = self._parse_combinada(combinada)

                # fallback serie
                if not serie:
                    sr = str(self._pick(row, c.get("serie"), colmap) or "").strip()
                    if sr and not self._serie_invalida(sr):
                        serie = sr

                # fallback numero
                if not numero:
                    nr = str(self._pick(row, c.get("numero"), colmap) or "").strip()
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
                    "combinada": combinada,
                    "fecha_emision": self._pick(row, c.get("fecha_emision"), colmap),
                    "vencimiento": self._pick(row, c.get("vencimiento"), colmap),
                    "estado_fs": str(self._pick(row, c.get("estado_fs"), colmap) or "").strip(),
                    "estado_cont": str(self._pick(row, c.get("estado_cont"), colmap) or "").strip(),
                    "fue_cobrado": 0,
                    "match_id": None,
                }

                factura["source_hash"] = self._make_hash(factura)
                facturas.append(factura)

            except Exception as e:
                warn(f"[Factura {idx}] Error → {e}")

        ok(f"Facturas mapeadas: {len(facturas)}")
        return facturas

    # ============================================================
    #  MAPEO BANCOS
    # ============================================================
    def map_bancos(self, df: pd.DataFrame, nombre_tabla: str) -> list[dict]:
        info(f"Mapeando movimientos bancarios desde '{nombre_tabla}'…")

        df_norm, colmap = normalize_dataframe_columns(df)
        movimientos: list[dict] = []

        # identificar banco por tabla
        codigo = next(
            (alias for alias, tabla in self.tablas_bancos.items() if tabla == nombre_tabla),
            None
        )
        if not codigo:
            warn(f"No se encontró código de banco para tabla {nombre_tabla}")
            return []

        c = self.cols_bank

        for idx, row in df_norm.iterrows():
            try:
                monto = clean_amount(self._pick(row, c.get("monto"), colmap))
                moneda = str(self._pick(row, c.get("moneda"), colmap) or "").upper().strip()

                mov = {
                    "fecha": self._pick(row, c.get("fecha"), colmap),
                    "tipo_mov": str(self._pick(row, c.get("tipo_mov"), colmap) or "").strip(),
                    "descripcion": str(self._pick(row, c.get("descripcion"), colmap) or "").strip(),
                    "operacion": str(self._pick(row, c.get("operacion"), colmap) or "").strip(),
                    "destinatario": str(self._pick(row, c.get("destinatario"), colmap) or "").strip(),
                    "tipo_documento": str(self._pick(row, c.get("tipo_documento"), colmap) or "").strip(),
                    "monto": monto,
                    "moneda": moneda,
                    "banco_codigo": codigo,
                }

                mov["source_hash"] = self._make_hash(mov)
                movimientos.append(mov)

            except Exception as e:
                warn(f"[Banco {codigo} fila {idx}] Error → {e}")

        ok(f"Movimientos mapeados: {len(movimientos)}")
        return movimientos
