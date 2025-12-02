# src/transformers/calculator.py
from __future__ import annotations

# ------------------------------------------------------------
#  BOOTSTRAP RUTAS — PARA QUE FUNCIONE AL EJECUTAR DIRECTO
# ------------------------------------------------------------
import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parents[2]   # <-- Punto CLAVE

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
# ------------------------------------------------------------

import pandas as pd
from datetime import timedelta
from typing import Optional

# Core PulseForge
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config, PulseForgeConfig
from src.core.validations import (
    validate_igv,
    validate_detraccion,
    validate_tipo_cambio,
)

# IA
from src.transformers.ai_helpers import ai_classify


# ============================================================
#  CALCULATOR · MOTOR FINANCIERO
# ============================================================
class Calculator:
    """
    Motor de cálculo financiero de PulseForge:
      - Normaliza y enriquece facturas.
      - Normaliza y enriquece movimientos bancarios.
      - Aplica reglas de negocio (IGV, detracción, tolerancias, TC, etc.).
      - Opcionalmente usa IA para clasificar movimientos.
    """

    def __init__(self, cfg: Optional[PulseForgeConfig] = None):
        info("Inicializando Calculator PulseForge…")

        # Config centralizada
        self.cfg: PulseForgeConfig = cfg or get_config()

        # Validaciones empresariales → blindaje de configuración
        self.igv: float = float(validate_igv(self.cfg.igv))
        self.detraccion: float = float(validate_detraccion(self.cfg.detraccion_porcentaje))
        self.days_tolerance: int = int(self.cfg.days_tolerance_pago)
        self.monto_variacion: float = float(self.cfg.monto_variacion)
        self.tipo_cambio_usd_pen: float = float(validate_tipo_cambio(self.cfg.tipo_cambio_usd_pen))

        self.activar_ia: bool = bool(self.cfg.activar_ia)
        self.modo_debug: bool = bool(self.cfg.modo_debug)

        ok("Calculator inicializado correctamente con configuración centralizada.")

    # -------------------------------------------------------
    #   Helpers internos
    # -------------------------------------------------------
    @staticmethod
    def _parse_forma_pago(value) -> int:
        """
        Extrae la cantidad de días de la forma de pago.
        Casos:
          - 'CONTADO' → 0
          - 'CREDITO' sin días → 0
          - 'CREDITO 30 DÍAS', '30D', '30 días' → 30
        """
        if value is None:
            return 0

        txt = str(value).strip().lower()

        if not txt:
            return 0

        if "contado" in txt:
            return 0

        # 'credito' sin números explícitos → se asume 0
        if "credito" in txt and not any(ch.isdigit() for ch in txt):
            return 0

        # extraer dígitos
        dias_str = "".join(ch for ch in txt if ch.isdigit())
        try:
            return int(dias_str) if dias_str else 0
        except Exception:
            return 0

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura que las columnas estén limpias:
          - Strips
          - Nombres estándares cuando sea posible
        """
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # =======================================================
    #   FACTURAS
    # =======================================================
    def process_facturas(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica cálculos financieros a las facturas:
          - IGV
          - Total con IGV
          - Detracción
          - Neto recibido
          - Monto abono a banco (sin detracción)
          - Monto abono a cuenta de detracción
          - Días de pago desde forma_pago
          - Ventana de pago (fecha límite ± tolerancia)

        Requiere, como mínimo, columnas:
          - subtotal
          - fecha_emision
          - forma_pago
        """
        info("Aplicando cálculos financieros a facturas…")

        df = self._normalize_columns(df_facturas)

        # Subtotal → numérico
        if "subtotal" not in df.columns:
            error("La tabla de facturas no contiene la columna 'subtotal'.")
            raise ValueError("Falta columna obligatoria 'subtotal' en facturas.")

        df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0)

        # Fecha emisión
        if "fecha_emision" not in df.columns:
            error("La tabla de facturas no contiene la columna 'fecha_emision'.")
            raise ValueError("Falta columna obligatoria 'fecha_emision' en facturas.")

        df["fecha_emision"] = pd.to_datetime(df["fecha_emision"], errors="coerce")

        # Forma de pago (crudo)
        if "forma_pago" not in df.columns:
            warn("No se encontró 'forma_pago' en facturas. Se asumirá contado (0 días).")
            df["forma_pago"] = ""

        # Días de pago calculados
        df["dias_pago"] = df["forma_pago"].apply(self._parse_forma_pago)

        # Filtrar solo facturas con subtotal > 0
        df_valid = df[df["subtotal"] > 0].copy()

        if df_valid.empty:
            warn("No hay facturas válidas (subtotal > 0).")
            return df_valid

        # ====================================================
        #  CÁLCULO FINANCIERO — EXACTO A LO QUE ME EXPLICASTE
        # ====================================================
        # 1) IGV
        df_valid["igv"] = (df_valid["subtotal"] * self.igv).round(2)

        # 2) Total con IGV
        df_valid["total_con_igv"] = (df_valid["subtotal"] + df_valid["igv"]).round(2)

        # 3) Monto detracción (lo que se va a la cuenta de detracción SUNAT)
        df_valid["detraccion_monto"] = (df_valid["total_con_igv"] * self.detraccion).round(2)

        # 4) Neto recibido en banco (monto que realmente entra a la cuenta corriente)
        df_valid["neto_recibido"] = (df_valid["total_con_igv"] - df_valid["detraccion_monto"]).round(2)

        # Alias de negocio claros (para cruce con bancos / reportes)
        df_valid["monto_abono_banco"] = df_valid["neto_recibido"]
        df_valid["monto_abono_detraccion"] = df_valid["detraccion_monto"]

        # 5) Flags útiles para análisis
        df_valid["tiene_detraccion"] = df_valid["detraccion_monto"] > 0

        # Fechas de pago y ventana de tolerancia
        df_valid["fecha_limite_pago"] = df_valid["fecha_emision"] + df_valid["dias_pago"].apply(
            lambda d: timedelta(days=int(d) if pd.notna(d) else 0)
        )
        df_valid["fecha_inicio_ventana"] = df_valid["fecha_limite_pago"] - timedelta(days=self.days_tolerance)
        df_valid["fecha_fin_ventana"] = df_valid["fecha_limite_pago"] + timedelta(days=self.days_tolerance)

        if self.modo_debug:
            info("DEBUG Facturas — muestra rápida:")
            print(df_valid.head())

        ok("Cálculos financieros aplicados con éxito a facturas.")
        return df_valid

    # =======================================================
    #   BANCOS
    # =======================================================
    def process_bancos(self, df_bancos: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza y enriquece movimientos bancarios:
          - Fecha estándar → 'Fecha'
          - Monto numérico → 'Monto'
          - Moneda estándar → 'moneda'
          - Descripción → 'Descripcion'
          - Operación → 'Operacion'
          - Monto en PEN (conversión si es USD)
          - Rango de variación de monto (± monto_variacion)
          - Flag es_dolares
          - Clasificación IA opcional: tipo_operacion_ia, prob, justificación
        """
        info("Preparando movimientos bancarios…")

        df = self._normalize_columns(df_bancos)

        # -------------------------------
        #  Fecha
        # -------------------------------
        fecha_cols = [c for c in df.columns if c.lower() in ("fecha", "fecha_mov", "fechaoperacion", "fecha_operacion")]
        if fecha_cols:
            df["Fecha"] = pd.to_datetime(df[fecha_cols[0]], errors="coerce")
        else:
            warn("No se encontró columna de fecha clara en bancos. Se usará NaT.")
            df["Fecha"] = pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))

        # -------------------------------
        #  Monto
        # -------------------------------
        monto_cols = [c for c in df.columns if c.lower() in ("monto", "montototal", "importe", "importe_total")]
        if monto_cols:
            df["Monto"] = pd.to_numeric(df[monto_cols[0]], errors="coerce").fillna(0)
        else:
            warn("No se encontró columna de monto. Se creará 'Monto' = 0.")
            df["Monto"] = 0.0

        # -------------------------------
        #  Moneda
        # -------------------------------
        moneda_cols = [c for c in df.columns if c.lower() == "moneda"]
        if moneda_cols:
            df["moneda"] = df[moneda_cols[0]].astype(str).str.upper().str.strip()
        else:
            warn("No se encontró columna 'moneda'. Se asumirá 'PEN'.")
            df["moneda"] = "PEN"

        # -------------------------------
        #  Descripción
        # -------------------------------
        desc_cols = [
            c for c in df.columns
            if c.lower() in ("descripcion", "descripción", "glosa", "detalle", "concepto")
        ]
        if desc_cols:
            df["Descripcion"] = df[desc_cols[0]].astype(str)
        else:
            warn("No se encontró columna de descripción. 'Descripcion' quedará vacía.")
            df["Descripcion"] = ""

        # -------------------------------
        #  Operación
        # -------------------------------
        oper_cols = [c for c in df.columns if c.lower() in ("operacion", "nro_operacion", "referencia", "referencia1")]
        if oper_cols:
            df["Operacion"] = df[oper_cols[0]].astype(str)
        else:
            df["Operacion"] = ""

        # -------------------------------
        #  Conversión a PEN y variaciones
        # -------------------------------
        df["es_dolares"] = df["moneda"].str.contains("USD") | df["moneda"].str.contains("DOL")

        def _to_pen(row):
            if row.get("es_dolares", False):
                return round(float(row["Monto"]) * self.tipo_cambio_usd_pen, 2)
            return round(float(row["Monto"]), 2)

        df["Monto_PEN"] = df.apply(_to_pen, axis=1)

        df["monto_variacion_min"] = df["Monto_PEN"] - self.monto_variacion
        df["monto_variacion_max"] = df["Monto_PEN"] + self.monto_variacion

        # ====================================================
        #  CLASIFICACIÓN IA (OPCIONAL)
        # ====================================================
        if self.activar_ia:
            info("IA activada → clasificando descripciones bancarias…")

            def _classify_safe(desc: str):
                desc = (desc or "").strip()
                if not desc:
                    return {
                        "tipo": "otro",
                        "probabilidad": 0.0,
                        "justificacion": "Sin descripción.",
                    }
                try:
                    return ai_classify(desc)
                except Exception as e:
                    warn(f"Error IA clasificando movimiento: {e}")
                    return {
                        "tipo": "otro",
                        "probabilidad": 0.3,
                        "justificacion": "Error IA. Fallback local.",
                    }

            resultados = df["Descripcion"].apply(_classify_safe)

            df["tipo_operacion_ia"] = resultados.apply(lambda r: r.get("tipo"))
            df["tipo_operacion_ia_prob"] = resultados.apply(lambda r: r.get("probabilidad"))
            df["tipo_operacion_ia_justificacion"] = resultados.apply(lambda r: r.get("justificacion"))

        else:
            warn("IA desactivada en configuración. Se omiten clasificaciones de movimientos.")
            df["tipo_operacion_ia"] = "otro"
            df["tipo_operacion_ia_prob"] = 0.0
            df["tipo_operacion_ia_justificacion"] = "IA desactivada."

        if self.modo_debug:
            info("DEBUG Bancos — muestra rápida:")
            print(df.head())

        ok("Movimientos bancarios preparados correctamente.")
        return df


# ============================================================
#  TEST DIRECTO DEL MÓDULO
# ============================================================
if __name__ == "__main__":
    print("\n==============================================")
    print("🔵  PULSEFORGE · CALCULATOR ENGINE TEST")
    print("==============================================\n")

    calc = Calculator()

    # ------------------- FACTURAS DE PRUEBA -------------------
    df_facturas_demo = pd.DataFrame([
        {
            "subtotal": 100,
            "fecha_emision": "2024-01-01",
            "forma_pago": "Crédito 30 días",
        },
        {
            "subtotal": 200,
            "fecha_emision": "2024-02-15",
            "forma_pago": "CONTADO",
        },
    ])

    facturas_proc = calc.process_facturas(df_facturas_demo)
    print("\nFACTURAS PROCESADAS:")
    print(facturas_proc[[
        "subtotal",
        "igv",
        "total_con_igv",
        "detraccion_monto",
        "neto_recibido",
        "monto_abono_banco",
        "monto_abono_detraccion",
        "fecha_emision",
        "fecha_limite_pago",
        "fecha_inicio_ventana",
        "fecha_fin_ventana",
    ]])

    # ------------------- BANCOS DE PRUEBA ---------------------
    df_bancos_demo = pd.DataFrame([
        {
            "fecha": "2024-01-31",
            "monto": 118,
            "moneda": "PEN",
            "descripcion": "Abono por pago de factura F001-0001 Cliente X",
            "operacion": "123456",
        },
        {
            "fecha_mov": "2024-01-05",
            "montototal": 30,
            "moneda": "USD",
            "glosa": "Transfer interbancaria CCI Cliente Y",
            "nro_operacion": "789012",
        },
    ])

    bancos_proc = calc.process_bancos(df_bancos_demo)
    print("\nBANCOS PROCESADOS:")
    print(bancos_proc[[
        "Fecha",
        "Monto",
        "moneda",
        "Monto_PEN",
        "monto_variacion_min",
        "monto_variacion_max",
        "Descripcion",
        "Operacion",
    ]])

    ok("\nTEST COMPLETADO\n")
