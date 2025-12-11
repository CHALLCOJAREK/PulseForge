# src/loaders/match_writer.py
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Corporativo
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.db import PulseForgeDB, DatabaseError


# ============================================================
#   MATCH WRITER — Manejador de escritura de coincidencias
# ============================================================
class MatchWriter:

    def __init__(self):
        info("Inicializando MatchWriter…")
        self.db = PulseForgeDB()   # conexión corporativa
        ok("MatchWriter listo.")

    # --------------------------------------------------------
    # Insertar match individual (CORREGIDO)
    # --------------------------------------------------------
    def insert_match(self, row: Dict[str, Any]) -> Optional[int]:
        """
        Inserta un match en match_pf.
        Retorna el ID generado (rowid), 100% seguro y sin cursor cerrado.
        """
        if not row:
            warn("insert_match() recibió un dict vacío.")
            return None

        # Normalizar campos requeridos
        factura_hash = row.get("factura_id") or row.get("factura_hash")
        banco_hash = row.get("movimiento_id") or row.get("banco_hash")

        if not factura_hash:
            warn("Match sin factura_hash → ignorado.")
            return None

        # Calcular porcentaje_match
        monto_fac = float(row.get("total_con_igv") or row.get("subtotal") or 0)
        monto_banco = float(row.get("monto_banco_equivalente") or row.get("monto_banco") or 0)
        diferencia = abs(monto_fac - monto_banco)

        porcentaje = round(1 - (diferencia / monto_fac), 4) if monto_fac > 0 else 0

        data = {
            "factura_hash": str(factura_hash),
            "banco_hash": str(banco_hash),
            "monto_factura": monto_fac,
            "monto_banco": monto_banco,
            "diferencia": diferencia,
            "porcentaje_match": porcentaje,
            "estado": row.get("match_tipo") or "NO_MATCH",
            "fecha_match": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        try:
            # -----------------------------------------
            # INSERT seguro con lastrowid (SIN cursor externo)
            # -----------------------------------------
            conn = self.db.connect()
            cur = conn.cursor()

            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            query = f"INSERT INTO match_pf ({cols}) VALUES ({placeholders})"

            cur.execute(query, list(data.values()))
            conn.commit()

            match_id = cur.lastrowid   # ⭐ SEGURO, ESTÁNDAR, SIN CERRARSE

            ok(f"Match registrado → factura_hash={factura_hash} (id={match_id})")
            return match_id

        except Exception as e:
            error(f"Error insertando match: {e}")
            return None

    # --------------------------------------------------------
    # Actualizar factura con match_id
    # --------------------------------------------------------
    def update_factura_match_id(self, factura_id_or_hash: Any, match_id: int):
        """
        Actualiza facturas_pf.match_id usando facture_hash o id.
        """
        if not match_id:
            warn("update_factura_match_id() recibió match_id vacío.")
            return

        try:
            if isinstance(factura_id_or_hash, int):
                where = "id = ?"
                params = (factura_id_or_hash,)
            else:
                where = "source_hash = ?"
                params = (str(factura_id_or_hash),)

            self.db.update(
                "facturas_pf",
                {"match_id": match_id},
                where,
                params
            )
            ok(f"Factura actualizada con match_id={match_id}")

        except DatabaseError as e:
            error(f"Error actualizando factura con match_id: {e}")

    # --------------------------------------------------------
    # Guardar lista completa de matches (OPTIMIZADO)
    # --------------------------------------------------------
    def save_many(self, rows: List[Dict[str, Any]]):
        """
        Guarda múltiples filas de match y actualiza facturas.
        """
        if not rows:
            warn("save_many() → Lista de matches vacía.")
            return

        info(f"Guardando {len(rows)} matches…")

        for row in rows:
            match_id = self.insert_match(row)

            if match_id:
                factura_hash = row.get("factura_id") or row.get("factura_hash")
                self.update_factura_match_id(factura_hash, match_id)

        ok("Matches guardados correctamente.")

