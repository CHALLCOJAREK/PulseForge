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
    # Insertar match individual
    # --------------------------------------------------------
    def insert_match(self, row: Dict[str, Any]) -> Optional[int]:
        """
        Inserta un match en match_pf.
        Retorna el ID generado.
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

        # Calcular porcentaje_match cuando no llega
        monto_fac = row.get("total_con_igv") or row.get("subtotal") or 0
        monto_banco = row.get("monto_banco_equivalente") or row.get("monto_banco") or 0

        try:
            monto_fac = float(monto_fac)
            monto_banco = float(monto_banco)
        except:
            monto_fac = 0
            monto_banco = 0

        diferencia = abs(monto_fac - monto_banco)

        if monto_fac > 0:
            porcentaje = round(1 - (diferencia / monto_fac), 4)
        else:
            porcentaje = 0

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
            self.db.insert("match_pf", data)
            ok(f"Match registrado → factura_hash={factura_hash}")
        except DatabaseError as e:
            error(f"Error insertando match: {e}")
            return None

        # Obtener ID generado
        cur = self.db.execute("SELECT last_insert_rowid()")
        match_id = cur.fetchone()[0]
        return match_id

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
            # Detectar si me dieron un hash o un id numérico
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
    # Guardar lista completa de matches
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


# ============================================================
#  PRUEBA RÁPIDA SOLO SI SE EJECUTA DIRECTO
# ============================================================
if __name__ == "__main__":
    mw = MatchWriter()
    ejemplo = {
        "factura_id": "ABC123",
        "movimiento_id": "XYZ789",
        "subtotal": 100,
        "total_con_igv": 118,
        "monto_banco": 118,
        "match_tipo": "MATCH"
    }
    mw.save_many([ejemplo])
    ok("Test local de MatchWriter completado.")
