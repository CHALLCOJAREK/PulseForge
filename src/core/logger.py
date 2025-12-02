# src/core/logger.py
from __future__ import annotations

import sys
import os
from datetime import datetime
from pathlib import Path
import threading


# ============================================================
#   PULSEFORGE LOG ENGINE — CORPORATE EDITION
# ============================================================

# --- Colores consola ---
class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"


# --- Configuración de carpeta de logs ---
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "pulseforge.log"

# Lock para escrituras seguras en multi-hilo
_log_lock = threading.Lock()


# ============================================================
#   FUNCIONES INTERNAS
# ============================================================
def _timestamp() -> str:
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def _write_file(level: str, message: str):
    """Escribe en pulseforge.log de forma segura."""
    with _log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{_timestamp()}] [{level}] {message}\n")


def _format_console(color: str, prefix: str, message: str) -> str:
    return f"{color}{prefix}{Colors.RESET} {message}"


# ============================================================
#   API PÚBLICA — DIRECTA, LIMPIA, ELEGANTE
# ============================================================
def info(msg: str):
    console_msg = _format_console(Colors.BLUE, "🔵 INFO", msg)
    print(console_msg)
    _write_file("INFO", msg)


def ok(msg: str):
    console_msg = _format_console(Colors.GREEN, "🟢 OK", msg)
    print(console_msg)
    _write_file("OK", msg)


def warn(msg: str):
    console_msg = _format_console(Colors.YELLOW, "🟡 WARN", msg)
    print(console_msg)
    _write_file("WARN", msg)


def error(msg: str):
    console_msg = _format_console(Colors.RED, "🔴 ERROR", msg)
    print(console_msg, file=sys.stderr)
    _write_file("ERROR", msg)


# ============================================================
#   TEST DIRECTO DEL MÓDULO
# ============================================================
if __name__ == "__main__":
    print("\n============================================")
    print("🔵  PULSEFORGE · LOG ENGINE TEST")
    print("============================================\n")

    info("Esto es un mensaje INFO de prueba.")
    ok("Todo salió correcto.")
    warn("Advertencia de ejemplo.")
    error("Error simulado para validar el logger.")

    print("\n🟢 TEST FINALIZADO — Revisa logs/pulseforge.log\n")
