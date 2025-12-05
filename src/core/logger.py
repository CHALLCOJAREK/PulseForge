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
    CYAN = "\033[96m"
    RESET = "\033[0m"


# --- Carpeta logs ---
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pulseforge.log"

_log_lock = threading.Lock()


# ============================================================
#   FUNCIONES INTERNAS
# ============================================================
def _timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_file(level: str, message: str):
    with _log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{_timestamp()}] [{level}] {message}\n")


def _format_console(color: str, prefix: str, message: str) -> str:
    return f"{color}{prefix}{Colors.RESET} {message}"


# ============================================================
#   LOGS TRADICIONALES
# ============================================================
def info(msg: str):
    print(_format_console(Colors.BLUE, "🔵 INFO", msg))
    _write_file("INFO", msg)


def ok(msg: str):
    print(_format_console(Colors.GREEN, "🟢 OK", msg))
    _write_file("OK", msg)


def warn(msg: str):
    print(_format_console(Colors.YELLOW, "🟡 WARN", msg))
    _write_file("WARN", msg)


def error(msg: str):
    print(_format_console(Colors.RED, "🔴 ERROR", msg), file=sys.stderr)
    _write_file("ERROR", msg)


# ============================================================
#   SISTEMA DE BARRA DE PROGRESO (MATCHING, ETL, ETC)
# ============================================================

_last_progress = ""

def start_progress(total: int, label: str = "PROCESO"):
    """Inicia una barra de progreso"""
    global _last_progress
    _last_progress = ""
    print(f"{Colors.CYAN}[{label}] Iniciando…{Colors.RESET}")


def update_progress(current: int, total: int, label: str = "PROCESO"):
    """Actualiza una barra de progreso dinámica en una sola línea"""
    if total == 0:
        return

    percent = int((current / total) * 100)
    filled = int(percent / 5)  # cada bloque es 5%
    bar = "█" * filled + "░" * (20 - filled)

    line = f"{Colors.CYAN}[{label}] {bar}  {percent}%  ({current}/{total}){Colors.RESET}"

    # sobrescribe la línea anterior
    sys.stdout.write("\r" + line)
    sys.stdout.flush()


def finish_progress(total: int, label: str = "PROCESO"):
    """Marca el final de la barra"""
    print(f"\r{Colors.GREEN}[{label}] COMPLETADO  ✔ ({total} items){Colors.RESET}\n")
