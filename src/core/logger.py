# src/core/logger.py
from __future__ import annotations

import sys
import os
from datetime import datetime
from pathlib import Path
import threading

# -------------------------
# Paleta de colores consola
# -------------------------
class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    RESET = "\033[0m"


# -------------------------
# Configuración de archivos
# -------------------------
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pulseforge.log"

_lock = threading.Lock()


# -------------------------
# Utilidades internas
# -------------------------
def _timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write(level: str, msg: str):
    with _lock:
        try:
            with LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(f"[{_timestamp()}] [{level}] {msg}\n")
        except Exception:
            pass


def _console(color: str, tag: str, msg: str) -> str:
    return f"{color}{tag}{Colors.RESET} {msg}"


# -------------------------
# Loggers principales
# -------------------------
def info(msg: str):
    print(_console(Colors.BLUE, "🔵 INFO", msg))
    _write("INFO", msg)


def ok(msg: str):
    print(_console(Colors.GREEN, "🟢 OK", msg))
    _write("OK", msg)


def warn(msg: str):
    print(_console(Colors.YELLOW, "🟡 WARN", msg))
    _write("WARN", msg)


def error(msg: str):
    print(_console(Colors.RED, "🔴 ERROR", msg), file=sys.stderr)
    _write("ERROR", msg)


# -------------------------
# Barras de progreso
# -------------------------
def start_progress(total: int, label: str = "PROCESO"):
    print(f"{Colors.CYAN}[{label}] Iniciando...{Colors.RESET}")


def update_progress(current: int, total: int, label: str = "PROCESO"):
    if total == 0:
        return

    pct = int((current / total) * 100)
    blk = pct // 5
    bar = "█" * blk + "░" * (20 - blk)

    line = f"{Colors.CYAN}[{label}] {bar} {pct}% ({current}/{total}){Colors.RESET}"
    sys.stdout.write("\r" + line)
    sys.stdout.flush()


def finish_progress(total: int, label: str = "PROCESO"):
    print(f"\r{Colors.GREEN}[{label}] COMPLETADO ✔ ({total} items){Colors.RESET}\n")
