# src/core/logger.py
from __future__ import annotations

import sys
import os
from datetime import datetime
from pathlib import Path
import threading

# =====================================================
#  CONFIGURACIÓN DESDE settings.json Y .env (si existen)
# =====================================================
# Para evitar dependencias circulares, solo cargamos
# env variables directamente y settings.json manualmente.

ROOT = Path(__file__).resolve().parents[2]
SETTINGS_PATH = ROOT / "config" / "settings.json"

# Defaults (si no hay settings/env)
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE = ROOT / "logs" / "pulseforge.log"
DEFAULT_CONSOLE = True

# Intenta cargar settings.json
def _load_settings():
    if not SETTINGS_PATH.exists():
        return {}

    try:
        import json
        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

_settings = _load_settings()
_log_cfg = _settings.get("logging", {})

# Resolución jerárquica:
# env > settings.json > defaults
LOG_LEVEL = os.environ.get("LOG_LEVEL", _log_cfg.get("level", DEFAULT_LOG_LEVEL)).upper()
CONSOLE_ENABLED = os.environ.get("LOG_CONSOLE", str(_log_cfg.get("console", DEFAULT_CONSOLE))).lower() in ("1", "true", "yes", "y", "on")

# Path del logfile
log_file_from_settings = _log_cfg.get("file", None)
if log_file_from_settings:
    LOG_FILE = ROOT / log_file_from_settings.replace("./", "")
else:
    LOG_FILE = DEFAULT_LOG_FILE

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


# =====================================================
#  PALETA DE COLORES
# =====================================================
class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    RESET = "\033[0m"


# =====================================================
#  THREAD-LOCK
# =====================================================
_lock = threading.Lock()


# =====================================================
#  UTILIDADES INTERNAS
# =====================================================
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


# =====================================================
#  CONTROL DE NIVEL
# =====================================================
LEVEL_PRIORITY = {
    "ERROR": 40,
    "WARN": 30,
    "INFO": 20,
    "OK": 15,
    "DEBUG": 10
}

CURRENT_LEVEL = LEVEL_PRIORITY.get(LOG_LEVEL, 20)


def _allow(level: str) -> bool:
    return LEVEL_PRIORITY[level] >= CURRENT_LEVEL


# =====================================================
#  LOGGERS PÚBLICOS
# =====================================================
def info(msg: str):
    if _allow("INFO"):
        if CONSOLE_ENABLED:
            print(_console(Colors.BLUE, "🔵 INFO", msg))
        _write("INFO", msg)


def ok(msg: str):
    if _allow("OK"):
        if CONSOLE_ENABLED:
            print(_console(Colors.GREEN, "🟢 OK", msg))
        _write("OK", msg)


def warn(msg: str):
    if _allow("WARN"):
        if CONSOLE_ENABLED:
            print(_console(Colors.YELLOW, "🟡 WARN", msg))
        _write("WARN", msg)


def error(msg: str):
    if _allow("ERROR"):
        if CONSOLE_ENABLED:
            print(_console(Colors.RED, "🔴 ERROR", msg), file=sys.stderr)
        _write("ERROR", msg)


# =====================================================
#  BARRAS DE PROGRESO
# =====================================================
def start_progress(total: int, label: str = "PROCESO"):
    if CONSOLE_ENABLED:
        print(f"{Colors.CYAN}[{label}] Iniciando...{Colors.RESET}")


def update_progress(current: int, total: int, label: str = "PROCESO"):
    if not CONSOLE_ENABLED:
        return

    if total == 0:
        return

    pct = int((current / total) * 100)
    blk = pct // 5
    bar = "█" * blk + "░" * (20 - blk)

    line = f"{Colors.CYAN}[{label}] {bar} {pct}% ({current}/{total}){Colors.RESET}"
    sys.stdout.write("\r" + line)
    sys.stdout.flush()


def finish_progress(total: int, label: str = "PROCESO"):
    if CONSOLE_ENABLED:
        print(f"\r{Colors.GREEN}[{label}] COMPLETADO ✔ ({total} items){Colors.RESET}\n")
