# src/cli.py

import os
import sys
import argparse

# -----------------------------------------
# Garantizar que el proyecto raíz esté en el path
# (solo una vez, limpio, sin contaminación)
# -----------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# -----------------------------------------
# Pipelines
# -----------------------------------------
from src.pipelines.full_run import full_run
from src.pipelines.incremental import incremental_run

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


def main():
    parser = argparse.ArgumentParser(
        description="PulseForge CLI – Ejecuta procesos FULL o INCREMENTAL."
    )

    parser.add_argument(
        "mode",
        choices=["full", "inc"],
        help="Modo de ejecución: full (todo el pipeline) o inc (incremental)."
    )

    args = parser.parse_args()

    if args.mode == "full":
        info("Ejecutando PulseForge en modo FULL RUN...")
        full_run()
        ok("Proceso FULL terminado. ✔")

    elif args.mode == "inc":
        info("Ejecutando PulseForge en modo INCREMENTAL...")
        incremental_run()
        ok("Proceso INCREMENTAL terminado. ✔")

    else:
        error("Modo no reconocido.")


if __name__ == "__main__":
    main()
