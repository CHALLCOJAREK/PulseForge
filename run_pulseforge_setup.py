# run_pulseforge_setup.py
import subprocess
import sys
import os
from pathlib import Path

# =====================================================
#  FIX UNIVERSAL DE IMPORTS (FUNCIONA DESDE RAÍZ)
# =====================================================
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

# Agregar rutas al sys.path
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

print("🔧 Fix de rutas aplicado (src ahora es importable).")


# =====================================================
# Ejecutar scripts del proyecto en orden seguro
# =====================================================
def run(title, rel_path):
    file_path = ROOT / "src" / rel_path

    print("\n====================================================")
    print(f"🔵  Ejecutando: {title}")
    print(f"📌  Archivo: {file_path}")
    print("====================================================\n")

    if not file_path.exists():
        print(f"❌ ERROR: No se encontró el archivo → {file_path}")
        sys.exit(1)

    # FIX DEFINITIVO: subprocess con PYTHONPATH correcto
    env = {
        **os.environ,
        "PYTHONPATH": f"{str(ROOT)};{str(SRC)}"
    }

    result = subprocess.run([sys.executable, str(file_path)], env=env)

    if result.returncode != 0:
        print(f"\n❌ ERROR ejecutando {title}\n")
        sys.exit(1)

    print(f"\n🟢 {title} finalizado correctamente.\n")


# =====================================================
# ORDEN OFICIAL DE EJECUCIÓN (NO CAMBIAR)
# =====================================================
run("1) Construcción de Base pulseforge.sqlite", "loaders/newdb_builder.py")
run("2) Pipeline de Clientes", "pipelines/pipeline_clients.py")
run("3) Pipeline de Facturas", "pipelines/pipeline_facturas.py")
run("4) Pipeline de Bancos", "pipelines/pipeline_bancos.py")
run("5) Pipeline de Matching", "pipelines/pipeline_matcher.py")

print("\n====================================================")
print("🚀  PULSEFORGE COMPLETO — TODO EJECUTADO SIN ERRORES")
print("====================================================\n")
