import subprocess
import datetime
import sys
import os
import shutil

# ======================================================
#   CONFIG
# ======================================================
RUTA_BACKUP = r"C:\BACKUPS_JAREK\Backup-PulseForge"
RUTA_ESTRUCTURA = r"C:\Proyectos\PulseForge\estructura.txt"
CARPETAS_IGNORADAS = ["forge_env", "venv", ".venv", "env"]  # entornos virtuales

# ======================================================
# Ejecutar comandos con control elegante
# ======================================================
def run(cmd, msg_ok=None):
    result = subprocess.run(cmd, shell=True, text=True)
    if result.returncode != 0:
        print(f"\n❌  ERROR ejecutando: {cmd}")
        sys.exit(1)
    if msg_ok:
        print(f"   ✔️  {msg_ok}")

# ======================================================
# Crear backup completo del proyecto
# ======================================================
def hacer_backup():
    origen = os.path.abspath(os.getcwd())
    os.makedirs(RUTA_BACKUP, exist_ok=True)

    fecha = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    destino = os.path.join(RUTA_BACKUP, f"Backup_{fecha}")

    try:
        print("\n🗂️  Creando backup del proyecto…")
        shutil.copytree(origen, destino)
        print(f"   ✔️  Backup creado en:\n       {destino}")
    except Exception as e:
        print(f"❌  Error creando backup: {e}")
        sys.exit(1)

# ======================================================
# Crear estructura del proyecto en un archivo
# ======================================================
def escribir_estructura():
    print("\n📄  Generando estructura del proyecto…")

    root = os.path.abspath(os.getcwd())
    lines = ["📦 PulseForge\n"]

    for carpeta_raiz, subdirs, archivos in os.walk(root):
        # Transformar rutas a formato relativo
        rel = os.path.relpath(carpeta_raiz, root)

        # Saltar contenido de carpetas ignoradas
        if any(rel.split(os.sep)[0] == ign for ign in CARPETAS_IGNORADAS):
            continue

        indent = " ┃ " * (rel.count(os.sep))
        folder_name = os.path.basename(carpeta_raiz)

        # Mostrar carpetas ignoradas solo como nombre
        if folder_name in CARPETAS_IGNORADAS:
            lines.append(f"{indent}┗ 📂 {folder_name}  (contenido oculto)\n")
            subdirs[:] = []  # evita que baje dentro
            continue

        # Carpeta normal
        if rel != ".":
            lines.append(f"{indent}📂 {folder_name}\n")

        # Archivos
        for archivo in archivos:
            lines.append(f"{indent} ┣ 📜 {archivo}\n")

    with open(RUTA_ESTRUCTURA, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"   ✔️  Estructura guardada en:\n       {RUTA_ESTRUCTURA}")

# ======================================================
# PROCESO PRINCIPAL
# ======================================================
if __name__ == "__main__":
    mensaje = f"Auto-commit {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    print("\n====================================================")
    print(" 🔥  GIT PUSH + BACKUP + ESTRUCTURA – Fénix Engine v4 ")
    print("====================================================\n")

    print("📌  Inicializando proceso...\n")

    # 1) ADD
    print("📂  Añadiendo archivos…")
    run("git add .", "Archivos añadidos al stage")

    # 2) COMMIT
    print("\n📝  Creando commit…")
    run(f'git commit -m "{mensaje}"', "Commit creado")

    # 3) PUSH
    print("\n🚀  Subiendo cambios al repositorio remoto…")
    run("git push", "Push completado")

    # 4) BACKUP
    hacer_backup()

    # 5) ESTRUCTURA
    escribir_estructura()

    print("\n✨  Todo ok, Jarek. Repo actualizado + Backup asegurado + Estructura lista.\n")
