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

# Carpetas cuyos CONTENIDOS NO deben mostrarse
CARPETAS_IGNORADAS = ["forge_env", "venv", ".venv", "env"]

# Carpetas especiales con profundidad limitada (solo nivel 1–2)
CARPETAS_NIVEL_LIMITADO = [".git"]


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
# Crear backup completo con progreso REAL %
# ======================================================
def hacer_backup():
    origen = os.path.abspath(os.getcwd())
    os.makedirs(RUTA_BACKUP, exist_ok=True)

    fecha = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    destino = os.path.join(RUTA_BACKUP, f"Backup_{fecha}")

    print("\n🗂️  Creando backup del proyecto…\n")

    # Contar archivos
    total_archivos = 0
    for ruta_actual, subdirs, files in os.walk(origen):
        if any(ign in ruta_actual for ign in CARPETAS_IGNORADAS):
            continue
        total_archivos += len(files)

    if total_archivos == 0:
        print("❌ No hay archivos para copiar.")
        return

    os.makedirs(destino, exist_ok=True)

    archivos_copiados = 0
    for ruta_actual, subdirs, files in os.walk(origen):

        if any(ign in ruta_actual for ign in CARPETAS_IGNORADAS):
            continue

        rel_path = os.path.relpath(ruta_actual, origen)
        destino_carpeta = os.path.join(destino, rel_path)
        os.makedirs(destino_carpeta, exist_ok=True)

        for file in files:
            origen_file = os.path.join(ruta_actual, file)
            destino_file = os.path.join(destino_carpeta, file)

            try:
                shutil.copy2(origen_file, destino_file)
                archivos_copiados += 1
                porcentaje = (archivos_copiados / total_archivos) * 100

                sys.stdout.write(f"\r📦 Copiando archivos… {porcentaje:6.2f}%")
                sys.stdout.flush()

            except Exception as e:
                print(f"\n❌ Error copiando {origen_file}: {e}")

    print(f"\n\n   ✔️  Backup creado en:\n       {destino}")


# ======================================================
# Generar estructura del proyecto
# ======================================================
def escribir_estructura():
    print("\n📄  Generando estructura del proyecto…")

    root = os.path.abspath(os.getcwd())
    lines = ["📦 PulseForge\n"]

    for carpeta_raiz, subdirs, files in os.walk(root):
        rel = os.path.relpath(carpeta_raiz, root)

        if any(rel.split(os.sep)[0] == ign for ign in CARPETAS_IGNORADAS):
            continue

        partes = rel.split(os.sep)
        carpeta_top = partes[0]

        if carpeta_top in CARPETAS_NIVEL_LIMITADO:
            nivel = len(partes)

            if nivel == 1:
                lines.append(f"📂 {carpeta_top}  (contenido limitado)\n")
            elif nivel == 2:
                indent = " ┃ "
                lines.append(f"{indent}📂 {partes[1]}\n")

            continue

        if rel != ".":
            indent = " ┃ " * (rel.count(os.sep))
            folder_name = os.path.basename(carpeta_raiz)
            lines.append(f"{indent}📂 {folder_name}\n")

        for archivo in files:
            indent = " ┃ " * (rel.count(os.sep))
            lines.append(f"{indent} ┣ 📜 {archivo}\n")

    with open(RUTA_ESTRUCTURA, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"   ✔️  Estructura guardada en:\n       {RUTA_ESTRUCTURA}")


# ======================================================
# PROCESO PRINCIPAL — ORDEN CORRECTO
# 1) Estructura
# 2) Git push
# 3) Backup
# ======================================================
if __name__ == "__main__":
    mensaje = f"Auto-commit {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    print("\n====================================================")
    print(" 🔥  GIT PUSH + BACKUP + ESTRUCTURA – Fénix Engine v5 ")
    print("====================================================\n")

    print("📌  Inicializando proceso...\n")

    # 1) ESTRUCTURA
    escribir_estructura()

    # 2) GIT
    print("\n📂  Añadiendo archivos…")
    run("git add .", "Archivos añadidos al stage")

    print("\n📝  Creando commit…")
    run(f'git commit -m "{mensaje}"', "Commit creado")

    print("\n🚀  Subiendo cambios al repositorio remoto…")
    run("git push", "Push completado")

    # 3) BACKUP
    hacer_backup()

    print("\n✨  Todo ok, Jarek. Estructura primero + Git al día + Backup asegurado.\n")
