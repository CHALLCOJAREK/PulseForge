import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.core.env_loader import get_env

# Estilos de mensaje
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class DatabaseManager:
    """
    Administra conexiones a:
    - BD origen (DataPulse)
    - BD destino (PulseForge)
    Crea la BD destino si no existe.
    """

    def __init__(self):
        info("Inicializando gestor de bases de datos...")

        self.env = get_env()

        # Conexión BD origen
        self.engine_origen = None

        # Conexión BD nueva
        self.engine_destino = None

        self._connect_origen()
        self._connect_destino()

        ok("Conexiones a bases de datos listas. PulseForge puede trabajar. 🚀")


    # ==============================================================
    #   CONEXIÓN A BD ORIGEN (DataPulse)
    # ==============================================================

    def _connect_origen(self):
        """Conecta a la base de datos de DataPulse (origen)."""

        db_path = self.env.DB_PATH_ORIGEN

        info(f"Conectando a BD origen (DataPulse): {db_path}")

        if not os.path.exists(db_path):
            error("La base de datos origen NO existe. Verifica la ruta en .env")
            raise FileNotFoundError(f"No se encuentra la BD origen en: {db_path}")

        try:
            self.engine_origen = create_engine(f"sqlite:///{db_path}")
            ok("Conexión con BD origen exitosa.")
        except SQLAlchemyError as e:
            error(f"Error conectando a BD origen: {e}")
            raise


    # ==============================================================
    #   CONEXIÓN A BD DESTINO (PulseForge)
    # ==============================================================

    def _connect_destino(self):
        """Conecta o crea la nueva base de datos destino."""

        db_path_new = self.env.DB_PATH_NUEVA

        info(f"Preparando conexión a BD destino (PulseForge): {db_path_new}")

        # Crear directorio si no existe
        dir_path = os.path.dirname(db_path_new)
        if not os.path.exists(dir_path):
            warn(f"Carpeta '{dir_path}' no existe. Creándola...")
            os.makedirs(dir_path)
            ok("Carpeta creada.")

        # Si la BD no existe → crear archivo vacío
        if not os.path.exists(db_path_new):
            warn("BD destino no existe. Creándola automáticamente...")
            open(db_path_new, 'a').close()
            ok("BD destino creada correctamente.")

        try:
            self.engine_destino = create_engine(f"sqlite:///{db_path_new}")
            ok("Conexión con BD destino exitosa.")
        except SQLAlchemyError as e:
            error(f"Error conectando a BD destino: {e}")
            raise



# ==============================================================
#   SINGLETON GLOBAL
# ==============================================================

_db_instance = None

def get_db():
    """Retorna una sola instancia global del DatabaseManager."""
    global _db_instance
    if _db_instance is None:
        info("Cargando gestor global de BD...")
        _db_instance = DatabaseManager()
    return _db_instance
