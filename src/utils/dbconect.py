import duckdb
import logging

log = logging.getLogger(__name__)

def conectar_bd(db_path: str = ":memory:"):
    """
    Crea y devuelve una instancia de conexión.
    Por defecto es en memoria, pero acepta una ruta de archivo.
    """
    try:
        return duckdb.connect(db_path)
    except Exception as e:
        log.error(f"Error al conectar a {db_path}: {e}")
        return None

def desconectar_bd(conexion):
    """Cierra una conexión específica sin usar variables globales."""
    if conexion:
        try:
            conexion.close()
        except Exception as e:
            log.error(f"Error al cerrar la conexión: {e}")