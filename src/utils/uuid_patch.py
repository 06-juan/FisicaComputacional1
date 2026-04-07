"""
Parche UUID para bronze.parquet — Bronze layer
Agrega la columna row_id (UUID v4) como surrogate key al parquet existente.
Ejecutar UNA sola vez sobre el bronze.parquet ya generado.

Uso:
    python -m src.ingest.uuid_patch
"""

import duckdb
import logging
from pathlib import Path
from src.utils.dbconect import conectar_bd, desconectar_bd


logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

BRONZE_PATH = "data/bronze/bronze.parquet"
BRONZE_TMP  = "data/bronze/bronze_with_uuid.parquet"


def agregar_uuid_a_bronze() -> None:
    if not Path(BRONZE_PATH).exists():
        raise FileNotFoundError(f"No se encontró: {BRONZE_PATH}")

    con = conectar_bd()
    try:
        # Verificar si ya tiene row_id
        cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{BRONZE_PATH}') LIMIT 1"
        ).fetchall()]

        if "row_id" in cols:
            log.info("row_id ya existe en Bronze. Nada que hacer.")
            return

        n_antes = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{BRONZE_PATH}')"
        ).fetchone()[0]

        log.info("Inyectando row_id UUID en %d filas...", n_antes)

        # Crear parquet temporal con uuid() como primera columna
        con.execute(f"""
            COPY (
                SELECT
                    uuid()::VARCHAR  AS row_id,
                    *
                FROM read_parquet('{BRONZE_PATH}')
            ) TO '{BRONZE_TMP}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Verificar unicidad del UUID antes de reemplazar
        dupes = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT row_id, COUNT(*) c
                FROM read_parquet('{BRONZE_TMP}')
                GROUP BY row_id HAVING c > 1
            )
        """).fetchone()[0]

        if dupes > 0:
            Path(BRONZE_TMP).unlink()
            raise ValueError(f"UUID no unico: {dupes} duplicados. Abortando.")

        n_despues = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{BRONZE_TMP}')"
        ).fetchone()[0]

        if n_antes != n_despues:
            Path(BRONZE_TMP).unlink()
            raise ValueError(
                f"Conteo inconsistente: {n_antes} -> {n_despues}. Abortando."
            )

        # Reemplazar el original
        Path(BRONZE_PATH).unlink()
        Path(BRONZE_TMP).rename(BRONZE_PATH)

        log.info("✅ row_id inyectado. Bronze: %d filas con PK surrogate.", n_despues)

    except Exception as exc:
        if Path(BRONZE_TMP).exists():
            Path(BRONZE_TMP).unlink()
        log.error("❌ Fallo al inyectar UUID: %s", exc)
        raise
    finally:
        desconectar_bd(con)


if __name__ == "__main__":
    agregar_uuid_a_bronze()
