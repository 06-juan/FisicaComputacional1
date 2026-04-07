"""
Capa SILVER — Transformación con contratos, particionado y transacciones
Nombres reales del parquet Bronze: latitud, longitud (no lat/lon)
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

BRONZE_PATH  = "data/bronze/bronze.parquet"
SILVER_PATH  = "data/silver/clima_cafe_silver.parquet"
SILVER_PARTS = "data/silver/partitioned/"


def _verificar_bronze(con) -> int:
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{BRONZE_PATH}')").fetchone()[0]
    if n == 0:
        raise ValueError("Bronze vacío — abortando Silver.")
    log.info("Bronze verificado: %d filas.", n)
    return n


def _detectar_columnas(con) -> dict:
    """
    Lee el schema real del parquet y devuelve el mapeo de nombres.
    Soporta lat/lon y latitud/longitud.
    """
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{BRONZE_PATH}') LIMIT 1"
    ).fetchall()]
    log.info("Columnas detectadas en Bronze: %s", cols)

    lat_col = "latitud" if "latitud" in cols else "lat"
    lon_col = "longitud" if "longitud" in cols else "lon"

    # detectar nombre de fecha
    fecha_col = next((c for c in cols if "fecha" in c.lower() or "date" in c.lower()), "fecha")

    return {"lat": lat_col, "lon": lon_col, "fecha": fecha_col}

def _transformar_a_staging(con) -> int:
    """
    Crea las tablas temporales y devuelve el conteo para validación
    SIN escribir archivos todavía.
    """
    m = _detectar_columnas(con)
    lat, lon, fecha = m["lat"], m["lon"], m["fecha"]

    # Paso 1 — aliases con prefijo _ para evitar BinderException de DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE _silver_base AS
        SELECT
            row_id AS bronze_id,
            CAST({fecha}   AS DATE)                         AS _fecha,
            YEAR(CAST({fecha} AS DATE))                     AS _anio,
            MONTH(CAST({fecha} AS DATE))                    AS _mes,
            ROUND({lat},  6)                                AS _lat,
            ROUND({lon},  6)                                AS _lon,
            ROUND(temp_k - 273.15, 2)                       AS _temp_c,
            ROUND(
                100 * EXP(17.625 * (dew_k - 273.15) /
                          (243.04 + (dew_k - 273.15))) /
                      EXP(17.625 * (temp_k - 273.15) /
                          (243.04 + (temp_k - 273.15))),
            2)                                              AS _rh_pct,
            ROUND(lluvia_m   * 1000, 4)                     AS _rain_mm,
            ROUND(rad_j_m2   / 1e6,  6)                     AS _rad_mj_m2,
            ROUND(evap_pot_m * 1000, 4)                     AS _evap_pot_mm
        FROM read_parquet('{BRONZE_PATH}')
        WHERE temp_k   BETWEEN 200 AND 340
          AND dew_k    BETWEEN 200 AND 340
          AND lluvia_m  >= 0
          AND rad_j_m2  >= 0
    """)

    # Paso 2 — construir pk y renombrar al esquema final
    con.execute("""
        CREATE OR REPLACE TABLE silver_staging AS
        SELECT
            bronze_id,
            CONCAT(CAST(_fecha AS VARCHAR), '|',
                   CAST(_lat   AS VARCHAR), '|',
                   CAST(_lon   AS VARCHAR))  AS pk_silver,
            _fecha       AS fecha,
            _anio        AS anio,
            _mes         AS mes,
            _lat         AS lat,
            _lon         AS lon,
            _temp_c      AS temp_c,
            _rh_pct      AS rh_pct,
            _rain_mm     AS rain_mm,
            _rad_mj_m2   AS rad_mj_m2,
            _evap_pot_mm AS evap_pot_mm,
            current_timestamp AS processed_at
        FROM _silver_base
    """)

    # Validación de duplicados (falla rápido)
    dupes = con.execute("SELECT COUNT(*) FROM (SELECT pk_silver FROM silver_staging GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0]
    if dupes > 0:
        raise ValueError(f"Detección de {dupes} PKs duplicadas en staging.")

    # Retornamos el conteo de la TABLA, no del archivo en memoria
    return con.execute("SELECT COUNT(*) FROM silver_staging").fetchone()[0]

def procesar_silver() -> None:
    con = duckdb.connect()
    try:
        con.execute("BEGIN")
        
        n_bronze = _verificar_bronze(con)
        n_silver = _transformar_a_staging(con)

        # Validación de pérdida
        perdida = abs(n_bronze - n_silver) / n_bronze * 100
        if perdida > 2.0:
            raise ValueError(f"Pérdida excesiva: {perdida:.2f}% (Umbral 2%)")

        # SI PASÓ LAS PRUEBAS: Escribimos físicamente
        Path("data/silver").mkdir(parents=True, exist_ok=True)
        
        # Archivo completo
        con.execute(f"COPY silver_staging TO '{SILVER_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        # Particiones
        con.execute(f"COPY silver_staging TO '{SILVER_PARTS}' (FORMAT PARQUET, PARTITION_BY (anio, mes), OVERWRITE_OR_IGNORE TRUE)")

        con.execute("COMMIT")
        log.info("✅ Transacción exitosa. Disco actualizado. Filas: %d", n_silver)

    except Exception as exc:
        con.execute("ROLLBACK")
        log.error("❌ ROLLBACK ejecutado — Los archivos en disco NO fueron alterados. Error: %s", exc)
        raise
    finally:
        con.close()

if __name__ == "__main__":
    procesar_silver()