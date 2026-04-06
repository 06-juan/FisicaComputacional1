"""
Capa GOLD — Agregaciones con contratos, PK compuesta y transacciones
Lee desde Silver (que ya tiene columnas lat/lon normalizadas)
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

SILVER_PATH       = "data/silver/clima_cafe_silver.parquet"
GOLD_RANKING      = "data/gold/ranking_puntos.parquet"
GOLD_ESTACIONAL   = "data/gold/estacionalidad_mensual.parquet"
GOLD_ESTAC_PARTS  = "data/gold/estacionalidad_partitioned/"

LAT_MIN, LAT_MAX = 4.0, 6.0
LON_MIN, LON_MAX = -76.5, -74.5

TEMP_MIN_OPT, TEMP_MAX_OPT = 17.0, 23.0
RH_MIN_OPT,   RH_MAX_OPT   = 70.0, 85.0
RAIN_MIN_OPT               = 3.0
RAD_MIN_OPT                = 10.0


def _verificar_silver(con) -> int:
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{SILVER_PATH}')").fetchone()[0]
    if n == 0:
        raise ValueError("Silver vacío — abortando Gold.")
    log.info("Silver verificado: %d filas.", n)
    return n


def _construir_ranking(con) -> int:
    Path("data/gold").mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        CREATE OR REPLACE TABLE gold_ranking_staging AS
        SELECT
            CONCAT(CAST(ROUND(lat,6) AS VARCHAR), '|',
                   CAST(ROUND(lon,6) AS VARCHAR))         AS pk_gold,
            ROUND(lat, 6)                                 AS lat,
            ROUND(lon, 6)                                 AS lon,
            COUNT(*)                                      AS dias_evaluados,
            SUM(CASE
                WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT}
                 AND rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}
                 AND rain_mm  >= {RAIN_MIN_OPT}
                 AND rad_mj_m2 >= {RAD_MIN_OPT}
                THEN 1 ELSE 0 END)                        AS dias_optimos,
            ROUND(
                SUM(CASE
                    WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT}
                     AND rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}
                     AND rain_mm  >= {RAIN_MIN_OPT}
                     AND rad_mj_m2 >= {RAD_MIN_OPT}
                    THEN 1.0 ELSE 0.0 END
                ) * 100.0 / NULLIF(COUNT(*), 0), 2)       AS pct_optimos,
            ROUND(AVG(
                (CASE WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT} THEN 25 ELSE 0 END) +
                (CASE WHEN rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}   THEN 25 ELSE 0 END) +
                (CASE WHEN rain_mm  >= {RAIN_MIN_OPT}                           THEN 25 ELSE 0 END) +
                (CASE WHEN rad_mj_m2 >= {RAD_MIN_OPT}                           THEN 25 ELSE 0 END)
            ), 2)                                          AS score_final,
            current_timestamp                              AS aggregated_at
        FROM read_parquet('{SILVER_PATH}')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        GROUP BY ROUND(lat,6), ROUND(lon,6)
        ORDER BY score_final DESC
    """)

    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT pk_gold, COUNT(*) c
            FROM gold_ranking_staging
            GROUP BY pk_gold HAVING c > 1
        )
    """).fetchone()[0]
    if dupes:
        raise ValueError(f"Gold ranking tiene {dupes} PK duplicadas.")

    n = con.execute("SELECT COUNT(*) FROM gold_ranking_staging").fetchone()[0]
    con.execute(f"""
        COPY gold_ranking_staging TO '{GOLD_RANKING}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info("Gold ranking escrito: %d puntos → %s", n, GOLD_RANKING)
    return n


def _construir_estacionalidad(con) -> None:
    Path(GOLD_ESTAC_PARTS).mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        CREATE OR REPLACE TABLE gold_estac_staging AS
        SELECT
            CONCAT(CAST(mes AS VARCHAR), '|',
                   CAST(ROUND(lat,6) AS VARCHAR), '|',
                   CAST(ROUND(lon,6) AS VARCHAR))              AS pk_estacional,
            mes,
            ROUND(lat, 6)                                      AS lat,
            ROUND(lon, 6)                                      AS lon,
            ROUND(AVG(
                (CASE WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT} THEN 25 ELSE 0 END) +
                (CASE WHEN rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}   THEN 25 ELSE 0 END) +
                (CASE WHEN rain_mm  >= {RAIN_MIN_OPT}                           THEN 25 ELSE 0 END) +
                (CASE WHEN rad_mj_m2 >= {RAD_MIN_OPT}                           THEN 25 ELSE 0 END)
            ), 2)                                              AS score_mensual_promedio,
            ROUND(STDDEV(
                (CASE WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT} THEN 25 ELSE 0 END) +
                (CASE WHEN rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}   THEN 25 ELSE 0 END) +
                (CASE WHEN rain_mm  >= {RAIN_MIN_OPT}                           THEN 25 ELSE 0 END) +
                (CASE WHEN rad_mj_m2 >= {RAD_MIN_OPT}                           THEN 25 ELSE 0 END)
            ), 2)                                              AS variabilidad_score,
            ROUND(
                SUM(CASE
                    WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT}
                     AND rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}
                     AND rain_mm  >= {RAIN_MIN_OPT}
                     AND rad_mj_m2 >= {RAD_MIN_OPT}
                    THEN 1.0 ELSE 0.0 END
                ) * 100.0 / NULLIF(COUNT(*), 0), 2)            AS confort_pct,
            current_timestamp                                  AS aggregated_at
        FROM read_parquet('{SILVER_PATH}')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        GROUP BY mes, ROUND(lat,6), ROUND(lon,6)
        ORDER BY confort_pct DESC
    """)

    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT pk_estacional, COUNT(*) c
            FROM gold_estac_staging
            GROUP BY pk_estacional HAVING c > 1
        )
    """).fetchone()[0]
    if dupes:
        raise ValueError(f"Gold estacionalidad tiene {dupes} PK duplicadas.")

    con.execute(f"""
        COPY gold_estac_staging TO '{GOLD_ESTACIONAL}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.execute(f"""
        COPY gold_estac_staging TO '{GOLD_ESTAC_PARTS}'
        (FORMAT PARQUET, COMPRESSION ZSTD,
         PARTITION_BY (mes), OVERWRITE_OR_IGNORE TRUE)
    """)
    log.info("Gold estacionalidad escrita: %s", GOLD_ESTACIONAL)

def _exportar_a_csv(con):
    # Creamos la carpeta de artefactos si no existe
    Path("artifacts").mkdir(parents=True, exist_ok=True)
    
    log.info("🚀 Exportando artefactos a CSV...")
    
    # Exportamos el ranking ordenado por score (así los 5 mejores quedan arriba)
    con.execute(f"""
        COPY (SELECT * FROM gold_ranking_staging) 
        TO 'artifacts/ranking_puntos_elite.csv' 
        (FORMAT CSV, HEADER)
    """)
    
    # Exportamos la estacionalidad
    con.execute(f"""
        COPY gold_estac_staging 
        TO 'artifacts/estacionalidad_mensual.csv' 
        (FORMAT CSV, HEADER)
    """)

def procesar_gold() -> None:
    con = duckdb.connect()
    try:
        con.execute("BEGIN")
        _verificar_silver(con)
        n_rank = _construir_ranking(con)
        _construir_estacionalidad(con)
        _exportar_a_csv(con)
        con.execute("COMMIT")
        log.info("✅ Gold completado. Puntos ranking: %d", n_rank)

    except Exception as exc:
        con.execute("ROLLBACK")
        log.error("❌ ROLLBACK Gold — %s", exc)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    procesar_gold()