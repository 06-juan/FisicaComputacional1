"""
Capa GOLD — Agregaciones con contratos, PK compuesta y transacciones
Lee desde Silver (columnas lat/lon normalizadas)

Tablas producidas:
  1. ranking_puntos.parquet         — score anual por punto (lat/lon)
  2. estacionalidad_mensual.parquet — score mensual REGIONAL (12 filas)
  3. resumen_ejecutivo.parquet      — CROSS JOIN ranking x estacionalidad (W03)
  4. artifacts/*.csv                — exports para consumo externo (W06B)
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

SILVER_PATH      = "data/silver/clima_cafe_silver.parquet"
GOLD_RANKING     = "data/gold/ranking_puntos.parquet"
GOLD_ESTACIONAL  = "data/gold/estacionalidad_mensual.parquet"
GOLD_RESUMEN     = "data/gold/resumen_ejecutivo.parquet"
GOLD_ESTAC_PARTS = "data/gold/estacionalidad_partitioned/"

LAT_MIN, LAT_MAX = 4.0, 6.0
LON_MIN, LON_MAX = -76.5, -74.5

TEMP_MIN_OPT, TEMP_MAX_OPT = 17.0, 23.0
RH_MIN_OPT,   RH_MAX_OPT   = 70.0, 85.0
RAIN_MIN_OPT               = 3.0
RAD_MIN_OPT                = 10.0


def _score_expr(prefix=""):
    p = f"{prefix}." if prefix else ""
    return f"""
        (CASE WHEN {p}temp_c  BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT} THEN 25 ELSE 0 END) +
        (CASE WHEN {p}rh_pct  BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}   THEN 25 ELSE 0 END) +
        (CASE WHEN {p}rain_mm >= {RAIN_MIN_OPT}                           THEN 25 ELSE 0 END) +
        (CASE WHEN {p}rad_mj_m2 >= {RAD_MIN_OPT}                          THEN 25 ELSE 0 END)
    """


def _verificar_silver(con) -> int:
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{SILVER_PATH}')").fetchone()[0]
    if n == 0:
        raise ValueError("Silver vacio — abortando Gold.")
    log.info("Silver verificado: %d filas.", n)
    return n


def _construir_ranking(con) -> int:
    Path("data/gold").mkdir(parents=True, exist_ok=True)
    score = _score_expr()

    con.execute(f"""
        CREATE OR REPLACE TABLE gold_ranking_staging AS
        SELECT
            CONCAT(CAST(ROUND(lat,6) AS VARCHAR), '|',
                   CAST(ROUND(lon,6) AS VARCHAR))   AS pk_gold,
            ROUND(lat, 6)                           AS lat,
            ROUND(lon, 6)                           AS lon,
            COUNT(*)                                AS dias_evaluados,
            SUM(CASE
                WHEN temp_c  BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT}
                 AND rh_pct  BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}
                 AND rain_mm >= {RAIN_MIN_OPT}
                 AND rad_mj_m2 >= {RAD_MIN_OPT}
                THEN 1 ELSE 0 END)                  AS dias_optimos,
            ROUND(
                SUM(CASE
                    WHEN temp_c  BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT}
                     AND rh_pct  BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}
                     AND rain_mm >= {RAIN_MIN_OPT}
                     AND rad_mj_m2 >= {RAD_MIN_OPT}
                    THEN 1.0 ELSE 0.0 END
                ) * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_optimos,
            ROUND(AVG({score}), 2)                  AS score_final,
            current_timestamp                       AS aggregated_at
        FROM read_parquet('{SILVER_PATH}')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        GROUP BY ROUND(lat,6), ROUND(lon,6)
    """)

    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT pk_gold, COUNT(*) c
            FROM gold_ranking_staging GROUP BY pk_gold HAVING c > 1
        )
    """).fetchone()[0]
    if dupes:
        raise ValueError(f"Gold ranking tiene {dupes} PK duplicadas.")

    n = con.execute("SELECT COUNT(*) FROM gold_ranking_staging").fetchone()[0]
    con.execute(f"COPY gold_ranking_staging TO '{GOLD_RANKING}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    log.info("Gold ranking escrito: %d puntos -> %s", n, GOLD_RANKING)
    return n


def _construir_estacionalidad(con) -> None:
    """
    Estacionalidad REGIONAL: agrega toda la region por mes.
    Produce 12 filas (una por mes) — sin lat/lon.
    PK: pk_estacional_mes (= mes, valores 1-12).
    """
    Path(GOLD_ESTAC_PARTS).mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        CREATE OR REPLACE TABLE gold_estac_staging AS
        SELECT
            mes                                                AS pk_estacional_mes,
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
                    WHEN (
                        (CASE WHEN temp_c   BETWEEN {TEMP_MIN_OPT} AND {TEMP_MAX_OPT} THEN 25 ELSE 0 END) +
                        (CASE WHEN rh_pct   BETWEEN {RH_MIN_OPT}   AND {RH_MAX_OPT}   THEN 25 ELSE 0 END) +
                        (CASE WHEN rain_mm  >= {RAIN_MIN_OPT}                          THEN 25 ELSE 0 END) +
                        (CASE WHEN rad_mj_m2 >= {RAD_MIN_OPT}                         THEN 25 ELSE 0 END)
                    ) >= 75 THEN 1.0 ELSE 0.0 END
                ) * 100.0 / NULLIF(COUNT(*), 0), 2
            )                                                  AS confort_pct,
            current_timestamp                                  AS aggregated_at
        FROM read_parquet('{SILVER_PATH}')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        GROUP BY mes
        ORDER BY confort_pct DESC
    """)

    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT pk_estacional_mes, COUNT(*) c
            FROM gold_estac_staging GROUP BY pk_estacional_mes HAVING c > 1
        )
    """).fetchone()[0]
    if dupes:
        raise ValueError(f"Gold estacionalidad tiene {dupes} PK duplicadas.")

    con.execute(f"COPY gold_estac_staging TO '{GOLD_ESTACIONAL}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.execute(f"""
        COPY gold_estac_staging TO '{GOLD_ESTAC_PARTS}'
        (FORMAT PARQUET, COMPRESSION ZSTD,
         PARTITION_BY (pk_estacional_mes), OVERWRITE_OR_IGNORE TRUE)
    """)
    log.info("Gold estacionalidad escrita: %s", GOLD_ESTACIONAL)


def _construir_resumen_ejecutivo(con) -> None:
    """
    W03 — CROSS JOIN seguro entre ranking y estacionalidad regional.

    Diseno actual:
      - gold_ranking_staging:  144 filas (1 por punto lat/lon), tiene lat/lon
      - gold_estac_staging:     12 filas (1 por mes, promedio regional), SIN lat/lon

    Por eso se usa CROSS JOIN (no JOIN con ON):
      - CROSS JOIN produce 144 x 12 = 1.728 filas — cardinalidad controlada
      - Un JOIN con ON lat=lat hubiera sido incorrecto: estac no tiene lat/lon
      - Un CROSS JOIN sobre tablas grandes SI explota; aqui es seguro porque
        estacionalidad tiene exactamente 12 filas por diseno

    Resultado: cada punto del ranking queda enriquecido con el perfil
    estacional regional de los 12 meses.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold_resumen_staging AS
        SELECT
            CONCAT(CAST(r.lat AS VARCHAR), '|',
                   CAST(r.lon AS VARCHAR), '|',
                   CAST(e.pk_estacional_mes AS VARCHAR)) AS pk_resumen,

            r.lat,
            r.lon,
            r.score_final,
            r.dias_evaluados,
            r.dias_optimos,
            r.pct_optimos,

            e.pk_estacional_mes                          AS mes,
            e.score_mensual_promedio,
            e.variabilidad_score,
            e.confort_pct                                AS confort_pct_mes,

            ROUND(e.score_mensual_promedio - r.score_final, 2) AS desviacion_estacional,

            current_timestamp AS aggregated_at

        FROM gold_ranking_staging r
        -- CROSS JOIN: estac tiene solo 12 filas (regional) — no tiene lat/lon
        -- Produce exactamente 144 x 12 = 1.728 filas
        CROSS JOIN gold_estac_staging e
        ORDER BY r.score_final DESC, e.pk_estacional_mes
    """)

    n         = con.execute("SELECT COUNT(*) FROM gold_resumen_staging").fetchone()[0]
    n_ranking = con.execute("SELECT COUNT(*) FROM gold_ranking_staging").fetchone()[0]
    esperado  = n_ranking * 12
    if n != esperado:
        log.warning("Cardinalidad del CROSS JOIN: esperado=%d, obtenido=%d", esperado, n)

    con.execute(f"COPY gold_resumen_staging TO '{GOLD_RESUMEN}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    log.info("Gold resumen ejecutivo escrito: %d filas (CROSS JOIN 144x12) -> %s", n, GOLD_RESUMEN)


def _exportar_a_csv(con) -> None:
    """W06B — Exports a CSV para consumo externo."""
    Path("artifacts").mkdir(parents=True, exist_ok=True)
    log.info("Exportando artefactos CSV...")

    con.execute("""
        COPY (SELECT * FROM gold_ranking_staging ORDER BY score_final DESC)
        TO 'artifacts/ranking_puntos_elite.csv' (FORMAT CSV, HEADER)
    """)
    con.execute("""
        COPY gold_estac_staging
        TO 'artifacts/estacionalidad_mensual.csv' (FORMAT CSV, HEADER)
    """)
    con.execute("""
        COPY (SELECT * FROM gold_resumen_staging ORDER BY score_final DESC, mes)
        TO 'artifacts/resumen_ejecutivo.csv' (FORMAT CSV, HEADER)
    """)
    log.info("Artefactos CSV escritos en artifacts/")


def procesar_gold() -> None:
    con = duckdb.connect()
    try:
        con.execute("BEGIN")
        _verificar_silver(con)
        n_rank = _construir_ranking(con)
        _construir_estacionalidad(con)
        _construir_resumen_ejecutivo(con)
        _exportar_a_csv(con)
        con.execute("COMMIT")
        log.info("Gold completado. Puntos ranking: %d", n_rank)

    except Exception as exc:
        con.execute("ROLLBACK")
        log.error("ROLLBACK Gold — %s", exc)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    procesar_gold()