"""
W05 — Performance + EXPLAIN ANALYZE
Ejecuta EXPLAIN ANALYZE sobre las queries principales de Silver y Gold.
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)

BRONZE_PATH = "data/raw/raw.parquet"
SILVER_PATH = "data/silver/clima_cafe_silver.parquet"

TEMP_MIN, TEMP_MAX = 17.0, 23.0
RH_MIN,   RH_MAX   = 70.0, 85.0
RAIN_MIN            = 3.0
RAD_MIN             = 10.0
LAT_MIN, LAT_MAX    = 4.0, 6.0
LON_MIN, LON_MAX    = -76.5, -74.5


def _explain_query(con, nombre: str, sql: str) -> None:
    print(f"\n{'─'*55}")
    print(f"  EXPLAIN ANALYZE → {nombre}")
    print(f"{'─'*55}")
    try:
        rows = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        for r in rows:
            print(r[-1] if r else "")
    except Exception as e:
        log.warning("No se pudo obtener EXPLAIN para '%s': %s", nombre, e)
    print(f"{'─'*55}\n")


def ejecutar_explain(capa: str) -> None:
    """capa: 'silver' | 'gold'"""
    con = duckdb.connect()

    if capa == "silver":
        if not Path(BRONZE_PATH).exists():
            log.warning("Bronze no encontrado, saltando EXPLAIN silver.")
            con.close()
            return
        _explain_query(con, "Silver — transformacion fisica", f"""
            SELECT
                ROUND(temp_k - 273.15, 2) AS temp_c,
                ROUND(lluvia_m * 1000, 4) AS rain_mm,
                ROUND(rad_j_m2 / 1e6,  6) AS rad_mj_m2,
                YEAR(CAST(fecha AS DATE))  AS anio,
                MONTH(CAST(fecha AS DATE)) AS mes
            FROM read_parquet('{BRONZE_PATH}')
            WHERE temp_k BETWEEN 200 AND 340
              AND dew_k  BETWEEN 200 AND 340
              AND lluvia_m >= 0
              AND rad_j_m2 >= 0
            LIMIT 10
        """)

    elif capa == "gold":
        if not Path(SILVER_PATH).exists():
            log.warning("Silver no encontrado, saltando EXPLAIN gold.")
            con.close()
            return

        # W05 — EXPLAIN del ranking
        _explain_query(con, "Gold — ranking de confort por punto", f"""
            SELECT
                ROUND(lat, 6) AS lat,
                ROUND(lon, 6) AS lon,
                COUNT(*) AS dias_evaluados,
                SUM(CASE
                    WHEN temp_c  BETWEEN {TEMP_MIN} AND {TEMP_MAX}
                     AND rh_pct  BETWEEN {RH_MIN}   AND {RH_MAX}
                     AND rain_mm >= {RAIN_MIN}
                     AND rad_mj_m2 >= {RAD_MIN}
                    THEN 1 ELSE 0 END) AS dias_optimos
            FROM read_parquet('{SILVER_PATH}')
            WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
              AND lon BETWEEN {LON_MIN} AND {LON_MAX}
            GROUP BY ROUND(lat,6), ROUND(lon,6)
        """)

        # W03 — EXPLAIN del JOIN entre ranking y estacionalidad
        # Este JOIN es seguro: usa PK compuesta lat|lon como clave de union
        # Un JOIN sin clave o con columnas no indexadas puede explotar en memoria
        _explain_query(con, "Gold — JOIN ranking x estacionalidad (W03: JOIN seguro con PK)", f"""
            WITH ranking AS (
                SELECT
                    ROUND(lat,6) AS lat,
                    ROUND(lon,6) AS lon,
                    COUNT(*) AS dias_evaluados,
                    ROUND(AVG(
                        (CASE WHEN temp_c  BETWEEN {TEMP_MIN} AND {TEMP_MAX} THEN 25 ELSE 0 END) +
                        (CASE WHEN rh_pct  BETWEEN {RH_MIN}   AND {RH_MAX}   THEN 25 ELSE 0 END) +
                        (CASE WHEN rain_mm >= {RAIN_MIN}                      THEN 25 ELSE 0 END) +
                        (CASE WHEN rad_mj_m2 >= {RAD_MIN}                     THEN 25 ELSE 0 END)
                    ), 2) AS score_final
                FROM read_parquet('{SILVER_PATH}')
                WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
                  AND lon BETWEEN {LON_MIN} AND {LON_MAX}
                GROUP BY ROUND(lat,6), ROUND(lon,6)
            ),
            estacional AS (
                SELECT
                    ROUND(lat,6) AS lat,
                    ROUND(lon,6) AS lon,
                    mes,
                    ROUND(AVG(
                        (CASE WHEN temp_c  BETWEEN {TEMP_MIN} AND {TEMP_MAX} THEN 25 ELSE 0 END) +
                        (CASE WHEN rh_pct  BETWEEN {RH_MIN}   AND {RH_MAX}   THEN 25 ELSE 0 END) +
                        (CASE WHEN rain_mm >= {RAIN_MIN}                      THEN 25 ELSE 0 END) +
                        (CASE WHEN rad_mj_m2 >= {RAD_MIN}                     THEN 25 ELSE 0 END)
                    ), 2) AS score_mes
                FROM read_parquet('{SILVER_PATH}')
                WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
                  AND lon BETWEEN {LON_MIN} AND {LON_MAX}
                GROUP BY ROUND(lat,6), ROUND(lon,6), mes
            )
            SELECT
                r.lat, r.lon,
                r.score_final,
                r.dias_evaluados,
                e.mes,
                e.score_mes
            FROM ranking r
            -- JOIN seguro: lat+lon son las PKs de ambas CTEs — cardinalidad 1:12
            -- Un JOIN sin PK aqui produciria un producto cartesiano de 144x1728 = 248.832 filas
            JOIN estacional e
              ON r.lat = e.lat
             AND r.lon = e.lon
            ORDER BY r.score_final DESC, e.mes
            LIMIT 20
        """)

    else:
        log.warning("Capa '%s' no reconocida para EXPLAIN.", capa)

    con.close()
