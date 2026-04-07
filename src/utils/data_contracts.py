"""
Contratos de Datos — Arquitectura Medallón
Capa BRONZE / SILVER / GOLD
"""

import duckdb
import logging
from pathlib import Path
from src.utils.dbconect import conectar_bd, desconectar_bd

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONTRATO BRONZE
# ─────────────────────────────────────────────
BRONZE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bronze_clima (
    row_id       VARCHAR        PRIMARY KEY,          -- UUID generado en ingesta
    fecha        DATE           NOT NULL,
    lat          DOUBLE         NOT NULL,
    lon          DOUBLE         NOT NULL,
    temp_k       DOUBLE         NOT NULL CHECK (temp_k  BETWEEN 200 AND 340),
    dew_k        DOUBLE         NOT NULL CHECK (dew_k   BETWEEN 200 AND 340),
    lluvia_m     DOUBLE         NOT NULL CHECK (lluvia_m >= 0),
    rad_j_m2     DOUBLE         NOT NULL CHECK (rad_j_m2 >= 0),
    evap_pot_m   DOUBLE                  CHECK (evap_pot_m >= 0),
    ingested_at  TIMESTAMP      NOT NULL DEFAULT current_timestamp
);
"""

# ─────────────────────────────────────────────
# CONTRATO SILVER
# ─────────────────────────────────────────────
SILVER_SCHEMA = """
CREATE TABLE IF NOT EXISTS silver_clima (
    pk_silver    VARCHAR        PRIMARY KEY,          -- fecha|lat|lon
    fecha        DATE           NOT NULL,
    anio         INTEGER        NOT NULL CHECK (anio BETWEEN 2000 AND 2100),
    mes          INTEGER        NOT NULL CHECK (mes  BETWEEN 1 AND 12),
    lat          DOUBLE         NOT NULL,
    lon          DOUBLE         NOT NULL,
    temp_c       DOUBLE         NOT NULL CHECK (temp_c  BETWEEN -80 AND 60),
    rh_pct       DOUBLE         NOT NULL CHECK (rh_pct  BETWEEN 0   AND 100),
    rain_mm      DOUBLE         NOT NULL CHECK (rain_mm >= 0),
    rad_mj_m2    DOUBLE         NOT NULL CHECK (rad_mj_m2 >= 0),
    evap_pot_mm  DOUBLE                  CHECK (evap_pot_mm >= 0),
    processed_at TIMESTAMP      NOT NULL DEFAULT current_timestamp
);
"""

# ─────────────────────────────────────────────
# CONTRATO GOLD — ranking de confort
# ─────────────────────────────────────────────
GOLD_RANKING_SCHEMA = """
CREATE TABLE IF NOT EXISTS gold_ranking (
    pk_gold         VARCHAR   PRIMARY KEY,            -- lat|lon
    lat             DOUBLE    NOT NULL,
    lon             DOUBLE    NOT NULL,
    score_final     DOUBLE    NOT NULL CHECK (score_final BETWEEN 0 AND 100),
    dias_evaluados  INTEGER   NOT NULL CHECK (dias_evaluados > 0),
    dias_optimos    INTEGER   NOT NULL CHECK (dias_optimos >= 0),
    pct_optimos     DOUBLE    NOT NULL CHECK (pct_optimos BETWEEN 0 AND 100),
    aggregated_at   TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""

# ─────────────────────────────────────────────
# CONTRATO GOLD — estacionalidad mensual
# ─────────────────────────────────────────────
GOLD_ESTACIONAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS gold_estacionalidad (
    pk_estacional          VARCHAR   PRIMARY KEY,     -- mes|lat|lon
    mes                    INTEGER   NOT NULL CHECK (mes BETWEEN 1 AND 12),
    lat                    DOUBLE    NOT NULL,
    lon                    DOUBLE    NOT NULL,
    score_mensual_promedio DOUBLE    NOT NULL,
    variabilidad_score     DOUBLE    NOT NULL CHECK (variabilidad_score >= 0),
    confort_pct            DOUBLE    NOT NULL CHECK (confort_pct BETWEEN 0 AND 100),
    aggregated_at          TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""


def aplicar_contratos(db_path: str = "data/pipeline.duckdb") -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Usamos tu función de conexión para mantener la abstracción
    con = conectar_bd(db_path) 
    if not con:
        return

    try:
        # DuckDB permite transacciones explícitas, pero hay que ser precavidos
        con.begin() 
        
        schemas = [
            ("bronze_clima", BRONZE_SCHEMA),
            ("silver_clima", SILVER_SCHEMA),
            ("gold_ranking", GOLD_RANKING_SCHEMA),
            ("gold_estacionalidad", GOLD_ESTACIONAL_SCHEMA),
        ]

        for nombre, ddl in schemas:
            con.execute(ddl)
            log.info(f"Contrato verificado/aplicado: {nombre}")

        con.commit()
        log.info("Éxito: Estado de la base de datos sincronizado.")

    except Exception as exc:
        # Solo intentamos rollback si la conexión sigue viva
        try:
            con.rollback()
        except:
            pass 
        log.error(f"Fallo crítico. Se intentó revertir cambios: {exc}")
        raise
    finally:
        desconectar_bd(con)


if __name__ == "__main__":
    aplicar_contratos()
