import duckdb
import os

# --- CONFIGURACIÓN ---
RAW_PATH = 'data/raw/raw.parquet' 
SILVER_PATH = 'data/silver/'
OUTPUT_SILVER = os.path.join(SILVER_PATH, 'clima_cafe_silver.parquet')

os.makedirs(SILVER_PATH, exist_ok=True)

import duckdb

def procesar_silver():
    con = duckdb.connect()
    
    # OUTPUT_SILVER y RAW_PATH deben estar definidos en tu script
    query = f"""
    WITH basic_calc AS (
        SELECT 
            CAST(fecha AS DATE) as fecha,
            CAST(latitud AS DOUBLE) as lat,
            CAST(longitud AS DOUBLE) as lon,
            (temp_k - 273.15) AS t_c,
            (dew_k - 273.15) AS td_c,
            (lluvia_m * 1000.0) AS lluvia_raw,
            (rad_j_m2 / 1000000.0) AS rad_raw,
            (evap_pot_m * 1000.0) AS evap_raw
        FROM '{RAW_PATH}'
        -- RESTRICCIÓN GEOGRÁFICA (Data Contract 3.2)
        WHERE latitud BETWEEN -4.3 AND 12.5 
          AND longitud BETWEEN -79.0 AND -66.8
    ),
    thermo_logic AS (
        SELECT 
            *,
            -- Presión de vapor saturado y real (kPa)
            0.61078 * exp((17.27 * t_c) / (t_c + 237.3)) AS es,
            0.61078 * exp((17.27 * td_c) / (td_c + 237.3)) AS ea
        FROM basic_calc
    ),
    integrity_check AS (
        SELECT
            fecha, lat, lon,
            -- RESTRICCIÓN TÉRMINA (Data Contract 2.0)
            GREATEST(-10.0, LEAST(45.0, t_c)) as temp_c,
            -- RESTRICCIÓN T/Td (Data Contract 3.2): Punto de rocío <= Temperatura
            LEAST(td_c, t_c) as dew_c,
            -- RESTRICCIÓN HUMEDAD (Data Contract 3.2): Truncar a 100%
            LEAST(100.0, GREATEST(0.0, 100.0 * (ea / es))) as humedad_relativa,
            -- RESTRICCIÓN VALORES POSITIVOS (Data Contract 2.0)
            GREATEST(0.0, lluvia_raw) as lluvia_mm,
            GREATEST(0.0, rad_raw) as rad_mj_m2,
            GREATEST(0.0, evap_raw) as evap_pot_mm,
            es, ea
        FROM thermo_logic
    )
    SELECT 
        fecha,
        lat,
        lon,
        ROUND(temp_c, 2) as temp_c,
        ROUND(dew_c, 2) as dew_c,
        ROUND(humedad_relativa, 2) as humedad_relativa,
        ROUND(lluvia_mm, 4) as lluvia_mm,
        ROUND(rad_mj_m2, 4) as rad_mj_m2,
        ROUND(evap_pot_mm, 4) as evap_pot_mm,
        -- Indicadores Gold-Ready
        ROUND(es - ea, 3) as vpd_kpa,
        ROUND(lluvia_mm - evap_pot_mm, 4) as balance_hidrico_mm
    FROM integrity_check
    WHERE fecha IS NOT NULL -- Requisito NOT NULL
    ORDER BY fecha, lat, lon
    """
    
    print("💎 Refinando Capa Silver según el Data Contract...")
    con.execute(f"COPY ({query}) TO '{OUTPUT_SILVER}' (FORMAT PARQUET, COMPRESSION ZSTD)")

def sanity_checks():
    con = duckdb.connect()
    # Sanity Check final en consola
    check = con.execute(f"""
        SELECT 
            COUNT(*) as total,
            MAX(humedad_relativa) as max_hr,
            MIN(temp_c) as min_t,
            SUM(CASE WHEN dew_c > temp_c THEN 1 ELSE 0 END) as errores_fisicos
        FROM '{OUTPUT_SILVER}'
    """).df()
    
    print("\n--- INFORME DE CUMPLIMIENTO (SLA) ---")
    print(check)

if __name__ == "__main__":
    if os.path.exists(RAW_PATH):
        procesar_silver()
        sanity_checks()
    else:
        print(f"❌ Error: No se encuentra '{RAW_PATH}'. Revisa la consolidación previa.")