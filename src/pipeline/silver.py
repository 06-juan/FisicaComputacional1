import duckdb
import os

SILVER_PATH = 'data/silver/'
os.makedirs(SILVER_PATH, exist_ok=True)

RAW_PRESSURE = 'data/raw/pressure_bronze.parquet'
RAW_LAND = 'data/raw/land_bronze.parquet'
OUTPUT_SILVER = os.path.join(SILVER_PATH, 'clima_eje_cafetero_silver.parquet')

def procesar_silver():
    con = duckdb.connect()
    
    query = f"""WITH clean_land AS (
                SELECT 
                    (valid_time - INTERVAL 5 HOURS) AS hora_local,
                    ROUND(latitude, 1) as lat_join, -- Redondeamos a 0.1 grados
                    ROUND(longitude, 1) as lon_join,
                    latitude, longitude, t2m, sp, tp
                FROM '{RAW_LAND}'
                ),
                clean_pressure AS (
                    SELECT 
                        (valid_time - INTERVAL 5 HOURS) AS hora_local,
                        ROUND(latitude, 1) as lat_join,
                        ROUND(longitude, 1) as lon_join,
                        t, u, v
                    FROM '{RAW_PRESSURE}'
                )
                SELECT 
                    l.hora_local,
                    l.latitude,
                    l.longitude,
                    ROUND(l.t2m - 273.15, 2)                              AS temp_c,
                    ROUND(l.sp / 100.0, 2)                                AS pres_mbar,
                    ROUND(SQRT(p.u * p.u + p.v * p.v), 2)                AS w_speed_ms,
                    ROUND((ATAN2(p.u, p.v) * 180.0 / PI() + 180) % 360, 1) AS w_dir_deg,
                    ROUND(l.tp * 1000.0, 4)                               AS rain_mm
                FROM clean_land l 
                INNER JOIN clean_pressure p 
                    ON  l.hora_local = p.hora_local 
                    AND ABS(l.lat_join - p.lat_join) < 0.01 
                    AND ABS(l.lon_join - p.lon_join) < 0.01
                """
    
    print("Ejecutando transformaciones Silver con DuckDB...")
    con.execute(f"COPY ({query}) TO '{OUTPUT_SILVER}' (FORMAT PARQUET)")
    
    print(f"¡Capa Silver creada con éxito!")
    print(con.execute(f"SELECT * FROM '{OUTPUT_SILVER}' ORDER BY latitude ASC LIMIT 5").df())

#-- sanity check

def sanity_checks():

    con = duckdb.connect()

    print(con.execute("""SELECT 
            CAST(hora_local AS DATE) AS fecha,
            latitude,
            longitude,
            COUNT(*) AS conteo_horas,
            -- Esto nos dirá qué horas tenemos realmente en ese grupo
            list(strftime(hora_local, '%H:%M')) AS horas_presentes
        FROM 'data/silver/clima_eje_cafetero_silver.parquet'
        GROUP BY 1, 2, 3
        HAVING conteo_horas < 2  -- Solo muéstrame los que están incompletos
        ORDER BY fecha ASC;SELECT 
            CAST(hora_local AS DATE) AS fecha,
            latitude,
            longitude,
            COUNT(*) AS conteo_horas,
            -- Esto nos dirá qué horas tenemos realmente en ese grupo
            list(strftime(hora_local, '%H:%M')) AS horas_presentes
        FROM 'data/silver/clima_eje_cafetero_silver.parquet'
        GROUP BY 1, 2, 3
        HAVING conteo_horas < 2  -- Solo muéstrame los que están incompletos
        ORDER BY fecha ASC;""").df())

if __name__ == "__main__":
    procesar_silver()
    sanity_checks()