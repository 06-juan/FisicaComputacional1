import duckdb
import os

SILVER_PATH = 'data/silver/'
os.makedirs(SILVER_PATH, exist_ok=True)

RAW_PRESSURE = 'data/raw/pressure.parquet'
RAW_LAND = 'data/raw/land.parquet'
OUTPUT_SILVER = os.path.join(SILVER_PATH, 'clima_eje_cafetero_silver.parquet')

def procesar_silver():
    con = duckdb.connect()
    
    query = f"""WITH clean_land AS (
                    SELECT 
                        src.valid_time AS time,
                        src.latitude,
                        src.longitude,
                        src.t2m,
                        src.sp,
                        src.tp
                    FROM '{RAW_LAND}' AS src
                ),
                clean_pressure AS (
                    SELECT 
                        src.valid_time AS time,
                        src.latitude,
                        src.longitude,
                        src.t,
                        src.u,
                        src.v
                    FROM '{RAW_PRESSURE}' AS src
                )
                SELECT 
                    l.time,
                    l.latitude,
                    l.longitude,
                    ROUND(l.t2m - 273.15, 2)                              AS temp_c,
                    ROUND(l.sp / 100.0, 2)                                AS pres_mbar,
                    ROUND(SQRT(p.u * p.u + p.v * p.v), 2)                AS w_speed_ms,
                    ROUND((ATAN2(p.u, p.v) * 180.0 / PI() + 180) % 360, 1) AS w_dir_deg,
                    ROUND(l.tp * 1000.0, 4)                               AS rain_mm
                FROM clean_land l
                INNER JOIN clean_pressure p 
                    ON  l.time      = p.time 
                    AND l.latitude  = p.latitude 
                    AND l.longitude = p.longitude
                """
    
    print("Ejecutando transformaciones Silver con DuckDB...")
    con.execute(f"COPY ({query}) TO '{OUTPUT_SILVER}' (FORMAT PARQUET)")
    
    print(f"¡Capa Silver creada con éxito!")
    print(con.execute(f"SELECT * FROM '{OUTPUT_SILVER}' LIMIT 5").df())

if __name__ == "__main__":
    procesar_silver()