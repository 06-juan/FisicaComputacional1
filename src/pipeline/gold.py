import os
import duckdb

def crear_gold_agricola(con):
    query = """
    SELECT 
        strftime(hora_local, '%Y-%m') AS mes_anio,
        latitude, 
        longitude,
        ROUND(AVG(temp_c), 2) AS temp_media_c,
        ROUND(MAX(temp_c), 1) AS t_max_absoluta,
        ROUND(MIN(temp_c), 1) AS t_min_absoluta,
        ROUND(SUM(rain_mm), 2) AS lluvia_total_mm,
        -- Un día se considera lluvioso si cae más de 1mm (umbral agrícola)
        COUNT(CASE WHEN rain_mm > 1.0 THEN 1 END) AS dias_lluvia_significativa
    FROM 'data/silver/clima_eje_cafetero_silver.parquet'
    GROUP BY 1, 2, 3
    ORDER BY mes_anio, latitude
    """
    con.execute(f"COPY ({query}) TO 'data/gold/agricola_mensual.parquet' (FORMAT PARQUET)")
    print("✅ Tabla Gold Agrícola creada.")

def crear_gold_dinamica(con):
    query = """
    SELECT 
        CAST(hora_local AS DATE) AS fecha,
        latitude, 
        longitude,
        -- Amplitud térmica diaria
        ROUND(MAX(temp_c) - MIN(temp_c), 2) AS amplitud_termica,
        -- Promedio de velocidad del viento (Energía cinética promedio)
        ROUND(AVG(w_speed_ms), 2) AS viento_medio_ms,
        -- Ráfaga máxima detectada en los puntos de control
        MAX(w_speed_ms) AS ráfaga_max_ms,
        -- Clasificación física del día
        CASE 
            WHEN MAX(temp_c) > 28 THEN 'Caluroso'
            WHEN MIN(temp_c) < 12 THEN 'Frío/Helada'
            ELSE 'Templado'
        END AS clasificacion_termica
    FROM 'data/silver/clima_eje_cafetero_silver.parquet'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) = 2 --dos horas porque eso es con lo que trabajamos
    ORDER BY fecha, latitude
    """
    con.execute(f"COPY ({query}) TO 'data/gold/dinamica_diaria.parquet' (FORMAT PARQUET)")
    print("✅ Tabla Gold Dinámica creada.")

def procesar_gold():
    os.makedirs('data/gold', exist_ok=True)
    con = duckdb.connect()
    
    print("Generando Capa Gold...")
    crear_gold_agricola(con)
    crear_gold_dinamica(con)
    
    # Muestra rápida
    print("\n--- Muestra Gold Dinámica (Primeras filas) ---")
    print(con.execute("SELECT * FROM 'data/gold/dinamica_diaria.parquet' LIMIT 5").df())

    print("\n--- Muestra Gold agricola (Primeras filas) ---")
    print(con.execute("SELECT * FROM 'data/gold/agricola_mensual.parquet' LIMIT 5").df())

if __name__ == "__main__":
    procesar_gold()