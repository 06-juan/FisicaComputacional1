import duckdb
import os

# --- CONFIGURACIÓN ---
RAW_PATH = 'data/raw/raw.parquet' 
SILVER_PATH = 'data/silver/'
OUTPUT_SILVER = os.path.join(SILVER_PATH, 'clima_colombia_silver.parquet')

os.makedirs(SILVER_PATH, exist_ok=True)

def procesar_silver():
    con = duckdb.connect()
    
    # Lógica de Transformación:
    # 1. Conversión de Kelvin a Celsius (Temp y Dewpoint).
    # 2. Humedad Relativa (Magnus-Tetens).
    # 3. VPD (Vapor Pressure Deficit): Esencial para estrés hídrico.
    # 4. Escalamiento de lluvia, radiación y evaporación.
    
    query = f"""
    WITH basic_calc AS (
        SELECT 
            fecha,
            latitud,
            longitud,
            (temp_k - 273.15) AS t_c,
            (dew_k - 273.15) AS td_c,
            (lluvia_m * 1000.0) AS lluvia_mm,
            (rad_j_m2 / 1000000.0) AS rad_mj_m2,
            (evap_pot_m * 1000.0) AS evap_pot_mm
        FROM '{RAW_PATH}'
        -- Filtro para Colombia completa
        WHERE latitud BETWEEN -4.3 AND 12.5 
          AND longitud BETWEEN -79.0 AND -66.8
    ),
    thermo_indicators AS (
        SELECT 
            *,
            -- Presión de vapor de saturación (es) en kPa
            0.61078 * exp((17.27 * t_c) / (t_c + 237.3)) AS es,
            -- Presión de vapor real (ea) en kPa usando el punto de rocío
            0.61078 * exp((17.27 * td_c) / (td_c + 237.3)) AS ea
        FROM basic_calc
    )
    SELECT 
        fecha,
        latitud,
        longitud,
        ROUND(t_c, 2) AS temp_c,
        -- Humedad Relativa (%)
        ROUND(100.0 * (ea / es), 2) AS hr_pct,
        -- VPD (kPa): es - ea
        ROUND(es - ea, 3) AS vpd_kpa,
        ROUND(lluvia_mm, 4) AS lluvia_mm,
        ROUND(rad_mj_m2, 4) AS rad_mj_m2,
        ROUND(evap_pot_mm, 4) AS evap_pot_mm,
        -- Balance hídrico simple diario
        ROUND(lluvia_mm - evap_pot_mm, 4) AS balance_hidrico_mm
    FROM thermo_indicators
    ORDER BY fecha, latitud, longitud
    """
    
    print("💎 Transformando Capa Bronze a Silver con indicadores biofísicos...")
    con.execute(f"COPY ({query}) TO '{OUTPUT_SILVER}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    
    print(f"✅ Capa Silver creada con éxito en: {OUTPUT_SILVER}")
    
    # Muestra de resultados para verificar el VPD
    print("\n--- MUESTRA DE DATOS SILVER (BIOFÍSICA) ---")
    print(con.execute(f"SELECT * FROM '{OUTPUT_SILVER}' LIMIT 5").df())

def sanity_checks():
    con = duckdb.connect()
    print("\n--- SANITY CHECK: Consistencia Física ---")
    # Verificamos que HR no supere el 100% y que el VPD sea coherente
    res = con.execute(f"""
        SELECT 
            COUNT(*) as total_filas,
            MIN(hr_pct) as hr_min,
            MAX(hr_pct) as hr_max,
            AVG(vpd_kpa) as vpd_promedio,
            COUNT(*) FILTER (WHERE vpd_kpa < 0) as errores_fisicos_vpd
        FROM '{OUTPUT_SILVER}'
    """).df()
    print(res)

if __name__ == "__main__":
    if os.path.exists(RAW_PATH):
        procesar_silver()
        sanity_checks()
    else:
        print(f"❌ Error: No se encuentra '{RAW_PATH}'. Revisa la consolidación previa.")