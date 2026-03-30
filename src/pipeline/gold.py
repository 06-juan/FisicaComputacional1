import duckdb
import os

silver_path = 'data/silver/clima_cafe_silver.parquet'
os.makedirs('data/gold', exist_ok=True)

def generar_ranking_gold():
    con = duckdb.connect()
    gold_ranking = 'data/gold/ranking_puntos.parquet'
    
    print("🏆 Calculando y guardando el Ranking de Confort Cafetero...")
    
    # Usamos una subconsulta para poder usar el 'score_final' en el cálculo del %
    query = f"""
    COPY (
        WITH scoring_base AS (
            SELECT 
                lat, lon,
                (CASE WHEN temp_c BETWEEN 18 AND 22 THEN 10
                      WHEN temp_c BETWEEN 15 AND 25 THEN 5
                      ELSE 0 END +
                 CASE WHEN vpd_kpa BETWEEN 0.3 AND 0.7 THEN 10
                      WHEN vpd_kpa BETWEEN 0.1 AND 1.0 THEN 5
                      ELSE 0 END +
                 CASE WHEN humedad_relativa BETWEEN 70 AND 85 THEN 10
                      ELSE 5 END +
                 CASE WHEN balance_hidrico_mm > 0 THEN 10 ELSE 0 END) as score_dia
            FROM '{silver_path}'
        ),
        puntos_agrupados AS (
            SELECT 
                lat, lon,
                ROUND(AVG(score_dia), 2) as score_final,
                COUNT(*) as dias_evaluados
            FROM scoring_base
            GROUP BY lat, lon
        )
        SELECT 
            *,
            ROUND((score_final / 40.0) * 100, 2) || '%' as dias_optimos
        FROM puntos_agrupados
        ORDER BY score_final DESC
        LIMIT 5
    ) TO '{gold_ranking}' (FORMAT PARQUET);
    """
    
    con.execute(query)
    print(f"✅ Ranking guardado en: {gold_ranking}")
    
    # Mostrar resultados en consola
    mejores_puntos = con.execute(f"SELECT * FROM '{gold_ranking}'").df()
    print("\n🌟 LOS 5 MEJORES PUNTOS PARA CULTIVO:")
    print(mejores_puntos)
    return mejores_puntos

def generar_estacionalidad_mensual():
    con = duckdb.connect()
    gold_estacionalidad = 'data/gold/estacionalidad_mensual.parquet'

    query = f"""
    COPY (
        WITH scoring_diario AS (
            SELECT 
                month(fecha) as mes,
                lat, lon,
                -- Mantenemos tu lógica de puntos
                (CASE WHEN temp_c BETWEEN 18 AND 22 THEN 10 WHEN temp_c BETWEEN 15 AND 25 THEN 5 ELSE 0 END +
                 CASE WHEN vpd_kpa BETWEEN 0.3 AND 0.7 THEN 10 WHEN vpd_kpa BETWEEN 0.1 AND 1.0 THEN 5 ELSE 0 END +
                 CASE WHEN humedad_relativa BETWEEN 70 AND 85 THEN 10 ELSE 5 END +
                 CASE WHEN balance_hidrico_mm > 0 THEN 10 ELSE 0 END) as score_dia
            FROM '{silver_path}'
        )
        SELECT 
            mes,
            ROUND(AVG(score_dia), 2) as score_mensual_promedio,
            ROUND(STDDEV(score_dia), 2) as variabilidad_score,
            ROUND((score_mensual_promedio / 40.0) * 100, 2) || '%' as confort_pct
        FROM scoring_diario
        GROUP BY mes
        ORDER BY score_mensual_promedio DESC
    ) TO '{gold_estacionalidad}' (FORMAT PARQUET);
    """
    con.execute(query)
    print(f"📈 Estacionalidad guardada en: {gold_estacionalidad}")
    
    # Mostrar resumen
    resumen = con.execute("SELECT * FROM 'data/gold/estacionalidad_mensual.parquet'").df()
    print("\n📅 SCORE PROMEDIO POR MES (Toda la región):")
    print(resumen)

def procesar_gold():
    generar_ranking_gold()
    generar_estacionalidad_mensual()

if __name__ == "__main__":
    generar_ranking_gold()
    generar_estacionalidad_mensual()