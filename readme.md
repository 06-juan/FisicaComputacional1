===============================================================================
   PIPELINE DE INGENIERÍA DE DATOS CLIMÁTICOS: ERA5-LAND & CAFÉ ☕🌦️
===============================================================================

Este proyecto implementa una arquitectura de datos tipo Medallion optimizada 
para el análisis agroclimático en el Eje Cafetero, utilizando datos de alta 
resolución de ERA5-Land vía Google Earth Engine (GEE).

-------------------------------------------------------------------------------
1. ARQUITECTURA DEL PROYECTO (MEDALLION+)
-------------------------------------------------------------------------------

El flujo de datos se ha automatizado para eliminar la descarga manual:

A. CAPA RAW (Cloud): 
   Exportación de fragmentos temporales desde GEE a Google Drive (CSV/ZIP).

B. CAPA BRONZE (Local Ingest): 
   Descarga automática desde Drive a 'data/raw/' y consolidación masiva 
   en un único archivo 'raw.parquet' usando DuckDB.

C. CAPA SILVER (Transformación): 
   Limpieza, conversión de unidades (K a °C, Joules a MJ/m2) y cálculo de 
   variables derivadas (Humedad Relativa, Evapotranspiración).

D. CAPA GOLD (Analítica): 
   Agregaciones espaciales y temporales para modelos de calidad de café.

-------------------------------------------------------------------------------
2. CONFIGURACIÓN DE CREDENCIALES
-------------------------------------------------------------------------------

El sistema requiere dos niveles de acceso para la automatización total:

- GOOGLE EARTH ENGINE (GEE): 
  Necesario para procesar en la nube. Ejecuta en terminal:
  > earthengine authenticate

- GOOGLE DRIVE API: 
  Para descargar automáticamente los archivos procesados.
  1. Habilita 'Google Drive API' en Google Cloud Console.
  2. Descarga las credenciales 'Desktop App' como 'credentials.json'.
  3. Guárdalo en la raíz de este proyecto.
  * Nota: La primera ejecución generará un 'token.pickle' tras el login.

-------------------------------------------------------------------------------
3. ESTRUCTURA DEL DATASET (VARIABLES CRÍTICAS)
-------------------------------------------------------------------------------

CAPA BRONZE (raw.parquet):
- temp_k:       Temperatura aire 2m (K) -> Desarrollo de plagas (Roya).
- dew_k:        Punto de rocío (K)       -> Cálculo de Humedad Relativa.
- lluvia_m:     Precipitación total (m)  -> Estrés hídrico y floración.
- rad_j_m2:     Radiación solar (J/m2)   -> Fotosíntesis y secado.
- evap_pot_m:   Evaporación potencial(m) -> Balance hídrico del suelo.

CAPA SILVER (clima_cafe_silver.parquet):
- temp_c:       temp_k - 273.15          -> Grados Celsius.
- rh_pct:       Basado en temp y dew     -> % Humedad Relativa.
- rain_mm:      lluvia_m * 1000          -> Milímetros de lluvia.
- rad_mj_m2:    rad_j_m2 / 1,000,000     -> MegaJoules/m2.

-------------------------------------------------------------------------------
4. EJECUCIÓN DEL PIPELINE
-------------------------------------------------------------------------------

1. INGESTA Y DESCARGA (src/ingest/downloadData.py):
   - Lanza tareas de exportación en GEE.
   - Monitorea y descarga automáticamente de Drive (CSV o ZIP).
   - Une todo en un Parquet comprimido con ZSTD (vía DuckDB).

2. PROCESAMIENTO SILVER (src/pipeline/silver.py):
   - Aplica conversiones físicas y filtros de calidad.

-------------------------------------------------------------------------------
5. REQUISITOS TÉCNICOS
-------------------------------------------------------------------------------
- Python: 3.10+
- Librerías: earthengine-api, google-api-python-client, duckdb, pandas, pyarrow.
- Hardware: 8GB+ RAM recomendado para consolidación masiva.

===============================================================================
Proyecto desarrollado para el análisis climático regional - Quindío, Colombia.
===============================================================================