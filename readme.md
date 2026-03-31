PIPELINE DE INGENIERÍA DE DATOS CLIMÁTICOS: ERA5-LAND & CAFÉ ☕🌦️

Este proyecto implementa una arquitectura de datos tipo Medallion optimizada 
para el análisis agroclimático en el Eje Cafetero, utilizando datos de alta 
resolución de ERA5-Land vía Google Earth Engine (GEE).

-------------------------------------------------------------------------------
1. ARQUITECTURA DEL PROYECTO (MEDALLION+)
-------------------------------------------------------------------------------

El flujo de datos se ha automatizado mediante una lógica de cascada inteligente:

A. CAPA RAW (Cloud & Drive): 
   Exportación asíncrona de fragmentos anuales desde GEE a Google Drive en 
   formato CSV. Monitoreo de tareas en tiempo real desde el script local.

B. CAPA BRONZE (Ingesta Local): 
   Sincronización automática Drive -> Local ('data/raw/'). Se utiliza DuckDB 
   para realizar una consolidación masiva de múltiples archivos CSV en un único 
   'raw.parquet' optimizado con compresión ZSTD.

C. CAPA SILVER (Transformación): 
   Limpieza, conversión de unidades físicas y cálculo de variables derivadas 
   (Humedad Relativa, Radiación en MJ/m2) necesarias para modelos agronómicos.

D. CAPA GOLD (Analítica): 
   Agregaciones espaciales (BBOX Eje Cafetero) y temporales para indicadores 
   de calidad y fenología del café.

-------------------------------------------------------------------------------
2. CONFIGURACIÓN DE CREDENCIALES
-------------------------------------------------------------------------------

El sistema utiliza un archivo '.env' para gestionar credenciales sensibles. 
Sigue estos pasos para configurar tu entorno:

A. CREACIÓN DEL ARCHIVO .ENV:
   En la raíz del proyecto, crea un archivo llamado '.env' (sin nombre, solo 
   la extensión) y añade el siguiente contenido:

   # Google Earth Engine Configuration
   GEE_PROJECT_ID=tu-proyecto-id-de-gee

   # Google Drive Configuration
   DRIVE_FOLDER_NAME=NombreDeTuCarpetaEnDrive

   # Local Paths
   RAW_DATA_PATH=data/raw/

B. NIVELES DE ACCESO:
   - GOOGLE EARTH ENGINE (GEE): 
     Necesario para el procesamiento satelital. Ejecuta en terminal:
     > earthengine authenticate

   - GOOGLE DRIVE API (OAuth 2.0): 
     1. Habilita 'Google Drive API' en Google Cloud Console.
     2. Crea credenciales de tipo 'Desktop App'.
     3. Descarga el JSON y renombralo como 'credentials.json' en la raíz.
     * El script generará un 'token.json' automáticamente tras el primer login.

C. SEGURIDAD (IMPORTANTE):
   Asegúrate de que tu archivo '.gitignore' incluya las siguientes líneas para 
   evitar fugas de seguridad en GitHub:
   
   .env
   credentials.json
   token.json
   data/
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