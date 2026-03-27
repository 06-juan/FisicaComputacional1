# Pipeline de Ingeniería de Datos Climáticos: Eje Cafetero ☕🌦️

Este proyecto implementa una arquitectura de datos tipo **Medallion** (Bronze, Silver, Gold) para la ingesta, procesamiento y análisis de datos meteorológicos históricos provenientes de **Copernicus Climate Data Store (CDS)**, enfocándose en la región del Eje Cafetero, Colombia (Caldas, Quindío, Risaralda).

## 📋 Arquitectura del Proyecto

El flujo de datos se divide en tres capas funcionales:
1. **Capa Bronze (Ingestión):** Datos crudos descargados vía API en formato `.nc` (NetCDF) y consolidados en un CSV bruto en `data/bronze/`.
2. **Capa Silver (Transformación):** Limpieza de datos, manejo de valores nulos, conversión de unidades físicas (Kelvin a Celsius) y feature engineering (viento resultante).
3. **Capa Gold (Analítica):** Generación de KPIs climáticos y agregaciones mensuales listas para visualización o modelos de predicción.

---

## 🛠️ Configuración de Credenciales (CDS API)

Para que el script `downloadData.py` funcione, debes configurar tus credenciales de Copernicus en tu máquina local.

### ⚠️ Nota Crítica para Windows
La librería `cdsapi` requiere que el archivo de configuración `.cdsapirc` esté codificado en **UTF-8** (sin BOM). Si se guarda en UTF-16 (formato por defecto de PowerShell al usar `>`), el script lanzará un error de "Missing/incomplete configuration file".

**Pasos para configurar:**
1. Regístrate en [CDS Copernicus](https://cds.climate.copernicus.eu/).
2. Obtén tu UID y API Key desde tu perfil (Sección "API Key").
3. Crea el archivo en `C:\Users\TU_USUARIO\.cdsapirc` con el siguiente contenido:

url: https://cds.climate.copernicus.eu/api
key: TU_UID:TU_API_KEY

*Nota: Asegúrate de guardar el archivo usando el Bloc de Notas y seleccionando explícitamente la codificación UTF-8.*

---

## 📊 Estructura del Dataset

### Capa Raw — Variables descargadas de ERA5

| Variable | Descripción | Unidad | Fuente |
| :--- | :--- | :--- | :--- |
| `valid_time` | Marca de tiempo UTC | — | Ambas |
| `latitude` | Latitud | ° | Ambas |
| `longitude` | Longitud | ° | Ambas |
| `t` | Temperatura a nivel 1000 hPa | K | ERA5 Pressure Levels |
| `u` | Viento zonal a 1000 hPa | m/s | ERA5 Pressure Levels |
| `v` | Viento meridional a 1000 hPa | m/s | ERA5 Pressure Levels |
| `t2m` | Temperatura del aire a 2 m | K | ERA5-Land |
| `sp` | Presión en superficie | Pa | ERA5-Land |
| `tp` | Precipitación total | m | ERA5-Land |

### Capa Silver — Variables transformadas (`clima_eje_cafetero_silver.parquet`)

| Variable | Descripción | Unidad | Transformación |
| :--- | :--- | :--- | :--- |
| `time` | Marca de tiempo UTC | — | Renombrado de `valid_time` |
| `latitude` | Latitud | ° | Sin cambio |
| `longitude` | Longitud | ° | Sin cambio |
| `temp_c` | Temperatura del aire a 2 m | °C | `t2m - 273.15` |
| `pres_mbar` | Presión en superficie | mbar | `sp / 100` |
| `w_speed_ms` | Rapidez del viento | m/s | `√(u² + v²)` |
| `w_dir_deg` | Dirección del viento | ° (0–360) | `atan2(u, v) · 180/π + 180` |
| `rain_mm` | Precipitación total | mm | `tp × 1000` |

---

## 🚀 Ejecución del Pipeline

### 1. Ingesta (`src/ingest/downloadData.py`)
Descarga los datos para el Eje Cafetero (Lat: [4.0, 5.6], Lon: [-76.1, -75.2]). 
* **Restricción de Costo:** Debido a las políticas de la API CDS-Beta, se procesan bloques de 5 a 10 años para evitar el error `cost limits exceeded`.

### 2. Transformación (`src/pipeline/silver.py`)
* **Conversión Térmica:** Cálculo de grados Celsius: T(°C) = T(K) - 273.15.
* **Magnitud del Viento:** Cálculo vectorial de la velocidad real mediante la raíz cuadrada de la suma de los componentes u y v al cuadrado.
* **Normalización:** Manejo de NaNs generados por el merge de datasets con diferentes rangos temporales.

### 3. Agregación (`src/pipeline/gold.py`)
* Promedios diarios/mensuales por coordenada geográfica.
* Detección de anomalías térmicas y de precipitación relevantes para el sector agroindustrial.

---

## 📦 Requisitos Técnicos
* **Python:** 3.10+
* **Librerías:** `cdsapi`, `xarray`, `pandas`, `netCDF4`
* **Entorno:** Virtualenv (`.venv`) en Windows o Linux (Ubuntu).