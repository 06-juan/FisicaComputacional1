# DATA CONTRACT: Proyecto ERA5-Land Colombia (Física del Café)
# Versión: 2.0 (Gold Mission)
# Estado: Activo

## 0. DESCRIPCIÓN DEL DATASET
Este contrato define las especificaciones técnicas para el dataset consolidado de la capa BRONZE/SILVER extraído de Google Earth Engine (ERA5-Land Daily Aggregated). El objetivo es servir de entrada para modelos de idoneidad climática cafetera.

## 1. ESQUEMA DE DATOS (MAPPING DE INGESTA)

| Variable GEE (Input)                   | Columna Bronze (Output) | Unidad Física |
|----------------------------------------|-------------------------|---------------|
| temperature_2m                         | temp_c                  | Celsius       |
| dewpoint_temperature_2m                | dew_c                   | Celsius       |
| total_precipitation_sum                | lluvia_mm               | mm/día        |
| surface_solar_radiation_downwards_sum  | rad_mj_m2               | MJ/m²         |
| potential_evaporation_sum              | evap_pot_mm             | mm/día        |

### 1.1 REGLAS DE TRANSFORMACIÓN
1. Radiación: El valor de GEE viene en J/m². Debe dividirse por 1,000,000 para obtener MJ/m².
2. Precipitación/Evaporación: Vienen en metros (m). Deben multiplicarse por 1,000 para obtener mm.
3. Temperatura: Viene en Kelvin (K). Restar 273.15 para Celsius.

## 2. ESQUEMA DE DATOS (SCHEMA)

| Columna            | Tipo Dato | Unidad      | Restricción          | Descripción                                      |
|--------------------|-----------|-------------|----------------------|--------------------------------------------------|
| fecha              | DATE      | ISO 8601    | NOT NULL             | Fecha de la observación (YYYY-MM-DD).            |
| lon                | DOUBLE    | Grados Dec. | -79.0 a -66.8        | Longitud (WGS84).                                |
| lat                | DOUBLE    | Grados Dec. | -4.3 a 12.5          | Latitud (WGS84).                                 |
| temp_c             | DOUBLE    | Celsius     | -10.0 a 45.0         | Temperatura media a 2 metros.                    |
| dew_c              | DOUBLE    | Celsius     | < temp_c             | Punto de rocío a 2 metros.                       |
| humedad_relativa   | DOUBLE    | %           | 0.0 a 100.0          | Calculada via fórmula de Magnus.                 |
| lluvia_mm          | DOUBLE    | mm/día      | >= 0.0               | Precipitación total acumulada diaria.            |
| rad_mj_m2          | DOUBLE    | MJ/m²       | >= 0.0               | Radiación solar de onda corta descendente.       |
| evap_pot_mm        | DOUBLE    | mm/día      | >= 0.0               | Evaporación potencial diaria.                    |

## 3. CALIDAD DE DATOS (SLA)

### 3.1 Integridad
* **Completitud:** El dataset debe cubrir el rango 2015-01-01 hasta 2025-12-31 sin saltos temporales.
* **Valores Nulos:** No se permiten NULLs en las columnas `fecha`, `lat`, `lon`.

### 3.2 Validez Física
* **Relación T/Td:** El punto de rocío (`dew_c`) nunca debe ser mayor a la temperatura ambiente (`temp_c`).
* **Humedad:** Valores de `humedad_relativa` > 100% deben ser truncados a 100%.
* **Geometría:** Todas las coordenadas deben caer dentro del polígono simplificado de Colombia.

## 4. ESPECIFICACIONES DE SALIDA (CAPA GOLD)
El procesamiento posterior (Gold) debe generar los siguientes indicadores derivados:
1. **VPD (Vapor Pressure Deficit):** En kiloPascales (kPa).
2. **GDD (Grados Día):** Base 10°C.
3. **Balance Hídrico:** (lluvia_mm - evap_pot_mm).

## 5. FORMATO DE ALMACENAMIENTO
* **Formato:** Apache Parquet.
* **Compresión:** ZSTD (Nivel 3).
* **Particionamiento sugerido:** Por año o por región (departamento).

## 6. ESPECIFICACIONES DE LA CAPA GOLD (MODELO ANALÍTICO)

### 6.1 ÍNDICE DE CONFORT CAFETERO (ICC)
El dataset Gold debe calcular el score diario basado en los siguientes pesos:
- Temperatura (p_temp): 10 pts si [18°C-22°C], 5 pts si [15°C-25°C], else 0.
- VPD (p_vpd): 10 pts si [0.3-0.7 kPa], 5 pts si [0.1-1.0 kPa], else 0.
- Humedad (p_hr): 10 pts si [70%-85%], 5 pts si fuera de rango.
- Agua (p_agua): 10 pts si Balance Hídrico > 0 mm, else 0.

* Score Máximo Teórico: 40 puntos (100% Optimización Biosistémica).

### 6.2 AGREGACIONES REQUERIDAS
- Ranking Espacial: Top 5 coordenadas por Score Promedio Histórico.
- Estacionalidad Mensual: Promedio de Score y Desviación Estándar agrupado por mes (1-12).
- Porcentaje de Confort: Representación del score como (score/40)*100 con símbolo '%'.

## 7. REGLAS DE PRECISIÓN Y METADATOS

### 7.1 PRECISIÓN GEOGRÁFICA
- Las coordenadas (lat, lon) deben mantener una precisión de 6 decimales.
- Origen de datos: Centroide de celda ERA5-Land (~11.1 km de resolución).

### 7.2 PROTOCOLO DE ACTUALIZACIÓN (RE-RUN)
- Frecuencia: Bajo demanda o actualización mensual de GEE.
- Acción ante Fallos: Si una Task de GEE es cancelada (Error de Memoria), el 
  bloque temporal debe subdividirse de 5 años a 1 año.

## 8. CONTROL DE ACCESO Y SEGURIDAD (API ESTRATEGY)


### 8.1 SECRETOS Y LLAVES
- El archivo 'credentials.json' (Drive API) NUNCA debe incluirse en el dataset.
- El archivo 'token.pickle' es de uso local y temporal (User session).
- Repositorios: Se debe mantener un archivo '.gitignore' activo para evitar la 
  fuga de credenciales climáticas.

### 8.2 ALMACENAMIENTO FÍSICO LOCAL
- data/bronze/: Archivos CSV/ZIP temporales (Bronze).
- data/silver/: Parquet limpio y transformado (Single Source of Truth).
- data/gold/: Parquet con rankings y estacionalidad (Business Ready).
===============================================================================