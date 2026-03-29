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
* **Valores Nulos:** No se permiten NULLs en las columnas `fecha`, `lat`, `lon`. Para variables climáticas, el máximo de nulos permitido es 0.5% (manejo de bordes costeros).

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