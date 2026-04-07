# Arquitectura del Pipeline — ERA5-Land & Café ☕

Pipeline de ingeniería de datos para análisis agroclimático en el Eje Cafetero.  
Fuente: ERA5-Land via Google Earth Engine (GEE). Período: 2015–2024.

---

## Flujo general

```
GEE (ERA5-Land)
     │  exportación asíncrona por año (CSV)
     ▼
Google Drive
     │  sincronización automática
     ▼
[RAW] data/raw/                ← CSVs por año, sin tocar
     │  consolidación DuckDB → ZSTD Parquet
     ▼
[BRONZE] data/raw/raw.parquet  ← ingesta cruda + row_id UUID
     │  conversiones físicas, filtros, RH%
     ▼
[SILVER] data/silver/          ← clima_cafe_silver.parquet
         data/silver/partitioned/anio=X/mes=Y/
     │  agregaciones espaciales y temporales
     ▼
[GOLD]  data/gold/ranking_puntos.parquet
        data/gold/estacionalidad_mensual.parquet
        data/gold/estacionalidad_partitioned/mes=N/
     │
     ▼
mapa_interactivo.html
```

---

## Contratos de datos por capa

### BRONZE — `raw.parquet`

| Columna      | Tipo    | Restricción                  | Descripción                          |
|--------------|---------|------------------------------|--------------------------------------|
| row_id       | VARCHAR | PRIMARY KEY (UUID v4)        | Surrogate key generada en ingesta    |
| fecha        | DATE    | NOT NULL                     | Fecha de observación ERA5            |
| latitud      | DOUBLE  | NOT NULL                     | Latitud WGS84                        |
| longitud     | DOUBLE  | NOT NULL                     | Longitud WGS84                       |
| temp_k       | DOUBLE  | NOT NULL, BETWEEN 200–340    | Temperatura 2m (Kelvin)              |
| dew_k        | DOUBLE  | NOT NULL, BETWEEN 200–340    | Punto de rocío (Kelvin)              |
| lluvia_m     | DOUBLE  | NOT NULL, >= 0               | Precipitación total (metros)         |
| rad_j_m2     | DOUBLE  | NOT NULL, >= 0               | Radiación solar (J/m²)               |
| evap_pot_m   | DOUBLE  | >= 0                         | Evaporación potencial (metros)       |
| ingested_at  | TIMESTAMP | NOT NULL, DEFAULT now()    | Timestamp de ingesta                 |

### SILVER — `clima_cafe_silver.parquet`

| Columna      | Tipo    | Restricción                  | Descripción                          |
|--------------|---------|------------------------------|--------------------------------------|
| pk_silver    | VARCHAR | PRIMARY KEY (fecha\|lat\|lon)| PK natural compuesta serializada     |
| fecha        | DATE    | NOT NULL                     | Fecha de observación                 |
| anio         | INTEGER | NOT NULL, BETWEEN 2000–2100  | Año (para particionado)              |
| mes          | INTEGER | NOT NULL, BETWEEN 1–12       | Mes (para particionado)              |
| lat          | DOUBLE  | NOT NULL                     | Latitud redondeada 6 decimales       |
| lon          | DOUBLE  | NOT NULL                     | Longitud redondeada 6 decimales      |
| temp_c       | DOUBLE  | NOT NULL, BETWEEN -80–60     | Temperatura (°Celsius)               |
| rh_pct       | DOUBLE  | NOT NULL, BETWEEN 0–100      | Humedad relativa (%)                 |
| rain_mm      | DOUBLE  | NOT NULL, >= 0               | Precipitación (mm)                   |
| rad_mj_m2    | DOUBLE  | NOT NULL, >= 0               | Radiación solar (MJ/m²)              |
| evap_pot_mm  | DOUBLE  | >= 0                         | Evaporación potencial (mm)           |
| processed_at | TIMESTAMP | NOT NULL                   | Timestamp de transformación          |

### GOLD — `ranking_puntos.parquet`

| Columna        | Tipo      | Restricción                    | Descripción                        |
|----------------|-----------|--------------------------------|------------------------------------|
| pk_gold        | VARCHAR   | PRIMARY KEY (lat\|lon)         | Identificador espacial del punto   |
| lat / lon      | DOUBLE    | NOT NULL                       | Coordenadas del punto              |
| score_final    | DOUBLE    | NOT NULL, BETWEEN 0–100        | Score de confort cafetero          |
| dias_evaluados | INTEGER   | NOT NULL, > 0                  | Total días con datos               |
| dias_optimos   | INTEGER   | NOT NULL, >= 0                 | Días dentro de umbrales óptimos    |
| pct_optimos    | DOUBLE    | NOT NULL, BETWEEN 0–100        | Porcentaje días óptimos            |
| aggregated_at  | TIMESTAMP | NOT NULL                       | Timestamp de agregación            |

### GOLD — `estacionalidad_mensual.parquet`

| Columna               | Tipo      | Restricción                 | Descripción                           |
|-----------------------|-----------|-----------------------------|---------------------------------------|
| pk_estacional         | VARCHAR   | PRIMARY KEY (mes\|lat\|lon) | Identificador espacio-temporal        |
| mes                   | INTEGER   | NOT NULL, BETWEEN 1–12      | Mes del año                           |
| lat / lon             | DOUBLE    | NOT NULL                    | Coordenadas del punto                 |
| score_mensual_promedio| DOUBLE    | NOT NULL                    | Score promedio mensual                |
| variabilidad_score    | DOUBLE    | NOT NULL, >= 0              | Desviación estándar del score         |
| confort_pct           | DOUBLE    | NOT NULL, BETWEEN 0–100     | % días con confort óptimo en el mes   |
| aggregated_at         | TIMESTAMP | NOT NULL                    | Timestamp de agregación               |

---

## Umbrales agronómicos del café

| Variable    | Óptimo              | Fuente                        |
|-------------|---------------------|-------------------------------|
| Temperatura | 17 °C – 23 °C       | Federación Nacional Cafeteros |
| Humedad rel.| 70 % – 85 %         | CENICAFÉ                      |
| Lluvia      | ≥ 3 mm/día          | ERA5-Land / IDEAM             |
| Radiación   | ≥ 10 MJ/m²/día      | FAO Penman-Monteith           |

---

## Parámetros del pipeline

| Parámetro       | Valor                            |
|-----------------|----------------------------------|
| Bounding box    | lat [4.0, 6.0] / lon [-76.5, -74.5] |
| Período         | 2015 – 2024                      |
| Resolución GEE  | 0.1° (~11 km)                    |
| Compresión      | ZSTD (Parquet)                   |
| Tolerancia pérd.| ≤ 1% Bronze → Silver             |

---

## Estructura del repositorio

```
FisicaComputacional1/
├── main_v2.py                    # Orquestador principal
├── requirements.txt              # Dependencias con versiones fijadas
├── .env                          # Credenciales (no commitear)
├── credentials.json              # OAuth Drive (no commitear)
├── docs/
│   └── arquitectura.md           # Este archivo
├── src/
│   ├── contracts/
│   │   └── data_contracts.py     # Esquemas DuckDB PK/NOT NULL/CHECK
│   ├── ingest/
│   │   ├── downloadData.py       # GEE → Drive → Bronze Parquet
│   │   └── uuid_patch.py         # Inyección UUID a Bronze existente
│   ├── pipeline/
│   │   ├── silver_v2.py          # Transformaciones + particionado
│   │   └── gold_v2.py            # Agregaciones + PKs compuestas
│   ├── tests/
│   │   └── validate.py           # Validaciones automáticas del pipeline
│   └── utils/
│       ├── GoogleAutenticator.py
│       └── generadorMapa.py
└── data/                         # Ignorado en .gitignore
    ├── raw/
    │   ├── raw.parquet           # Bronze consolidado
    │   └── [CSVs por año]
    ├── silver/
    │   ├── clima_cafe_silver.parquet
    │   └── partitioned/
    └── gold/
        ├── ranking_puntos.parquet
        ├── estacionalidad_mensual.parquet
        └── estacionalidad_partitioned/
```

---

## Cómo ejecutar

```bash
# 1. Clonar y preparar entorno
git clone https://github.com/06-juan/FisicaComputacional1.git
cd FisicaComputacional1
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configurar credenciales
earthengine authenticate --project <tu-proyecto-gee>
# Copiar credentials.json a la raíz y crear .env

# 3. (Solo primera vez) Inyectar UUID al Bronze existente
python -m src.ingest.uuid_patch

# 4. Ejecutar pipeline completo
python main_v2.py
```

---

*Proyecto desarrollado para Ingeniería de Datos — Universidad del Quindío, 2026.*
