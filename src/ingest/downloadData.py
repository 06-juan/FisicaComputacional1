# src/ingest/downloadData.py
"""
Capa RAW — Descarga ERA5-Land via Google Earth Engine
Pipeline: GEE → Google Drive → data/raw/*.csv → data/raw/raw.parquet

Prerequisitos (una sola vez):
    pip install earthengine-api geemap duckdb pandas
    earthengine authenticate
"""

import ee
import os
import time
import glob
import duckdb
import pandas as pd

# ─────────────────────────────────────────
#  CONFIGURACIÓN CENTRAL
# ─────────────────────────────────────────

GEE_PROJECT = 'ingenieriadatos1'   # ← pon tu ID acá arriba, junto a las otras constantes

RAW_PATH        = 'data/raw/'
DRIVE_FOLDER    = 'ERA5_Colombia_RAW'     # Carpeta que se crea en tu Google Drive
DATE_START      = '2000-01-01'
DATE_END        = '2024-12-31'
SCALE_M         = 11132                   # Resolución nativa ERA5-Land (~11 km)

# Bounding box Colombia completa [W, S, E, N]
# Cambia a [−76.1, 4.0, −75.2, 5.6] si solo quieres el Eje Cafetero
BBOX = [-79.0, -4.3, -66.8, 12.5]

# Variables ERA5-Land a descargar (nombres exactos del catálogo GEE)
# temperature_2m        → Kelvin  (réstale 273.15 en Silver para °C)
# surface_pressure      → Pascales
# total_precipitation_sum → metros/día  (multiplica ×1000 para mm/día en Silver)
VARIABLES = [
    'temperature_2m',
    'surface_pressure',
    'total_precipitation_sum',
]

# ERA5-Land diario en GEE está disponible desde 1950 hasta ~3 meses atrás
DATASET_ID = 'ECMWF/ERA5_LAND/DAILY_AGGR'


# ─────────────────────────────────────────
#  UTILIDADES
# ─────────────────────────────────────────

def preparar_entorno():
    os.makedirs(RAW_PATH, exist_ok=True)
    print(f"📁 Directorio RAW listo: {RAW_PATH}")

def inicializar_gee():
    try:
        ee.Initialize(project=GEE_PROJECT)   # ← único cambio
        print("✅ Google Earth Engine inicializado.")
    except Exception as e:
        print("⚠️  GEE no autenticado. Ejecuta en terminal:")
        print("      earthengine authenticate")
        raise


# ─────────────────────────────────────────
#  DESCARGA: LANZAR EXPORT TASKS EN GEE
# ─────────────────────────────────────────

def lanzar_exports_gee():
    """
    Parte el rango 2000-2024 en bloques de 5 años para evitar timeouts
    y lanza un Export.table.toDrive por bloque.
    Retorna lista de tasks activas.
    """
    region = ee.Geometry.Rectangle(BBOX)

    # Bloques de 5 años → 5 tasks en paralelo
    bloques = [
        ('2000-01-01', '2004-12-31', 'ERA5_Colombia_2000_2004'),
        ('2005-01-01', '2009-12-31', 'ERA5_Colombia_2005_2009'),
        ('2010-01-01', '2014-12-31', 'ERA5_Colombia_2010_2014'),
        ('2015-01-01', '2019-12-31', 'ERA5_Colombia_2015_2019'),
        ('2020-01-01', '2024-12-31', 'ERA5_Colombia_2020_2024'),
    ]

    tasks = []
    for fecha_ini, fecha_fin, nombre in bloques:

        # Verificar si ya existe el CSV final en RAW_PATH
        patron = os.path.join(RAW_PATH, f"{nombre}*.csv")
        if glob.glob(patron):
            print(f"⏩ Ya existe {nombre}. Saltando...")
            continue

        print(f"🚀 Lanzando export: {nombre}  ({fecha_ini} → {fecha_fin})")

        coleccion = (
            ee.ImageCollection(DATASET_ID)
            .filterDate(fecha_ini, fecha_fin)
            .filterBounds(region)
            .select(VARIABLES)
        )

        # Convertimos la ImageCollection a FeatureCollection
        # addBands con la fecha como propiedad de cada pixel
        def imagen_a_features(imagen):
            fecha = imagen.date().format('YYYY-MM-dd')
            fc = imagen.sample(
                region=region,
                scale=SCALE_M,
                geometries=True,          # incluye lat/lon de cada pixel
                dropNulls=True,
            )
            return fc.map(lambda f: f.set('date', fecha))

        fc_total = coleccion.map(imagen_a_features).flatten()

        task = ee.batch.Export.table.toDrive(
            collection=fc_total,
            description=nombre,
            folder=DRIVE_FOLDER,
            fileNamePrefix=nombre,
            fileFormat='CSV',
            selectors=['date', 'longitude', 'latitude']
                      + VARIABLES,        # columnas ordenadas
        )
        task.start()
        tasks.append((nombre, task))
        print(f"   ✅ Task lanzada. ID: {task.id}")

    return tasks


# ─────────────────────────────────────────
#  MONITOREO DE TASKS
# ─────────────────────────────────────────

def monitorear_tasks(tasks, intervalo_seg=60):
    """
    Polling hasta que todas las tasks terminen o fallen.
    """
    if not tasks:
        print("ℹ️  No hay tasks nuevas que monitorear.")
        return

    print(f"\n⏳ Monitoreando {len(tasks)} tasks (polling cada {intervalo_seg}s)...")
    pendientes = list(tasks)

    while pendientes:
        time.sleep(intervalo_seg)
        aun_pendientes = []

        for nombre, task in pendientes:
            estado = task.status()['state']

            if estado == 'COMPLETED':
                print(f"   ✅ COMPLETADA: {nombre}")
            elif estado == 'FAILED':
                error = task.status().get('error_message', 'sin detalle')
                print(f"   ❌ FALLIDA: {nombre} → {error}")
            else:
                print(f"   ⏳ {nombre}: {estado}")
                aun_pendientes.append((nombre, task))

        pendientes = aun_pendientes

    print("\n🎉 Todas las tasks finalizaron.")


# ─────────────────────────────────────────
#  CONSOLIDAR CSVs → raw.parquet
# ─────────────────────────────────────────

def consolidar_a_raw_parquet():
    """
    Lee todos los CSVs descargados de Drive (que debes copiar a data/raw/)
    y los consolida en un único raw.parquet vía DuckDB.
    """
    parquet_final = os.path.join(RAW_PATH, 'raw.parquet')

    if os.path.exists(parquet_final):
        print(f"⏩ {parquet_final} ya existe. Saltando consolidación.")
        return

    csv_files = glob.glob(os.path.join(RAW_PATH, 'ERA5_Colombia_*.csv'))

    if not csv_files:
        print("⚠️  No se encontraron CSVs en data/raw/.")
        print("    Descarga los archivos de Google Drive y cópialos ahí.")
        return

    print(f"\n📂 {len(csv_files)} archivos CSV encontrados. Consolidando...")

    con = duckdb.connect()

    # DuckDB puede leer y unir múltiples CSVs en una sola query
    archivos_str = ', '.join([f"'{f}'" for f in csv_files])

    con.execute(f"""
        COPY (
            SELECT
                date::DATE                              AS fecha,
                longitude::DOUBLE                       AS longitud,
                latitude::DOUBLE                        AS latitud,
                temperature_2m::DOUBLE                  AS temperatura_2m_k,
                surface_pressure::DOUBLE                AS presion_superficie_pa,
                total_precipitation_sum::DOUBLE         AS precipitacion_total_m
            FROM read_csv_auto([{archivos_str}], union_by_name=True)
            ORDER BY fecha, latitud, longitud
        )
        TO '{parquet_final}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Reporte rápido
    stats = con.execute(f"""
        SELECT
            COUNT(*)                        AS total_registros,
            MIN(fecha)                      AS fecha_min,
            MAX(fecha)                      AS fecha_max,
            COUNT(DISTINCT fecha)           AS dias_unicos,
            COUNT(DISTINCT longitud || ',' || latitud::VARCHAR) AS pixeles_unicos
        FROM '{parquet_final}'
    """).fetchdf()

    print("\n─── REPORTE RAW ─────────────────────────────")
    print(f"  Total registros : {stats['total_registros'].iloc[0]:,}")
    print(f"  Rango temporal  : {stats['fecha_min'].iloc[0]}  →  {stats['fecha_max'].iloc[0]}")
    print(f"  Días únicos     : {stats['dias_unicos'].iloc[0]:,}")
    print(f"  Píxeles únicos  : {stats['pixeles_unicos'].iloc[0]:,}")
    print(f"  Guardado en     : {parquet_final}")
    print("─────────────────────────────────────────────")


# ─────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────

def procesar_raw():
    preparar_entorno()
    inicializar_gee()

    # Paso 1: lanzar exports en GEE (corren en la nube, no en tu PC)
    tasks = lanzar_exports_gee()

    # Paso 2: esperar a que terminen
    monitorear_tasks(tasks, intervalo_seg=60)

    # Paso 3: INSTRUCCIONES para el usuario
    print("\n" + "═"*55)
    print("  ACCIÓN MANUAL REQUERIDA")
    print("═"*55)
    print(f"  1. Abre Google Drive")
    print(f"  2. Entra a la carpeta: {DRIVE_FOLDER}")
    print(f"  3. Descarga todos los archivos ERA5_Colombia_*.csv")
    print(f"  4. Cópialos a: {os.path.abspath(RAW_PATH)}")
    print(f"  5. Vuelve a correr este script")
    print("═"*55 + "\n")

    # Paso 4: consolidar si ya están los CSVs
    consolidar_a_raw_parquet()


if __name__ == "__main__":
    procesar_raw()