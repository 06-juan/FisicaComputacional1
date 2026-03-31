# src/ingest/downloadData.py
"""
Capa RAW — Descarga ERA5-Land via Google Earth Engine
Pipeline: GEE → Google Drive → data/raw/*.csv → data/raw/raw.parquet

Partición anual — el Eje Cafetero tiene ~8 píxeles/día,
~2,900 features/año: GEE lo resuelve en ~4 segundos por task.
"""

import ee
import os
import time
import glob
import duckdb
from src.utils.drive_downloader import descargar_desde_drive

# ─────────────────────────────────────────
#  CONFIGURACIÓN CENTRAL
# ─────────────────────────────────────────
try:
    GEE_PROJECT  = os.getenv('GEE_PROJECT_ID')
    DRIVE_FOLDER = os.getenv('DRIVE_FOLDER_NAME')
except:
    print('revisa tu archivo .env hay problemas en tus credenciales')

RAW_PATH     = 'data/raw/'
SCALE_M      = 11132

BBOX = [-76.1, 4.0, -75.2, 5.6]  # Eje Cafetero

VARIABLES = [
    'temperature_2m',
    'total_precipitation_sum',
    'surface_solar_radiation_downwards_sum',
    'dewpoint_temperature_2m',
    'potential_evaporation_sum',
]

DATASET_ID = 'ECMWF/ERA5_LAND/DAILY_AGGR'
YEARS      = list(range(2015, 2025))


# ─────────────────────────────────────────
#  UTILIDADES
# ─────────────────────────────────────────

def preparar_entorno():
    os.makedirs(RAW_PATH, exist_ok=True)
    print(f"📁 Directorio RAW listo: {RAW_PATH}")


def inicializar_gee():
    try:
        ee.Initialize(project=GEE_PROJECT)
        print("✅ Google Earth Engine inicializado.")
    except Exception:
        print("⚠️  GEE no autenticado. Ejecuta:")
        print("      earthengine authenticate")
        raise


# ─────────────────────────────────────────
#  DESCARGA: 1 TASK POR AÑO
# ─────────────────────────────────────────

def lanzar_exports_gee(anios_faltantes):
    region = ee.Geometry.Rectangle(BBOX)
    tasks  = []

    for anio in anios_faltantes:
        nombre    = f'ERA5_EC_{anio}'
        fecha_ini = f'{anio}-01-01'
        fecha_fin = f'{anio}-12-31'
        patron    = os.path.join(RAW_PATH, f'{nombre}*.csv')

        if glob.glob(patron):
            print(f"⏩ Ya existe {nombre}.csv — saltando.")
            continue

        print(f"🚀 {nombre}  ({fecha_ini} → {fecha_fin})")

        coleccion = (
            ee.ImageCollection(DATASET_ID)
            .filterDate(fecha_ini, fecha_fin)
            .filterBounds(region)
            .select(VARIABLES)
        )

        def imagen_a_features(imagen):
            fecha = imagen.date().format('YYYY-MM-dd')
            fc = imagen.sample(
                region=region,
                scale=SCALE_M,
                geometries=True,
                dropNulls=True,
                tileScale=4,
            )
            def agregar_coords(f):
                coords = f.geometry().coordinates()
                return f.set({
                    'longitude': coords.get(0),
                    'latitude':  coords.get(1),
                    'date':      fecha,
                })
            return fc.map(agregar_coords)

        fc_total = coleccion.map(imagen_a_features).flatten()

        task = ee.batch.Export.table.toDrive(
            collection=fc_total,
            description=nombre,
            folder=DRIVE_FOLDER,
            fileNamePrefix=nombre,
            fileFormat='CSV',
            selectors=['date', 'longitude', 'latitude'] + VARIABLES,
        )
        task.start()
        tasks.append((nombre, task))
        print(f"   ✅ Task lanzada. ID: {task.id}")

    print(f"\n✅ {len(tasks)} tasks lanzadas.")
    return tasks


# ─────────────────────────────────────────
#  MONITOREO
# ─────────────────────────────────────────

def monitorear_tasks(tasks, intervalo_seg=30):
    if not tasks:
        print("ℹ️  No hay tasks nuevas que monitorear.")
        return

    print(f"\n⏳ Monitoreando {len(tasks)} tasks (polling cada {intervalo_seg}s)…")
    pendientes = list(tasks)

    while pendientes:
        time.sleep(intervalo_seg)
        aun_pendientes = []

        for nombre, task in pendientes:
            estado = task.status()['state']
            if estado == 'COMPLETED':
                print(f"   ✅ {nombre}")
            elif estado == 'FAILED':
                err = task.status().get('error_message', 'sin detalle')
                print(f"   ❌ {nombre} → {err}")
            else:
                aun_pendientes.append((nombre, task))

        if aun_pendientes:
            print(f"   ⏳ {len(aun_pendientes)} tasks pendientes…")
        pendientes = aun_pendientes

    print("🎉 Todas las tasks finalizaron.")


# ─────────────────────────────────────────
#  CONSOLIDAR CSVs → raw.parquet
# ─────────────────────────────────────────

def consolidar_a_raw_parquet(archivos_csv):
    parquet_final = os.path.join(RAW_PATH, 'raw.parquet')

    if os.path.exists(parquet_final):
        print(f"⏩ {parquet_final} ya existe. Saltando consolidación.")
        return

    print(f"\n📂 {len(archivos_csv)} CSVs encontrados. Consolidando…")

    archivos_str = ', '.join([f"'{f}'" for f in archivos_csv])
    con = duckdb.connect()

    con.execute(f"""
        COPY (
            SELECT
                date::DATE                                    AS fecha,
                longitude::DOUBLE                             AS longitud,
                latitude::DOUBLE                              AS latitud,
                temperature_2m::DOUBLE                        AS temp_k,
                dewpoint_temperature_2m::DOUBLE               AS dew_k,
                total_precipitation_sum::DOUBLE               AS lluvia_m,
                surface_solar_radiation_downwards_sum::DOUBLE AS rad_j_m2,
                potential_evaporation_sum::DOUBLE             AS evap_pot_m
            FROM read_csv_auto([{archivos_str}], union_by_name=True)
            ORDER BY fecha, latitud, longitud
        )
        TO '{parquet_final}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n = con.execute(f"SELECT COUNT(*) FROM '{parquet_final}'").fetchone()[0]
    print(f"✅ raw.parquet listo: {n:,} registros → {parquet_final}")


# ─────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────

def procesar_raw(forzar_descarga=False):
    preparar_entorno()
    
    # 1. Intentar obtener archivos locales
    csv_locales = glob.glob(os.path.join(RAW_PATH, 'ERA5_EC_*.csv'))
    
    # 2. Si no hay nada o pedimos refrescar, intentamos traer de Drive
    if not csv_locales or forzar_descarga:
        print("🔍 Sincronizando con Google Drive...")
        descargar_desde_drive()
        csv_locales = glob.glob(os.path.join(RAW_PATH, 'ERA5_EC_*.csv'))

    # 3. Si después de Drive seguimos sin archivos, GEE entra al rescate
    # IMPORTANTE: Solo lanzamos GEE para los años que FALTAN
    anios_faltantes = [y for y in YEARS if not any(f"ERA5_EC_{y}" in f for f in csv_locales)]
    
    if anios_faltantes:
        print(f"⚠️ Faltan datos para: {anios_faltantes}. Solicitando a GEE...")
        inicializar_gee()
        tasks = lanzar_exports_gee(anios_faltantes) # Ajustar función para recibir lista
        monitorear_tasks(tasks)
        
        # Después de que GEE termine, los archivos están en DRIVE, no en LOCAL.
        # Debemos descargar una última vez.
        print("📥 Descargando resultados recién generados de Drive...")
        descargar_desde_drive()
        csv_locales = glob.glob(os.path.join(RAW_PATH, 'ERA5_EC_*.csv'))

    # 4. Consolidación final
    if csv_locales:
        consolidar_a_raw_parquet(csv_locales)
    else:
        print("❌ Error crítico: No se pudieron obtener datos de ninguna fuente.")


if __name__ == '__main__':
    procesar_raw()