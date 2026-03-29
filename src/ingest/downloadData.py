import cdsapi
import xarray as xr
import zipfile
import shutil
import duckdb
import glob
import os

# Rutas
RAW_PATH = 'data/raw/'

def preparar_entorno():
    os.makedirs(RAW_PATH, exist_ok=True)

def descargar_datos():
    c = cdsapi.Client()
    area = [5.6, -76.1, 4.0, -75.2]
    
    # Colombia es UTC-5. 
    # Para tener 00:00 local -> Pedimos 05:00 UTC
    # Para tener 12:00 local -> Pedimos 17:00 UTC
    horas_utc = ['05:00', '17:00']

    anios = range(2024, 2026)
    meses = [f"{m:02d}" for m in range(1, 13)]
    dias =  [f"{d:02d}" for d in range(1, 32)]
    bloques = [anios[i:i+2] for i in range(0, len(anios), 2)]
    
    for bloque in bloques:
        anios_str = [str(a) for a in bloque]
        nombre_p = os.path.join(RAW_PATH, f'pressure_{anios_str[0]}_{anios_str[-1]}.nc')
        nombre_l = os.path.join(RAW_PATH, f'land_{anios_str[0]}_{anios_str[-1]}.nc')

        if os.path.exists(nombre_p) and os.path.exists(nombre_l):
            print(f"⏩ Saltando bloque {anios_str[0]}-{anios_str[-1]} (ya descargado).")
            continue
        
        print(f"--- Descargando bloque: {anios_str[0]}-{anios_str[-1]} ---")
        c.retrieve('reanalysis-era5-pressure-levels', {
            'variable': ['temperature', 'u_component_of_wind', 'v_component_of_wind'],
            'pressure_level': '1000',
            'product_type': 'reanalysis',
            'year': anios_str,
            'month': meses,
            'day': dias,
            'time': horas_utc,
            'area': area,
            'format': 'netcdf',
        }, nombre_p)

        print("--- Descargando ERA5-Land ---")
        c.retrieve('reanalysis-era5-land', {
            'variable': ['2m_temperature', 'surface_pressure', 'total_precipitation'],
            'year': anios_str,
            'month': meses,
            'day': dias,
            'time': horas_utc,
            'area': area,
            'format': 'netcdf'
        }, nombre_l)
    
def descomprimir_todos_los_zips():
    """Busca archivos .nc que en realidad son ZIP y los extrae."""
    zips = glob.glob(os.path.join(RAW_PATH, "*.nc")) + glob.glob(os.path.join(RAW_PATH, "*.zip"))
    
    for filepath in zips:
        try:
            with open(filepath, 'rb') as f:
                magic = f.read(4)
            
            if magic == b'PK\x03\x04':  # Firma estándar de un archivo ZIP
                print(f"📦 Descomprimiendo: {os.path.basename(filepath)}...")
                extract_dir = filepath + "_tmp"
                with zipfile.ZipFile(filepath, 'r') as z:
                    z.extractall(extract_dir)
                
                # Buscamos el archivo .nc real que estaba dentro
                nc_internos = glob.glob(os.path.join(extract_dir, "*.nc"))
                if nc_internos:
                    # Sobrescribimos el original con el contenido real
                    shutil.move(nc_internos[0], filepath)
                    print(f"   ✅ Extraído con éxito.")
                
                shutil.rmtree(extract_dir)
        except Exception as e:
            print(f"   ⚠️ No se pudo procesar como ZIP: {filepath} ({e})")

def consolidar_a_bronze_parquet():
    print("\n--- Iniciando Consolidación Capa Bronze ---")
    os.makedirs(RAW_PATH, exist_ok=True)
    
    # 1. Limpieza previa de archivos ZIP
    descomprimir_todos_los_zips()

    try:
        # 2. Identificar todos los fragmentos (bloques de años)
        # Suponiendo que tus archivos se llaman 'pressure_2000_2005.nc', etc.
        files_p = glob.glob(os.path.join(RAW_PATH, "pressure_*.nc"))
        files_l = glob.glob(os.path.join(RAW_PATH, "land_*.nc"))

        if not files_p or not files_l:
            raise FileNotFoundError("No se encontraron archivos .nc para consolidar.")

        print(f"📂 Fragmentos encontrados: {len(files_p)} de Pressure, {len(files_l)} de Land.")

        # 3. Carga Multi-Archivo con Xarray
        # 'combine=by_coords' une los años automáticamente siguiendo la línea de tiempo
        print("🔗 Uniendo fragmentos temporales...")
        ds_p = xr.open_mfdataset(files_p, combine='by_coords', engine='h5netcdf').squeeze()
        ds_l = xr.open_mfdataset(files_l, combine='by_coords', engine='h5netcdf').squeeze()

        # 4. Transformación a DataFrame
        print("🚀 Convirtiendo a DataFrames (esto puede tardar según el volumen)...")
        df_p = ds_p.to_dataframe().reset_index()
        df_l = ds_l.to_dataframe().reset_index()

        # 5. Volcado a Parquet usando DuckDB (Máxima compresión y velocidad)
        con = duckdb.connect()
        path_p = os.path.join(RAW_PATH, 'pressure_bronze.parquet')
        path_l = os.path.join(RAW_PATH, 'land_bronze.parquet')

        print("💾 Guardando en formato Parquet...")
        con.execute(f"COPY df_p TO '{path_p}' (FORMAT PARQUET)")
        con.execute(f"COPY df_l TO '{path_l}' (FORMAT PARQUET)")

        print(f"\n--- REPORTE BRONZE ---")
        print(f"✅ Pressure Bronze: {len(df_p)} registros")
        print(f"✅ Land Bronze: {len(df_l)} registros")
        print(f"📍 Archivos listos en: {RAW_PATH}")

    except Exception as e:
        print(f"❌ Error crítico en la consolidación: {e}")

def procesar_raw():
    preparar_entorno()
    descargar_datos() 
    consolidar_a_bronze_parquet()

if __name__ == "__main__":
    procesar_raw()