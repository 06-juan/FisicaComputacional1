import cdsapi
import xarray as xr
import pandas as pd
import zipfile
import shutil
import duckdb
import os

# Rutas
RAW_PATH = 'data/raw/'
FILE_PRESSURE = os.path.join(RAW_PATH, 'pressure_levels_raw.nc')
FILE_LAND = os.path.join(RAW_PATH, 'era5_land_raw.nc')

def preparar_entorno():
    os.makedirs(RAW_PATH, exist_ok=True)

def descargar_datos():
    c = cdsapi.Client()
    area = [5.6, -76.1, 4.0, -75.2]
    # Bajamos solo 2024 y 2025 para asegurar éxito
    anios = [str(year) for year in range(2000, 2025)]
    meses = [f"{m:02d}" for m in range(1, 13)]
    dias  = [f"{m:02d}" for m in range(1, 2)] # 1 dia para evitar error: Your request is too large, please reduce your selection.
                                              # podria hacerse en varias consultas pequeñas

    print("--- Descargando Pressure Levels ---")
    c.retrieve('reanalysis-era5-pressure-levels', {
        'variable': ['temperature', 'u_component_of_wind', 'v_component_of_wind'],
        'pressure_level': '1000',
        'product_type': 'reanalysis',
        'year': anios,
        'month': meses,
        'day': dias,
        'time': '12:00',
        'area': area,
        'format': 'netcdf',
    }, FILE_PRESSURE)

    print("--- Descargando ERA5-Land ---")
    c.retrieve('reanalysis-era5-land', {
        'variable': ['2m_temperature', 'surface_pressure', 'total_precipitation'],
        'year': anios,
        'month': meses,
        'day': dias,
        'time': '12:00',
        'area': area,
        'format': 'netcdf',
    }, FILE_LAND)

def descomprimir_si_zip(filepath):
    """Si el archivo es un ZIP, extrae el .nc que contiene y lo reemplaza."""
    with open(filepath, 'rb') as f:
        magic = f.read(4)
    
    if magic == b'PK\x03\x04':  # Firma ZIP
        print(f"  {os.path.basename(filepath)} es un ZIP — descomprimiendo...")
        extract_dir = filepath + '_unzipped'
        with zipfile.ZipFile(filepath, 'r') as z:
            z.extractall(extract_dir)
        
        # Busca el primer .nc dentro
        nc_files = [f for f in os.listdir(extract_dir) if f.endswith('.nc')]
        if not nc_files:
            raise FileNotFoundError(f"No se encontró ningún .nc dentro del ZIP: {filepath}")
        
        nc_inside = os.path.join(extract_dir, nc_files[0])
        shutil.move(nc_inside, filepath)
        shutil.rmtree(extract_dir)                # Limpia carpeta temporal
        print(f"  Extraído: {nc_files[0]}")

def detectar_engine(filepath):
    with open(filepath, 'rb') as f:
        magic = f.read(8)
    
    if magic[:4] == b'\x89HDF':
        return 'h5netcdf'
    elif magic[:3] == b'CDF':
        return 'scipy'
    elif magic[:4] == b'GRIB':
        return 'cfgrib'
    else:
        return 'h5netcdf'

def consolidar_a_bronze_parquet():
    print("Consolidando NetCDF a Parquet (Capa Bronze)...")
    try:
        # Descomprimir si vienen como ZIP
        descomprimir_si_zip(FILE_PRESSURE)
        descomprimir_si_zip(FILE_LAND)

        engine1 = detectar_engine(FILE_PRESSURE)
        engine2 = detectar_engine(FILE_LAND)
        print(f"  pressure → engine: {engine1}")
        print(f"  land     → engine: {engine2}")

        # 1. Abrir con Xarray (usando los motores que ya probamos)
        ds_p = xr.open_dataset(FILE_PRESSURE, engine=engine1).squeeze()
        ds_l = xr.open_dataset(FILE_LAND, engine=engine2,).squeeze()

        print("Archivos abiertos. Transformando a DataFrame...")

        # 2. Convertir a DataFrames (paso intermedio en memoria)
        df_p = ds_p.to_dataframe().reset_index()
        df_l = ds_l.to_dataframe().reset_index()

        # 3. Usar DuckDB para guardar como Parquet (altamente comprimido)
        con = duckdb.connect()
        con.execute(f"COPY df_p TO '{os.path.join(RAW_PATH, 'pressure.parquet')}' (FORMAT PARQUET)")
        con.execute(f"COPY df_l TO '{os.path.join(RAW_PATH, 'land.parquet')}' (FORMAT PARQUET)")
        
        print("Capa Bronze lista en formato Parquet.")
    except Exception as e:
        print(f"Error en Ingest: {e}")

def procesar_raw():
    preparar_entorno()
    descargar_datos() 
    consolidar_a_bronze_parquet()

if __name__ == "__main__":
    procesar_raw()