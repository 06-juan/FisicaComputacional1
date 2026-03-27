from ingest.downloadData import procesar_raw
from pipeline.silver import procesar_silver

# descargamos y guardamos data en .parquet
procesar_raw()

# hacemos el silver juntando tablas y calculando magnitudes
procesar_silver()