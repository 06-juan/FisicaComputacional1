from src.ingest.downloadData import procesar_raw
from src.pipeline.silver import procesar_silver
from src.pipeline.gold import procesar_gold

# descargamos y guardamos data en .parquet
procesar_raw()

# hacemos el silver juntando tablas y calculando magnitudes
procesar_silver()

#creamos dos tablas del gold
procesar_gold()