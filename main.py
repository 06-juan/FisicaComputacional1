from src.ingest.downloadData import procesar_raw
from src.pipeline.silver import procesar_silver
from src.pipeline.gold import procesar_gold
from src.utils.generadorMapa import generar_mapa_desde_gold

# descargamos y guardamos data en .parquet
procesar_raw(Descarga=False) # no descargamos para evitar configuracion damos .parquet de una

# hacemos el silver juntando tablas y calculando magnitudes
procesar_silver()

#creamos dos tablas del gold
procesar_gold()

#visualizamos los 5 mejores lugares para el café
generar_mapa_desde_gold()