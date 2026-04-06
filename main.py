import os
from dotenv import load_dotenv
load_dotenv()  # DEBE ir antes de cualquier import de src

from src.utils.logger import PipelineLogger
from src.utils.GoogleAutenticator import autenticar_drive
from src.ingest.downloadData import procesar_raw
from src.pipeline.silver import procesar_silver
from src.pipeline.gold import procesar_gold
from src.utils.generadorMapa import generar_mapa_desde_gold
from src.utils.data_contracts import aplicar_contratos
from src.utils.validate import validar_pipeline

def main():
    logger = PipelineLogger()
    
    try:
        # 1. Autenticación
        with logger.step("Autenticación Google Drive"):
            service = autenticar_drive()
            logger.success("Acceso a Google Drive verificado.")

        # 2. Contratos de datos
        with logger.step("Aplicando contratos de datos"):
            aplicar_contratos()

        # 3. Capa RAW
        with logger.step("Procesando capa RAW"):
            procesar_raw(forzar_descarga=False)

        # 4. Capa SILVER
        with logger.step("Procesando capa SILVER"):
            procesar_silver()

        # 5. Capa GOLD
        with logger.step("Procesando capa GOLD"):
            procesar_gold()

        # 6. Validaciones
        with logger.step("Ejecutando validaciones"):
            ok = validar_pipeline()
            if not ok:
                raise RuntimeError("Pipeline completado con errores críticos de validación.")

        # 7. Mapa
        with logger.step("Generando mapa interactivo"):
            generar_mapa_desde_gold()

        logger.success("Proceso finalizado con éxito.")
        logger.finish()

    except Exception as e:
        logger.error(f"Error crítico en el pipeline: {e}")
        logger.finish()
        raise

if __name__ == "__main__":
    main()