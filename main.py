import os
from dotenv import load_dotenv
load_dotenv()  # DEBE ir antes de cualquier import de src

from src.utils.GoogleAutenticator import autenticar_drive
from src.ingest.downloadData      import procesar_raw
from src.pipeline.silver       import procesar_silver
from src.pipeline.gold         import procesar_gold
from src.utils.generadorMapa      import generar_mapa_desde_gold
from src.utils.data_contracts import aplicar_contratos
from src.utils.validate           import validar_pipeline


def main():
    print("🚀 Iniciando Pipeline de Datos Climáticos - Eje Cafetero")
    try:
        # 1. Autenticación
        service = autenticar_drive()
        print("🔑 Acceso a Google Drive verificado.")

        # 2. Contratos de datos (idempotente)
        print("\n--- [CONTRATOS] ---")
        aplicar_contratos()

        # 3. Capa RAW
        print("\n--- [PASO 1: CAPA RAW] ---")
        procesar_raw(forzar_descarga=False)

        # 4. Capa SILVER
        print("\n--- [PASO 2: CAPA SILVER] ---")
        procesar_silver()

        # 5. Capa GOLD
        print("\n--- [PASO 3: CAPA GOLD] ---")
        procesar_gold()

        # 6. Validaciones automáticas
        print("\n--- [VALIDACIONES] ---")
        ok = validar_pipeline()
        if not ok:
            raise RuntimeError("Pipeline completado con errores críticos de validación.")

        # 7. Visualización
        print("\n--- [PASO 4: GENERANDO MAPA] ---")
        generar_mapa_desde_gold()

        print("\n✨ Proceso finalizado con éxito.")

    except Exception as e:
        print(f"\n❌ Error crítico en el pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
