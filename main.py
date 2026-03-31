import os
from dotenv import load_dotenv
from src.utils.GoogleAutenticator import autenticar_drive
from src.ingest.downloadData import procesar_raw
from src.pipeline.silver import procesar_silver
from src.pipeline.gold import procesar_gold
from src.utils.generadorMapa import generar_mapa_desde_gold

# 1. Cargar variables de entorno (.env)
load_dotenv()

def main():
    print("🚀 Iniciando Pipeline de Datos Climáticos - Eje Cafetero")
    
    try:
        # 2. Autenticación (Paso crítico)
        # Esto genera el token.json automáticamente si no existe
        service = autenticar_drive()
        print("🔑 Acceso a Google Drive verificado.")

        # 3. Capa RAW
        # Si Descarga=True, usará el 'service' que acabamos de crear
        print("\n--- [PASO 1: CAPA RAW] ---")
        procesar_raw(Descarga=False) 

        # 4. Capa SILVER
        print("\n--- [PASO 2: CAPA SILVER] ---")
        procesar_silver()

        # 5. Capa GOLD
        print("\n--- [PASO 3: CAPA GOLD] ---")
        procesar_gold()

        # 6. Visualización
        print("\n--- [PASO 4: GENERANDO MAPA] ---")
        generar_mapa_desde_gold()
        
        print("\n✨ Proceso finalizado con éxito.")

    except Exception as e:
        print(f"\n❌ Error crítico en el pipeline: {e}")

if __name__ == "__main__":
    main()