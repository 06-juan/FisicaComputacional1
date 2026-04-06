import pandas as pd
import folium
from folium.plugins import FloatImage
import os

# Configuración de rutas
GOLD_RANKING = 'data/gold/ranking_puntos.parquet'
MAPA_OUTPUT = 'data/gold/mapa_interactivo_cultivos.html'

def generar_mapa_desde_gold():
    # 1. Verificación de seguridad
    if not os.path.exists(GOLD_RANKING):
        print(f"❌ Error: No se encontró el archivo {GOLD_RANKING}. Ejecuta primero el pipeline de DuckDB.")
        return

    # 2. Carga de datos (Lectura ultra rápida de Parquet)
    print(f"📖 Leyendo datos de excelencia desde {GOLD_RANKING}...")
    df = pd.read_parquet(GOLD_RANKING)

    # 3. Configuración del lienzo (Mapa)
    # Centramos en el promedio de las coordenadas de tus 5 mejores puntos
    lat_center = df['lat'].mean()
    lon_center = df['lon'].mean()
    
    m = folium.Map(
        location=[lat_center, lon_center],
        zoom_start=10,
        tiles='https://{s}.tile.thunderforest.com/outdoors/{z}/{x}/{y}.png?apikey=YOUR_API_KEY', # Opcional: Estilo topográfico
        attr='&copy; OpenStreetMap contributors'
    )
    
    # Si no tienes API Key, usa este estilo limpio y profesional por defecto:
    folium.TileLayer('CartoDB dark_matter', name="Modo Oscuro (Contraste)").add_to(m)
    folium.TileLayer('OpenStreetMap', name="Mapa Vial").add_to(m)

    # 4. Renderizado de los Puntos
    for i, row in df.iterrows():
        # Los primeros 5 puntos (índices 0, 1, 2, 3, 4) son verdes, el resto negros
        if i < 5:
            color_punto = '#27ae60'  # Verde
            opacidad = 0.9
        else:
            color_punto = '#000000'  # Negro
            opacidad = 0.6 # Un poco más transparente para no saturar
        
        # HTML personalizado para el Popup (Estilo "Dashboard")
        popup_html = f"""
        <div style="font-family: 'Arial', sans-serif; width: 160px;">
            <h4 style="margin-bottom:5px; color:#1e8449;">Punto Elite #{i+1}</h4>
            <hr style="margin:5px 0;">
            <b>Score Final:</b> {row['score_final']}<br>
            <b>Confort:</b> <span style="color:#27ae60;">{row['dias_optimos']}</span><br>
            <b>Lat:</b> {row['lat']:.4f}<br>
            <b>Lon:</b> {row['lon']:.4f}
        </div>
        """
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=15, 
            popup=folium.Popup(popup_html, max_width=200),
            color='white',
            weight=1,
            fill=True,
            fill_color=color_punto,
            fill_opacity=opacidad,
            tooltip=f"Punto {i+1}"
        ).add_to(m)

    # 5. Guardar y finalizar
    folium.LayerControl().add_to(m) # Permite cambiar entre modo oscuro y normal
    m.save(MAPA_OUTPUT)
    
    print(f"✨ ¡Listo! El mapa científico ha sido generado.")
    print(f"🔗 Abre este archivo en tu navegador: {os.path.abspath(MAPA_OUTPUT)}")

if __name__ == "__main__":
    generar_mapa_desde_gold()