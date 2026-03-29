# 📓 Log de Decisiones Técnicas - Pipeline Climático Eje Cafetero

Este documento registra las decisiones críticas tomadas durante el desarrollo del pipeline de datos (Arquitectura Medallion), su justificación física y técnica, y las soluciones a los cuellos de botella encontrados.

# 28/03/2026 Juan  Jose
---

## 1. Gestión de Ingesta: Descarga por Lotes (Batching)
**Problema:** El servidor de Copernicus (CDS API) rechazaba las solicitudes de 20+ años por exceder los límites de tamaño ("Request too large") y presentaba tiempos de espera elevados.

**Decisión:** * Implementar un sistema de iteración por bloques de 2 a 5 años en la capa **Bronze**.
* Utilizar `xarray.open_mfdataset` para unificar los fragmentos `.nc` de forma transparente mediante *lazy loading*.

**Justificación:** Evita el tiempo de inactividad por errores de servidor y permite la **idempotencia**: si la descarga se interrumpe, el pipeline puede retomar desde el último bloque exitoso sin duplicar trabajo.

---

## 2. Normalización Temporal: Ajuste a Hora Local (UTC-5)
**Problema:** Los datos crudos en UTC mostraban temperaturas mínimas al "mediodía" (12:00 UTC = 07:00 AM Armenia), lo que generaba confusión en el análisis de ciclos diurnos.

**Decisión:** * Desplazar el eje temporal en la capa **Silver** utilizando un `INTERVAL -5 HOURS`.
* Renombrar la columna a `hora_local`.

**Justificación:** El Eje Cafetero se rige por el huso horario **UTC-5**. Para que los KPIs de la capa **Gold** (como la temperatura máxima diaria) sean físicamente coherentes con la realidad local y agrícola, el dato debe estar alineado con la posición del sol en la región.

---

## 3. Integración de Datasets: Resolución y Mallas (Grid Matching)
**Problema:** ERA5-Land ($0.1^\circ \approx 9\text{ km}$) y ERA5-Pressure ($0.25^\circ \approx 31\text{ km}$) tienen resoluciones espaciales distintas. Un `INNER JOIN` directo por coordenadas exactas resultaba en una pérdida masiva de datos (>80%).

**Decisión:** * **Redondeo Estratégico:** Redondear las latitudes y longitudes de ambos datasets a 1 decimal en una tabla temporal (`lat_join`, `lon_join`).
* **Join por Proximidad:** Realizar la unión mediante un margen de tolerancia (`ABS(diff) < 0.01`).

**Justificación:** Desde la física, esto equivale a una **interpolación del vecino más cercano**. Dado que el viento (Pressure Levels) varía más lentamente en el espacio que la temperatura de superficie (Land), es válido asignar el vector de viento del píxel de $31\text{ km}$ a los sub-píxeles de $9\text{ km}$ que lo componen.

---

## 4. Optimización de Almacenamiento: Formato Parquet
**Problema:** Los archivos NetCDF y CSV ocupaban demasiado espacio en disco y eran lentos para consultas analíticas.

**Decisión:** * Migrar todas las capas (Bronze, Silver, Gold) a formato **Apache Parquet** gestionado por **DuckDB**.

**Justificación:** El almacenamiento columnar de Parquet reduce el peso de los datos en un ~80% y permite que DuckDB ejecute el `JOIN` y las conversiones físicas (Kelvin a Celsius) en milisegundos, optimizando el uso de la CPU en el entorno local de desarrollo.

---

**Estado del Proyecto:** Capa Silver estable y normalizada.
**Próximo Hito:** Generación de KPIs mensuales de anomalías térmicas en Capa Gold.


## 5. Correccion de anomalias: 
**Problema:** Se detectaron anomalías en los límites temporales del dataset (primer y último día) debido al desfase de zona horaria (UTC-5). Para evitar sesgos en el cálculo de la amplitud térmica.

**Decisión:** se implementó una regla de integridad en la capa Gold que descarta cualquier unidad espacial-temporal que no cuente con el par de mediciones día/noche (00h y 12h local).

**Justificación:** ($\Delta T = 0$ por falta de muestras), 