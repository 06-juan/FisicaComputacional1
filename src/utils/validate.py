"""
Validaciones Automáticas del Pipeline — Arquitectura Medallón
"""

import duckdb
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


@dataclass
class ResultadoValidacion:
    check: str
    capa: str
    paso: bool
    detalle: str = ""
    critico: bool = True


class ValidadorPipeline:
    def __init__(self, parquet_bronze, parquet_silver,
                 parquet_gold_ranking, parquet_gold_estacional):
        self.bronze  = parquet_bronze
        self.silver  = parquet_silver
        self.g_rank  = parquet_gold_ranking
        self.g_estac = parquet_gold_estacional
        self.resultados: List[ResultadoValidacion] = []

    def _q(self, con, sql):
        return con.execute(sql).fetchone()[0]

    def _registrar(self, check, capa, paso, detalle="", critico=True):
        r = ResultadoValidacion(check, capa, paso, detalle, critico)
        self.resultados.append(r)
        icono = "✅" if paso else ("❌" if critico else "⚠️")
        log.info("%s [%s] %s — %s", icono, capa, check, detalle or "OK")

    def _columnas(self, con, path) -> list:
        """Devuelve la lista de columnas reales del parquet."""
        return [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 1"
        ).fetchall()]

    # ── 1. ROW COUNT ──────────────────────────────────────────────
    def check_row_counts(self, con):
        for etiqueta, path in [
            ("BRONZE",          self.bronze),
            ("SILVER",          self.silver),
            ("GOLD_RANKING",    self.g_rank),
            ("GOLD_ESTACIONAL", self.g_estac),
        ]:
            if not Path(path).exists():
                self._registrar("row_count", etiqueta, False,
                                f"Archivo no encontrado: {path}")
                continue
            n = self._q(con, f"SELECT COUNT(*) FROM read_parquet('{path}')")
            self._registrar("row_count", etiqueta, n > 0, f"{n:,} filas")

    # ── 2. UNICIDAD DE PK ─────────────────────────────────────────
    def check_pk_unicidad(self, con):
        # Bronze: PK compuesta (fecha + latitud + longitud) — no tiene row_id
        if Path(self.bronze).exists():
            cols = self._columnas(con, self.bronze)
            lat_col  = "latitud"  if "latitud"  in cols else "lat"
            lon_col  = "longitud" if "longitud" in cols else "lon"
            fecha_col = next((c for c in cols if "fecha" in c.lower()), "fecha")
            try:
                dupes = self._q(con, f"""
                    SELECT COUNT(*) FROM (
                        SELECT {fecha_col}, {lat_col}, {lon_col}, COUNT(*) c
                        FROM read_parquet('{self.bronze}')
                        GROUP BY {fecha_col}, {lat_col}, {lon_col}
                        HAVING c > 1
                    )
                """)
                self._registrar("pk_unicidad", "BRONZE",
                                dupes == 0,
                                f"{dupes} duplicados (fecha+lat+lon)" if dupes else "PK compuesta única")
            except Exception as e:
                self._registrar("pk_unicidad", "BRONZE", False, str(e))

        # Silver, Gold: PK serializada
        for etiqueta, path, pk_col in [
            ("SILVER",   self.silver,  "pk_silver"),
            ("GOLD_RK",  self.g_rank,  "pk_gold"),
            ("GOLD_ES",  self.g_estac, "pk_estacional"),
        ]:
            if not Path(path).exists():
                continue
            try:
                dupes = self._q(con, f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk_col}, COUNT(*) c
                        FROM read_parquet('{path}')
                        GROUP BY {pk_col} HAVING c > 1
                    )
                """)
                self._registrar("pk_unicidad", etiqueta, dupes == 0,
                                f"{dupes} PK duplicadas" if dupes else "PK única")
            except Exception as e:
                self._registrar("pk_unicidad", etiqueta, False,
                                f"Columna PK no encontrada: {e}")

    # ── 3. NULL CHECK ─────────────────────────────────────────────
    def check_no_nulls(self, con):
        # Bronze: detectar nombres reales de columnas
        if Path(self.bronze).exists():
            cols = self._columnas(con, self.bronze)
            lat_col   = "latitud"  if "latitud"  in cols else "lat"
            lon_col   = "longitud" if "longitud" in cols else "lon"
            fecha_col = next((c for c in cols if "fecha" in c.lower()), "fecha")
            bronze_criticos = [fecha_col, lat_col, lon_col, "temp_k", "dew_k", "lluvia_m"]
            for col in bronze_criticos:
                if col not in cols:
                    continue
                n = self._q(con,
                    f"SELECT COUNT(*) FROM read_parquet('{self.bronze}') WHERE {col} IS NULL")
                self._registrar("no_nulls", f"BRONZE.{col}", n == 0,
                                f"{n} nulos" if n else "sin nulos")

        # Silver: columnas ya normalizadas
        if Path(self.silver).exists():
            for col in ["fecha", "lat", "lon", "temp_c", "rh_pct", "rain_mm"]:
                n = self._q(con,
                    f"SELECT COUNT(*) FROM read_parquet('{self.silver}') WHERE {col} IS NULL")
                self._registrar("no_nulls", f"SILVER.{col}", n == 0,
                                f"{n} nulos" if n else "sin nulos")

    # ── 4. CONSISTENCIA Bronze → Silver ───────────────────────────
    def check_row_consistency(self, con, tolerancia_pct=1.0):
        if not Path(self.bronze).exists() or not Path(self.silver).exists():
            return
        n_b = self._q(con, f"SELECT COUNT(*) FROM read_parquet('{self.bronze}')")
        n_s = self._q(con, f"SELECT COUNT(*) FROM read_parquet('{self.silver}')")
        if n_b == 0:
            self._registrar("row_consistency", "BRONZE→SILVER", False, "Bronze vacío")
            return
        perdida = abs(n_b - n_s) / n_b * 100
        self._registrar("row_consistency", "BRONZE→SILVER", perdida <= tolerancia_pct,
                        f"Bronze={n_b:,} | Silver={n_s:,} | pérdida={perdida:.2f}%",
                        critico=False)

    # ── 5. RANGOS FÍSICOS ─────────────────────────────────────────
    def check_rangos_fisicos(self, con):
        if not Path(self.silver).exists():
            return
        rangos = {
            "temp_c":    "temp_c    NOT BETWEEN -80 AND 60",
            "rh_pct":    "rh_pct    NOT BETWEEN 0   AND 100",
            "rain_mm":   "rain_mm   < 0",
            "rad_mj_m2": "rad_mj_m2 < 0",
        }
        for campo, condicion in rangos.items():
            n = self._q(con,
                f"SELECT COUNT(*) FROM read_parquet('{self.silver}') WHERE {condicion}")
            self._registrar("rango_fisico", f"SILVER.{campo}", n == 0,
                            f"{n} valores fuera de rango" if n else "rango OK")

    # ── EJECUTAR TODOS ────────────────────────────────────────────
    def ejecutar(self) -> bool:
        con = duckdb.connect()
        try:
            self.check_row_counts(con)
            self.check_pk_unicidad(con)
            self.check_no_nulls(con)
            self.check_row_consistency(con)
            self.check_rangos_fisicos(con)
        finally:
            con.close()
        return self.resumen()

    def resumen(self) -> bool:
        total    = len(self.resultados)
        fallidos = [r for r in self.resultados if not r.paso]
        criticos = [r for r in fallidos if r.critico]
        log.info("─" * 60)
        log.info("RESUMEN: %d checks | %d fallidos | %d críticos",
                 total, len(fallidos), len(criticos))
        for r in fallidos:
            nivel = "CRÍTICO" if r.critico else "ADVERTENCIA"
            log.warning("  [%s] %s / %s — %s", nivel, r.capa, r.check, r.detalle)
        log.info("─" * 60)
        return len(criticos) == 0


def validar_pipeline(
    bronze          = "data/bronze/bronze.parquet",
    silver          = "data/silver/clima_cafe_silver.parquet",
    gold_ranking    = "data/gold/ranking_puntos.parquet",
    gold_estacional = "data/gold/estacionalidad_mensual.parquet",
) -> bool:
    return ValidadorPipeline(bronze, silver, gold_ranking, gold_estacional).ejecutar()


if __name__ == "__main__":
    ok = validar_pipeline()
    exit(0 if ok else 1)
