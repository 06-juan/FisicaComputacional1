"""
Pipeline ERA5-Land & Café — Eje Cafetero
Orquestador principal con argument parser.

Uso:
  python main.py                        # pipeline completo
  python main.py --desde silver         # arranca desde Silver (salta BRONZE)
  python main.py --desde gold           # arranca desde Gold  (salta BRONZE+Silver)
  python main.py --solo validar         # solo corre las validaciones
  python main.py --forzar-descarga      # fuerza re-descarga desde GEE/Drive
  python main.py --sin-mapa             # omite la generación del mapa HTML
  python main.py --explain              # muestra EXPLAIN ANALYZE de queries clave
  python main.py --desde silver --explain --sin-mapa
"""

import argparse
import sys
import time
import logging
from dotenv import load_dotenv

load_dotenv()  # DEBE ir antes de cualquier import de src

from src.utils.logger import PipelineLogger
from src.utils.GoogleAutenticator  import autenticar_drive
from src.ingest.downloadData       import procesar_bronze
from src.utils.uuid_patch         import agregar_uuid_a_bronze
from src.pipeline.silver           import procesar_silver
from src.pipeline.gold             import procesar_gold
from src.utils.generadorMapa       import generar_mapa_desde_gold
from src.utils.data_contracts  import aplicar_contratos
from src.utils.validate            import validar_pipeline
from src.utils.explain             import ejecutar_explain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PASOS_VALIDOS = ("Bronze", "silver", "gold")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python main.py",
        description="Pipeline agroclimático ERA5-Land → Eje Cafetero",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--desde",
        choices=PASOS_VALIDOS,
        default="bronze",
        metavar="PASO",
        help="Capa desde la que arrancar: bronze | silver | gold  (default: bronze)",
    )
    p.add_argument(
        "--solo",
        choices=("validar",),
        default=None,
        metavar="ACCION",
        help="Ejecutar solo una acción específica: validar",
    )
    p.add_argument(
        "--forzar-descarga",
        action="store_true",
        help="Forzar re-descarga de datos desde GEE/Drive aunque ya existan localmente",
    )
    p.add_argument(
        "--sin-mapa",
        action="store_true",
        help="Omitir la generación del mapa interactivo HTML al final",
    )
    p.add_argument(
        "--explain",
        action="store_true",
        help="Mostrar EXPLAIN ANALYZE de las queries principales (Silver y Gold)",
    )
    return p


def _encabezado(args: argparse.Namespace) -> None:
    print("=" * 60)
    print("  Pipeline ERA5-Land & Café — Eje Cafetero")
    print("=" * 60)
    print(f"  Desde:            {args.desde.upper()}")
    print(f"  Solo:             {args.solo or '—'}")
    print(f"  Forzar descarga:  {'sí' if args.forzar_descarga else 'no'}")
    print(f"  Sin mapa:         {'sí' if args.sin_mapa else 'no'}")
    print(f"  EXPLAIN:          {'sí' if args.explain else 'no'}")
    print("=" * 60)


def main(argv=None) -> int:
    parser = _build_parser()
    args   = parser.parse_args(argv)

    logger = PipelineLogger()
    t0 = time.perf_counter()

    try:
        logger.info("Configuración de ejecución:")
        logger.info(f"Desde: {args.desde}")
        logger.info(f"Forzar descarga: {args.forzar_descarga}")
        logger.info(f"Sin mapa: {args.sin_mapa}")
        logger.info(f"Explain: {args.explain}")

        # ── SOLO VALIDAR ─────────────────────────────
        if args.solo == "validar":
            with logger.step("Validaciones"):
                ok = validar_pipeline()
                return 0 if ok else 1

        # ── AUTENTICACIÓN ────────────────────────────
        with logger.step("Autenticación Google Drive"):
            autenticar_drive()
            logger.success("Acceso a Google Drive verificado.")

        # ── CONTRATOS ────────────────────────────────
        with logger.step("Aplicando contratos de datos"):
            aplicar_contratos()

        # ── BRONZE ──────────────────────────────────────
        if args.desde == "bronze":
            with logger.step("Procesando capa BRONZE"):
                procesar_bronze(forzar_descarga=args.forzar_descarga)
                agregar_uuid_a_bronze()

        # ── EXPLAIN SILVER ───────────────────────────
        if args.explain and args.desde in ("bronze", "silver"):
            with logger.step("EXPLAIN SILVER"):
                ejecutar_explain("silver")

        # ── SILVER ───────────────────────────────────
        if args.desde in ("bronze", "silver"):
            with logger.step("Procesando capa SILVER"):
                procesar_silver()

        # ── EXPLAIN GOLD ─────────────────────────────
        if args.explain:
            with logger.step("EXPLAIN GOLD"):
                ejecutar_explain("gold")

        # ── GOLD ─────────────────────────────────────
        with logger.step("Procesando capa GOLD"):
            procesar_gold()

        # ── VALIDACIONES ─────────────────────────────
        with logger.step("Ejecutando validaciones"):
            ok = validar_pipeline()
            if not ok:
                raise RuntimeError("Errores críticos de validación")

        # ── MAPA ─────────────────────────────────────
        if not args.sin_mapa:
            with logger.step("Generando mapa interactivo"):
                generar_mapa_desde_gold()

        logger.success("Pipeline ejecutado correctamente.")
        logger.finish()

        return 0

    except Exception as e:
        logger.error(f"Error crítico: {e}")
        logger.finish()
        return 1


if __name__ == "__main__":
    sys.exit(main())
