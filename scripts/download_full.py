"""Download completo de todas as secoes eleitorais - 1o turno 2022."""

import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataurnas.config import ESTADOS
from dataurnas.downloader.client import TSEClient
from dataurnas.downloader.tse_api import TSEApi
from dataurnas.downloader.manager import DownloadManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("download_full")

CICLO = "ele2022"
PLEITO = "406"  # 1o turno

# Ordem: estados menores primeiro para progresso rapido
ESTADOS_ORDENADOS = [
    # Pequenos (<5k secoes)
    "df", "ac", "ap", "rr", "ro", "to", "se",
    # Medios (5k-15k)
    "al", "am", "ms", "mt", "pb", "pi", "rn", "es", "ma",
    # Grandes (15k-35k)
    "go", "sc", "pe", "ce", "pa", "pr", "rs", "rj", "ba",
    # Gigantes (>35k)
    "mg", "sp",
]


async def download_all():
    start = time.time()
    total_stats = {"downloaded": 0, "skipped": 0, "errors": 0, "sections_processed": 0}

    async with TSEClient() as client:
        api = TSEApi(client)

        for i, uf in enumerate(ESTADOS_ORDENADOS):
            logger.info("=" * 60)
            logger.info("ESTADO %d/%d: %s", i + 1, len(ESTADOS_ORDENADOS), uf.upper())
            logger.info("=" * 60)

            manager = DownloadManager()
            try:
                stats = await manager.download_state(
                    client, api, CICLO, PLEITO, uf,
                    file_types=None,
                    max_sections=None,
                )
                for k in total_stats:
                    total_stats[k] += stats.get(k, 0)
            except Exception as e:
                logger.error("Erro fatal em %s: %s", uf.upper(), e)

            elapsed = time.time() - start
            logger.info(
                "ACUMULADO: %d secoes, %d downloads, %d erros | Tempo: %.0f min",
                total_stats["sections_processed"],
                total_stats["downloaded"],
                total_stats["errors"],
                elapsed / 60,
            )

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info("DOWNLOAD COMPLETO!")
    logger.info("Total: %d secoes processadas", total_stats["sections_processed"])
    logger.info("Downloads: %d | Skipped: %d | Erros: %d",
                total_stats["downloaded"], total_stats["skipped"], total_stats["errors"])
    logger.info("Tempo total: %.1f horas", elapsed / 3600)
    logger.info("=" * 60)
    print(f"RESULTADO_FINAL: {total_stats}")


if __name__ == "__main__":
    asyncio.run(download_all())
