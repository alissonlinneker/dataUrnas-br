"""Orquestrador de downloads do TSE."""

import asyncio
import logging
import random
from pathlib import Path
from typing import Callable, Optional

from ..config import MAX_CONCURRENT_DOWNLOADS, RAW_DIR, JSON_DIR
from ..models import Section, UrnaMeta
from .client import TSEClient
from .tse_api import TSEApi, StateConfig

logger = logging.getLogger(__name__)


class DownloadManager:
    """Gerencia downloads massivos do TSE com concorrencia controlada."""

    def __init__(
        self,
        raw_dir: Path = RAW_DIR,
        json_dir: Path = JSON_DIR,
        max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
        on_section_done: Optional[Callable[[Path, str], None]] = None,
    ):
        self._raw_dir = raw_dir
        self._json_dir = json_dir
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._on_section_done = on_section_done
        self._stats = {
            "downloaded": 0,
            "skipped": 0,
            "errors": 0,
            "sections_processed": 0,
        }

    def _section_dir(self, section: Section) -> Path:
        return (
            self._raw_dir
            / section.uf.lower()
            / section.municipio_codigo
            / section.zona
            / section.secao
        )

    async def download_section_files(
        self,
        client: TSEClient,
        api: TSEApi,
        ciclo: str,
        pleito: str,
        urna: UrnaMeta,
        file_types: Optional[list[str]] = None,
    ) -> dict:
        """Baixa os arquivos de uma urna especifica."""
        section = urna.section
        dest_dir = self._section_dir(section)
        results = {"ok": 0, "skip": 0, "fail": 0}

        for filename in urna.arquivos:
            # Filtrar por tipo de arquivo se especificado
            if file_types:
                matches = any(filename.endswith(ft) for ft in file_types)
                if not matches:
                    # Verificar tambem por sufixo parcial
                    matches = any(ft in filename for ft in file_types)
                    if not matches:
                        continue

            url = api.build_file_url(ciclo, pleito, section, urna.hash, filename)
            dest = dest_dir / filename

            async with self._semaphore:
                ok = await client.download_file(url, dest)
                if ok:
                    if dest.exists():
                        results["ok"] += 1
                    else:
                        results["skip"] += 1
                else:
                    results["fail"] += 1

        return results

    async def _process_one_section(
        self,
        section_sem: asyncio.Semaphore,
        client: TSEClient,
        api: TSEApi,
        ciclo: str,
        pleito: str,
        section,
        file_types: Optional[list[str]],
        uf: str,
        total: int,
    ):
        """Baixa e processa uma unica secao (chamado em paralelo)."""
        async with section_sem:
            try:
                urnas = await api.get_section_meta(ciclo, pleito, section)
                for urna in urnas:
                    results = await self.download_section_files(
                        client, api, ciclo, pleito, urna, file_types
                    )
                    self._stats["downloaded"] += results["ok"]
                    self._stats["skipped"] += results["skip"]
                    self._stats["errors"] += results["fail"]

                self._stats["sections_processed"] += 1

                if self._on_section_done:
                    section_dir = self._section_dir(section)
                    try:
                        self._on_section_done(section_dir, pleito)
                    except Exception as cb_err:
                        logger.debug("Callback erro: %s", cb_err)

                processed = self._stats["sections_processed"]
                if processed % 100 == 0:
                    logger.info(
                        "Progresso %s: %d/%d secoes | %d baixados, %d ignorados, %d erros",
                        uf.upper(),
                        processed,
                        total,
                        self._stats["downloaded"],
                        self._stats["skipped"],
                        self._stats["errors"],
                    )
            except Exception as e:
                logger.error(
                    "Erro na secao %s/%s/%s: %s",
                    section.municipio_codigo,
                    section.zona,
                    section.secao,
                    e,
                )
                self._stats["errors"] += 1

    async def download_state(
        self,
        client: TSEClient,
        api: TSEApi,
        ciclo: str,
        pleito: str,
        uf: str,
        file_types: Optional[list[str]] = None,
        max_sections: Optional[int] = None,
        municipio_filter: Optional[list[str]] = None,
    ) -> dict:
        """Baixa dados de um estado inteiro (ou amostra) com secoes em paralelo."""
        logger.info("Baixando dados de %s...", uf.upper())

        state_config = await api.get_state_config(ciclo, pleito, uf)
        if not state_config:
            logger.error("Nao foi possivel obter configuracao de %s", uf)
            return self._stats

        total_sections = api.count_sections(state_config)
        logger.info(
            "%s: %d municipios, %d secoes totais",
            state_config.nome,
            len(state_config.municipios),
            total_sections,
        )

        sections = list(api.iter_sections(state_config))

        # Filtrar por municipio se especificado
        if municipio_filter:
            sections = [
                s for s in sections
                if s.municipio_codigo in municipio_filter
                or s.municipio_nome.upper() in [m.upper() for m in municipio_filter]
            ]
            logger.info("Filtrado para %d secoes em municipios selecionados", len(sections))

        # Limitar amostra se especificado
        if max_sections and max_sections < len(sections):
            sections = random.sample(sections, max_sections)
            logger.info("Amostra aleatoria de %d secoes", max_sections)

        # Semaforo para limitar secoes concorrentes (separado do semaforo de arquivos)
        section_sem = asyncio.Semaphore(self._semaphore._value)

        # Processar secoes em lotes paralelos
        BATCH = 500
        for batch_start in range(0, len(sections), BATCH):
            batch = sections[batch_start:batch_start + BATCH]
            tasks = [
                self._process_one_section(
                    section_sem, client, api, ciclo, pleito,
                    section, file_types, uf, len(sections),
                )
                for section in batch
            ]
            await asyncio.gather(*tasks)

        logger.info(
            "Concluido %s: %d secoes, %d baixados, %d ignorados, %d erros",
            uf.upper(),
            self._stats["sections_processed"],
            self._stats["downloaded"],
            self._stats["skipped"],
            self._stats["errors"],
        )
        return self._stats

    async def download_sample(
        self,
        client: TSEClient,
        api: TSEApi,
        ciclo: str,
        pleito: str,
        states: list[str],
        sections_per_state: int = 50,
        file_types: Optional[list[str]] = None,
    ) -> dict:
        """Baixa amostra de dados de multiplos estados."""
        logger.info(
            "Baixando amostra: %d estados, %d secoes/estado",
            len(states),
            sections_per_state,
        )
        all_stats = {"downloaded": 0, "skipped": 0, "errors": 0, "sections_processed": 0}

        for uf in states:
            self._stats = {"downloaded": 0, "skipped": 0, "errors": 0, "sections_processed": 0}
            await self.download_state(
                client, api, ciclo, pleito, uf,
                file_types=file_types,
                max_sections=sections_per_state,
            )
            for k in all_stats:
                all_stats[k] += self._stats[k]

        logger.info(
            "Amostra concluida: %d secoes, %d baixados, %d erros",
            all_stats["sections_processed"],
            all_stats["downloaded"],
            all_stats["errors"],
        )
        return all_stats
