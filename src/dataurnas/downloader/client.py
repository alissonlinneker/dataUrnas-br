"""Cliente HTTP com retry e rate limiting para o TSE."""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import httpx

from ..config import CONNECT_TIMEOUT, MAX_RETRIES, RATE_LIMIT_PER_SECOND, READ_TIMEOUT

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter simples baseado em token bucket."""

    def __init__(self, rate: int = RATE_LIMIT_PER_SECOND):
        self._rate = rate
        self._tokens = rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_refill = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


class TSEClient:
    """Cliente HTTP para a API do TSE com retry e rate limiting."""

    def __init__(self, rate_limit: int = RATE_LIMIT_PER_SECOND):
        self._rate_limiter = RateLimiter(rate_limit)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=CONNECT_TIMEOUT,
                    read=READ_TIMEOUT,
                    write=READ_TIMEOUT,
                    pool=READ_TIMEOUT,
                ),
                limits=httpx.Limits(
                    max_connections=600,
                    max_keepalive_connections=300,
                ),
                follow_redirects=True,
                http2=True,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def fetch_json(self, url: str) -> Optional[dict]:
        """Busca JSON da API do TSE com retry."""
        for attempt in range(MAX_RETRIES):
            await self._rate_limiter.acquire()
            try:
                client = await self._get_client()
                response = await client.get(url)
                if response.status_code == 404:
                    logger.debug("404 para %s", url)
                    return None
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("Rate limited (429), aguardando %ds...", wait)
                    await asyncio.sleep(wait)
                    continue
                logger.error("HTTP %d para %s", e.response.status_code, url)
                if attempt == MAX_RETRIES - 1:
                    raise
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Erro de conexao (tentativa %d/%d): %s. Aguardando %ds...",
                    attempt + 1, MAX_RETRIES, e, wait,
                )
                await asyncio.sleep(wait)
        return None

    async def download_file(
        self, url: str, dest: Path, skip_existing: bool = True
    ) -> bool:
        """Baixa arquivo do TSE para o disco local.

        Retorna True se o arquivo foi baixado ou ja existia.
        """
        if skip_existing and dest.exists() and dest.stat().st_size > 0:
            logger.debug("Arquivo ja existe: %s", dest)
            return True

        dest.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(MAX_RETRIES):
            await self._rate_limiter.acquire()
            try:
                client = await self._get_client()
                async with client.stream("GET", url) as response:
                    if response.status_code == 404:
                        logger.debug("404 para %s", url)
                        return False
                    response.raise_for_status()
                    with open(dest, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                logger.debug("Baixado: %s -> %s", url, dest)
                return True
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("Rate limited, aguardando %ds...", wait)
                    await asyncio.sleep(wait)
                    continue
                logger.error("Erro HTTP %d ao baixar %s", e.response.status_code, url)
                if dest.exists():
                    dest.unlink()
                if attempt == MAX_RETRIES - 1:
                    return False
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Erro de conexao ao baixar (tentativa %d/%d): %s",
                    attempt + 1, MAX_RETRIES, e,
                )
                if dest.exists():
                    dest.unlink()
                await asyncio.sleep(wait)
            except Exception:
                if dest.exists():
                    dest.unlink()
                raise
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
