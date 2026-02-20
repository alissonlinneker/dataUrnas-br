"""Download + compilacao DuckDB em tempo real - ambos os turnos.

Baixa seções e compila no banco de dados imediatamente após o download.
Usa no máximo 500 Mbps de banda (100 conexões concorrentes, 500 req/s).

ARQUITETURA:
- O builder mantém conexão write exclusiva ao DB principal.
- A cada N seções compiladas, cria um snapshot (cópia) do DB.
- O dashboard lê do snapshot, que é atualizado periodicamente.
"""

import asyncio
import logging
import shutil
import sys
import time
import threading
from pathlib import Path
from queue import Queue, Empty

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataurnas.downloader.client import TSEClient
from dataurnas.downloader.tse_api import TSEApi
from dataurnas.downloader.manager import DownloadManager
from dataurnas.database.duckdb_store import DuckDBStore, DEFAULT_DB_PATH
from dataurnas.analyzer.batch import BatchAnalyzer
from dataurnas.config import RAW_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("download_and_build")

# Limites para 500 Mbps (maximizar banda disponível)
MAX_CONCURRENT = 500
RATE_LIMIT = 3000

# Compilação e snapshot
BATCH_SIZE = 200
SNAPSHOT_INTERVAL = 500  # Criar snapshot a cada N seções compiladas

SNAPSHOT_PATH = DEFAULT_DB_PATH.parent / "eleicoes_2022_live.duckdb"

CICLO = "ele2022"

# Ordem: estados menores primeiro
# Estados que PRECISAM de download primeiro (grandes, ainda com amostras)
# Depois os já baixados (apenas validação rápida)
ESTADOS_ORDENADOS = [
    "sc", "pe", "ce", "pa", "pr",
    "rs", "rj", "ba", "mg", "sp",
    "go", "ma", "es", "rn", "pi",
    "pb", "mt", "ms", "am", "al",
    "se", "to", "ro", "rr", "ap",
    "ac", "df",
]


def create_snapshot(store):
    """Cria snapshot do banco para leitura pelo dashboard."""
    try:
        store._conn.execute("CHECKPOINT")
        # Exportar para snapshot usando EXPORT/IMPORT nativo do DuckDB
        tmp_snap = SNAPSHOT_PATH.with_suffix(".duckdb.tmp")
        tmp_snap.unlink(missing_ok=True)
        # Copiar o arquivo principal (sem WAL)
        shutil.copy2(str(DEFAULT_DB_PATH), str(tmp_snap))
        # Mover atomicamente (rename é atômico no mesmo filesystem)
        tmp_snap.rename(SNAPSHOT_PATH)
        # Remover WAL do snapshot se existir
        wal = SNAPSHOT_PATH.with_suffix(".duckdb.wal")
        wal.unlink(missing_ok=True)
        logger.info("Snapshot atualizado: %s", SNAPSHOT_PATH.name)
    except Exception as e:
        logger.warning("Erro ao criar snapshot: %s", e)


class RealtimeBuilder:
    """Compila seções no DuckDB em tempo real.

    Mantém conexão persistente para performance, cria snapshots
    periódicos para o dashboard ler.
    """

    def __init__(self):
        self._store = DuckDBStore(DEFAULT_DB_PATH)
        self._analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
        self._queue: Queue = Queue()
        self._compiled = 0
        self._errors = 0
        self._done = False
        self._lock = threading.Lock()
        self._last_snapshot = 0

        # Carregar IDs existentes
        rows = self._store._conn.execute("SELECT id FROM secoes").fetchall()
        self._existing = {r[0] for r in rows}
        logger.info("Builder: %d seções já compiladas no DB", len(self._existing))

        # Criar snapshot inicial
        create_snapshot(self._store)

        # Iniciar thread de compilação
        self._thread = threading.Thread(target=self._compile_loop, daemon=False)
        self._thread.start()

    def on_section_downloaded(self, section_dir: Path, pleito: str):
        """Callback chamado após cada seção ser baixada."""
        prefix = f"o00{pleito}"
        bu_files = list(section_dir.glob(f"{prefix}*.bu")) + \
                   list(section_dir.glob(f"{prefix}*-bu.dat"))

        for bu_file in bu_files:
            secao_id = DuckDBStore._bu_file_to_secao_id(bu_file, self._analyzer)
            if not secao_id or secao_id in self._existing:
                continue
            self._queue.put((section_dir, bu_file, secao_id))

    def _compile_loop(self):
        """Thread de compilação - processa continuamente da fila."""
        while not self._done or not self._queue.empty():
            batch = []
            try:
                while len(batch) < BATCH_SIZE:
                    item = self._queue.get(timeout=2.0)
                    batch.append(item)
            except Empty:
                pass

            if not batch:
                continue

            with self._lock:
                for section_dir, bu_file, secao_id in batch:
                    if secao_id in self._existing:
                        continue
                    try:
                        self._store._process_section(
                            self._analyzer, section_dir, bu_file=bu_file
                        )
                        self._existing.add(secao_id)
                        self._compiled += 1
                    except Exception as e:
                        self._errors += 1
                        if self._errors <= 20:
                            logger.warning("Builder erro %s: %s", secao_id, e)

                # Checkpoint para persistir
                self._store._conn.execute("CHECKPOINT")

            logger.info(
                "Builder: +%d batch, total %d compiladas (%d erros), fila: %d",
                len(batch), self._compiled, self._errors, self._queue.qsize(),
            )

            # Criar snapshot periodicamente
            if self._compiled - self._last_snapshot >= SNAPSHOT_INTERVAL:
                with self._lock:
                    create_snapshot(self._store)
                self._last_snapshot = self._compiled

    def compile_remaining_from_disk(self):
        """Compila BUs que existem no disco mas não estão no banco."""
        logger.info("Compilando BUs restantes do disco...")
        all_bu_files = self._analyzer.find_bu_files()
        enqueued = 0
        for bu_file in all_bu_files:
            secao_id = DuckDBStore._bu_file_to_secao_id(bu_file, self._analyzer)
            if secao_id and secao_id not in self._existing:
                self._queue.put((bu_file.parent, bu_file, secao_id))
                enqueued += 1
        logger.info("Enfileirados %d BUs para compilação (de %d no disco)", enqueued, len(all_bu_files))

    def finish(self):
        """Sinaliza fim e aguarda fila esvaziar."""
        self._done = True
        logger.info("Builder: aguardando fila drenar (%d pendentes)...", self._queue.qsize())
        self._thread.join()  # Sem timeout - aguarda toda fila compilar

        # Snapshot final
        with self._lock:
            create_snapshot(self._store)

        count = self._store._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
        t1 = self._store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=1").fetchone()[0]
        t2 = self._store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=2").fetchone()[0]
        self._store.close()
        logger.info(
            "Builder final: +%d compilados, %d erros. Total DB: %d (1T: %d, 2T: %d)",
            self._compiled, self._errors, count, t1, t2,
        )

    @property
    def stats(self):
        return {"compiled": self._compiled, "errors": self._errors, "queue": self._queue.qsize()}


CONCURRENT_STATES = 3  # Baixar 3 estados ao mesmo tempo por turno (6 total)


async def download_state_task(builder: RealtimeBuilder, pleito: str, turno_label: str, uf: str, idx: int, total: int):
    """Baixa um estado com client HTTP próprio."""
    logger.info("%s ESTADO %d/%d: %s - INICIANDO", turno_label, idx, total, uf.upper())

    async with TSEClient(rate_limit=RATE_LIMIT // CONCURRENT_STATES) as client:
        api = TSEApi(client)
        manager = DownloadManager(
            max_concurrent=MAX_CONCURRENT // CONCURRENT_STATES,
            on_section_done=builder.on_section_downloaded,
        )
        try:
            stats = await manager.download_state(
                client, api, CICLO, pleito, uf,
                file_types=None,
                max_sections=None,
            )
            logger.info(
                "%s ESTADO %s CONCLUIDO: %d secoes, %d downloads, %d erros",
                turno_label, uf.upper(),
                stats.get("sections_processed", 0),
                stats.get("downloaded", 0),
                stats.get("errors", 0),
            )
        except Exception as e:
            logger.error("Erro fatal %s em %s: %s", turno_label, uf.upper(), e)


async def download_turno(builder: RealtimeBuilder, pleito: str, turno_label: str):
    """Baixa um turno inteiro com múltiplos estados em paralelo."""
    start = time.time()
    total = len(ESTADOS_ORDENADOS)

    # Processar estados em lotes de CONCURRENT_STATES
    for batch_start in range(0, total, CONCURRENT_STATES):
        batch = ESTADOS_ORDENADOS[batch_start:batch_start + CONCURRENT_STATES]
        logger.info("=" * 60)
        logger.info("%s LOTE %d-%d/%d: %s", turno_label,
                    batch_start + 1, batch_start + len(batch), total,
                    ", ".join(u.upper() for u in batch))
        logger.info("=" * 60)

        tasks = [
            download_state_task(builder, pleito, turno_label, uf, batch_start + j + 1, total)
            for j, uf in enumerate(batch)
        ]
        await asyncio.gather(*tasks)

        elapsed = time.time() - start
        logger.info("%s LOTE CONCLUIDO em %.0f min | DB: %s", turno_label, elapsed / 60, builder.stats)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info("%s COMPLETO em %.1f horas", turno_label, elapsed / 3600)
    logger.info("=" * 60)


async def main():
    builder = RealtimeBuilder()

    # Primeiro: compilar BUs que já existem no disco mas não estão no banco
    builder.compile_remaining_from_disk()

    # Depois: continuar download de ambos os turnos em paralelo
    # 2 turnos × 5 estados simultâneos = 10 estados baixando ao mesmo tempo
    await asyncio.gather(
        download_turno(builder, "406", "1T"),
        download_turno(builder, "407", "2T"),
    )

    builder.finish()
    logger.info("TUDO COMPLETO! Download + compilação finalizados.")


if __name__ == "__main__":
    asyncio.run(main())
