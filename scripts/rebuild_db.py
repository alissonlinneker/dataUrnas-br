#!/usr/bin/env python3
"""Rebuild do banco DuckDB com parser de logs corrigido (turno-aware).

SEGURANCA: Faz backup do banco antes de reconstruir.
Os dados raw (181 GB, 27 estados) NAO sao alterados.

Correcao: T2 .logjez acumula eventos do T1 (urna nao e limpa entre turnos).
O parser agora filtra por turno_date, eliminando:
- Falsos reboots no T2 (abertura T1 contada como reboot)
- hora_abertura incorreta no T2 (data do T1)
- votos_log duplicados no T2
- ~452k issues C06/A03 falsas no T2
"""

import shutil
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataurnas.database.duckdb_store import DuckDBStore, DEFAULT_DB_PATH
from dataurnas.analyzer.batch import BatchAnalyzer
from dataurnas.config import RAW_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rebuild")

BACKUP = DEFAULT_DB_PATH.parent / "eleicoes_2022_BACKUP.duckdb"
SNAPSHOT = DEFAULT_DB_PATH.parent / "eleicoes_2022_live.duckdb"


def main():
    # 1. Verificar dados raw no disco
    analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
    bu_files = analyzer.find_bu_files()
    total = len(bu_files)
    logger.info("BUs encontrados no disco: %d", total)

    if total < 900_000:
        logger.error(
            "ABORTANDO: Apenas %d BUs no disco (esperado ~941k). "
            "Verifique se os dados raw estao intactos em %s",
            total, RAW_DIR,
        )
        sys.exit(1)

    # 2. Backup do banco existente (SEGURANCA)
    if DEFAULT_DB_PATH.exists():
        logger.info("Fazendo backup: %s -> %s", DEFAULT_DB_PATH.name, BACKUP.name)
        shutil.copy2(str(DEFAULT_DB_PATH), str(BACKUP))
        size_gb = BACKUP.stat().st_size / 1e9
        logger.info("Backup criado com sucesso (%.2f GB)", size_gb)
    else:
        logger.info("Banco nao existe ainda, pulando backup")

    # 3. Abrir store e limpar tabelas (dados raw no disco sao preservados)
    store = DuckDBStore(DEFAULT_DB_PATH)
    store.clear()
    logger.info("Tabelas do banco limpas. Iniciando rebuild...")

    # 4. Recompilar todas as secoes
    start = time.time()
    compiled = 0
    errors = 0

    for i, bu_file in enumerate(bu_files):
        try:
            store._process_section(analyzer, bu_file.parent, bu_file=bu_file)
            compiled += 1
        except Exception as e:
            errors += 1
            if errors <= 50:
                logger.warning("Erro ao processar %s: %s", bu_file.name, e)

        # Checkpoint periodico para persistir e liberar WAL
        if (i + 1) % 1000 == 0:
            store._conn.execute("CHECKPOINT")
            elapsed = time.time() - start
            rate = compiled / (elapsed / 60) if elapsed > 0 else 0
            eta_min = (total - i - 1) / rate if rate > 0 else 0
            logger.info(
                "Progresso: %d/%d (%.1f%%) | %d ok, %d erros | %.0f/min | ETA: %.1f h",
                i + 1, total, (i + 1) / total * 100,
                compiled, errors, rate, eta_min / 60,
            )

    # 5. Finalizar
    store._conn.execute("CHECKPOINT")

    # Snapshot para dashboard
    logger.info("Criando snapshot para dashboard...")
    shutil.copy2(str(DEFAULT_DB_PATH), str(SNAPSHOT))
    wal = SNAPSHOT.with_suffix(".duckdb.wal")
    wal.unlink(missing_ok=True)

    elapsed = time.time() - start
    count = store._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
    t1 = store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=1").fetchone()[0]
    t2 = store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=2").fetchone()[0]

    # Verificacao rapida do fix
    reboots_t2 = store._conn.execute(
        "SELECT COUNT(*) FROM secoes WHERE turno=2 AND reboots > 0"
    ).fetchone()[0]
    total_t2 = t2 or 1
    pct_reboot_t2 = reboots_t2 / total_t2 * 100

    store.close()

    logger.info("=" * 60)
    logger.info("REBUILD COMPLETO em %.1f horas", elapsed / 3600)
    logger.info("Total: %d secoes (1T: %d, 2T: %d)", count, t1, t2)
    logger.info("Erros: %d (de %d tentativas)", errors, total)
    logger.info("Reboots T2: %d secoes (%.1f%%) - ANTES era ~96%%", reboots_t2, pct_reboot_t2)
    logger.info("=" * 60)

    if BACKUP.exists():
        logger.info("Backup preservado em: %s", BACKUP)


if __name__ == "__main__":
    main()
