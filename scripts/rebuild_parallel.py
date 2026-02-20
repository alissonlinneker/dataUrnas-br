#!/usr/bin/env python3
"""Rebuild PARALELO do banco DuckDB com parser turno-aware.

Usa multiprocessing para parsear seções em paralelo (CPU-bound),
enquanto um único processo principal insere no DuckDB (single-writer).

Suporta RESUME: detecta seções já inseridas e pula automaticamente.

SEGURANÇA: Backup já existe. Dados raw (181 GB) NÃO são alterados.
"""

import multiprocessing as mp
import shutil
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dataurnas.database.duckdb_store import DuckDBStore, DEFAULT_DB_PATH, _UF_REGIAO, TIMEZONE_OFFSETS
from dataurnas.analyzer.batch import BatchAnalyzer
from dataurnas.config import RAW_DIR

# Log em local persistente (não /private/tmp que limpa ao reiniciar)
LOG_FILE = Path(__file__).parent.parent / "data" / "rebuild_parallel.log"

_file_handler = logging.FileHandler(str(LOG_FILE), mode="a")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler], force=True)
logger = logging.getLogger("rebuild_parallel")
logger.setLevel(logging.INFO)

BACKUP = DEFAULT_DB_PATH.parent / "eleicoes_2022_BACKUP.duckdb"
SNAPSHOT = DEFAULT_DB_PATH.parent / "eleicoes_2022_live.duckdb"
NUM_WORKERS = 10
BATCH_SIZE = 500

# Variável global do worker - inicializada UMA VEZ por processo
_worker_analyzer = None


def _init_worker():
    """Inicializa o BatchAnalyzer uma única vez por processo worker."""
    global _worker_analyzer
    _worker_analyzer = BatchAnalyzer(raw_dir=RAW_DIR)


def parse_section(bu_file_str: str) -> dict | None:
    """Worker function: parseia UMA seção e retorna dados para inserção."""
    try:
        bu_file = Path(bu_file_str)
        section_dir = bu_file.parent

        result = _worker_analyzer._analyze_section(section_dir, bu_file=bu_file)

        uf = result["uf"]
        turno = result.get("turno", 1)
        secao_id = f"{turno}T/{uf}/{result['municipio']}/{result['zona']}/{result['secao']}"

        regiao = _UF_REGIAO.get(uf.lower(), "desconhecida")
        fuso = TIMEZONE_OFFSETS.get(uf.lower(), 0)
        bu_info = result.get("bu")
        timing = result.get("log_timing") or {}
        log_events = result.get("log_events") or {}

        eleitores_aptos = 0
        comparecimento = 0
        if bu_info:
            eleicoes = bu_info.get("eleicoes", [])
            if eleicoes:
                eleitores_aptos = eleicoes[0].get("aptos", 0)
                for el in eleicoes:
                    for c in el.get("cargos", []):
                        comp = c.get("comparecimento", 0)
                        if comp > comparecimento:
                            comparecimento = comp

        lib_codigo = bu_info.get("lib_codigo", 0) if bu_info else 0
        comp_bio = bu_info.get("comp_biometrico", 0) if bu_info else 0
        total_bio = lib_codigo + comp_bio
        pct_bio = round((comp_bio / total_bio * 100), 2) if total_bio > 0 else None
        pct_abs = round(((eleitores_aptos - comparecimento) / eleitores_aptos * 100), 2) if eleitores_aptos > 0 else None

        duracao = timing.get("duracao_votacao_min")
        is_reserva = result.get("tipo_urna") == "reservaSecao"
        issues = result.get("issues", [])

        secao_row = [
            secao_id, turno,
            uf, regiao, result["municipio"], result["zona"], result["secao"],
            result.get("modelo"), result.get("tipo_urna"), result.get("versao_sw"),
            fuso,
            bu_info.get("emissao") if bu_info else None,
            eleitores_aptos, comparecimento, lib_codigo, comp_bio, pct_bio, pct_abs,
            timing.get("hora_abertura"), timing.get("hora_encerramento"), duracao,
            log_events.get("reboots", 0), log_events.get("erros", 0),
            log_events.get("alertas_mesario", 0), log_events.get("votos_computados", 0),
            log_events.get("substituicoes", 0),
            is_reserva, len(issues) > 0, len(issues),
        ]

        issue_rows = []
        for issue in issues:
            issue_rows.append([
                secao_id, issue.codigo, issue.severidade.value,
                issue.descricao, issue.base_legal,
                str(issue.detalhes) if issue.detalhes else None,
            ])

        voto_rows = []
        totais_rows = []
        bu_obj = result.get("bu_obj")
        if bu_obj:
            for eleicao in bu_obj.resultados_por_eleicao:
                for cargo in eleicao.resultados:
                    nominais = brancos = nulos = legenda = 0
                    for voto in cargo.votos:
                        tipo = voto.tipo_voto.value
                        voto_rows.append([
                            secao_id, eleicao.id_eleicao,
                            cargo.nome_cargo, cargo.codigo_cargo,
                            tipo, voto.codigo_votavel, voto.partido,
                            voto.quantidade,
                        ])
                        if tipo == "nominal":
                            nominais += voto.quantidade
                        elif tipo == "branco":
                            brancos += voto.quantidade
                        elif tipo == "nulo":
                            nulos += voto.quantidade
                        elif tipo == "legenda":
                            legenda += voto.quantidade
                    total = nominais + brancos + nulos + legenda
                    totais_rows.append([
                        secao_id, eleicao.id_eleicao,
                        cargo.nome_cargo, cargo.codigo_cargo,
                        cargo.comparecimento, nominais, brancos, nulos, legenda, total,
                    ])

        return {
            "secao_id": secao_id,
            "secao": secao_row,
            "issues": issue_rows,
            "votos": voto_rows,
            "totais": totais_rows,
        }

    except Exception as e:
        return {"error": str(e), "file": bu_file_str}


def insert_batch(store, batch: list[dict]):
    """Insere um batch de resultados no DuckDB usando executemany."""
    secao_rows = []
    issue_rows = []
    voto_rows = []
    totais_rows = []

    for item in batch:
        if "error" in item:
            continue
        secao_rows.append(item["secao"])
        issue_rows.extend(item["issues"])
        voto_rows.extend(item["votos"])
        totais_rows.extend(item["totais"])

    if secao_rows:
        store._conn.executemany("""
            INSERT OR REPLACE INTO secoes VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?
            )
        """, secao_rows)

    if issue_rows:
        store._conn.executemany("""
            INSERT INTO issues VALUES (
                nextval('issue_seq'), ?, ?, ?, ?, ?, ?
            )
        """, issue_rows)

    if voto_rows:
        store._conn.executemany("""
            INSERT INTO votos VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, voto_rows)

    if totais_rows:
        store._conn.executemany("""
            INSERT INTO totais_cargo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, totais_rows)


def _flush_logs():
    for h in logging.root.handlers:
        h.flush()


def main():
    logger.info("=" * 60)
    logger.info("REBUILD PARALELO - INÍCIO")
    logger.info("=" * 60)
    _flush_logs()

    # 1. Verificar backup
    if not BACKUP.exists():
        logger.error("ABORTANDO: Backup não encontrado em %s", BACKUP)
        sys.exit(1)
    logger.info("Backup verificado: %s (%.2f GB)", BACKUP.name, BACKUP.stat().st_size / 1e9)

    # 2. Abrir store e verificar dados existentes (para resume)
    store = DuckDBStore(DEFAULT_DB_PATH)
    existing_count = store._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]

    if existing_count > 0:
        # Modo RESUME: carregar IDs já processados
        logger.info("MODO RESUME: %d seções já no banco. Carregando IDs existentes...", existing_count)
        _flush_logs()
        existing_ids = set(
            row[0] for row in store._conn.execute("SELECT id FROM secoes").fetchall()
        )
        logger.info("IDs carregados: %d", len(existing_ids))
    else:
        logger.info("Banco vazio. Rebuild completo.")
        existing_ids = set()

    # 3. Encontrar todos os BU files
    logger.info("Buscando arquivos BU no disco (pode levar 1-2 min)...")
    _flush_logs()
    analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
    bu_files = analyzer.find_bu_files()
    total_disk = len(bu_files)
    logger.info("BUs encontrados no disco: %d", total_disk)

    if total_disk < 900_000:
        logger.error("ABORTANDO: Apenas %d BUs (esperado ~941k)", total_disk)
        sys.exit(1)

    # 4. Converter para strings
    bu_file_strs = [str(f) for f in bu_files]
    total_remaining = total_disk  # Processaremos todos, skip dos existentes é feito após parse

    logger.info(
        "Processando %d BUs total (%d já no banco, %d a inserir estimados)",
        total_disk, existing_count, total_disk - existing_count,
    )
    logger.info("Workers: %d | Batch: %d | Snapshot a cada 50k", NUM_WORKERS, BATCH_SIZE)
    _flush_logs()

    # 5. Processar em paralelo
    start = time.time()
    compiled = 0
    skipped = 0
    errors = 0
    error_samples = []

    with mp.Pool(processes=NUM_WORKERS, initializer=_init_worker) as pool:
        batch = []
        for i, result in enumerate(pool.imap_unordered(parse_section, bu_file_strs, chunksize=100)):
            if result is None:
                errors += 1
                continue

            if "error" in result:
                errors += 1
                if len(error_samples) < 20:
                    error_samples.append(f"{result.get('file', '?')}: {result['error']}")
                continue

            # Skip se já existe no banco (modo resume)
            if existing_ids and result["secao_id"] in existing_ids:
                skipped += 1
                processed = compiled + errors + skipped
                if processed % 10000 == 0:
                    elapsed = time.time() - start
                    rate = processed / (elapsed / 60) if elapsed > 0 else 0
                    logger.info(
                        "Escaneando: %d/%d (%.1f%%) | %d inseridos, %d pulados, %d erros | %.0f/min",
                        processed, total_disk, processed / total_disk * 100,
                        compiled, skipped, errors, rate,
                    )
                    _flush_logs()
                continue

            batch.append(result)
            compiled += 1

            if len(batch) >= BATCH_SIZE:
                insert_batch(store, batch)
                batch = []

            processed = compiled + errors + skipped
            if processed % 2000 == 0:
                store._conn.execute("CHECKPOINT")
                elapsed = time.time() - start
                rate = processed / (elapsed / 60) if elapsed > 0 else 0
                new_rate = compiled / (elapsed / 60) if elapsed > 0 else 0
                eta_min = (total_disk - processed) / rate if rate > 0 else 0
                logger.info(
                    "Progresso: %d/%d (%.1f%%) | %d novos, %d pulados, %d erros | %.0f/min (%.0f novos/min) | ETA: %.1f h",
                    processed, total_disk, processed / total_disk * 100,
                    compiled, skipped, errors, rate, new_rate, eta_min / 60,
                )
                _flush_logs()

                # Snapshot intermediário a cada 50k processados
                total_in_db = existing_count + compiled
                if compiled > 0 and total_in_db % 50_000 < 2000:
                    logger.info("Criando snapshot intermediário para dashboard (%d seções no banco)...", total_in_db)
                    store._conn.execute("CHECKPOINT")
                    try:
                        shutil.copy2(str(DEFAULT_DB_PATH), str(SNAPSHOT))
                        wal = SNAPSHOT.with_suffix(".duckdb.wal")
                        wal.unlink(missing_ok=True)
                        logger.info("Snapshot atualizado.")
                    except Exception as e:
                        logger.warning("Falha ao criar snapshot: %s", e)
                    _flush_logs()

        if batch:
            insert_batch(store, batch)

    # 6. Checkpoint e snapshot final
    store._conn.execute("CHECKPOINT")
    logger.info("Criando snapshot final para dashboard...")
    shutil.copy2(str(DEFAULT_DB_PATH), str(SNAPSHOT))
    wal = SNAPSHOT.with_suffix(".duckdb.wal")
    wal.unlink(missing_ok=True)

    # 7. Estatísticas finais
    elapsed = time.time() - start
    count = store._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
    t1 = store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=1").fetchone()[0]
    t2 = store._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=2").fetchone()[0]
    reboots_t2 = store._conn.execute(
        "SELECT COUNT(*) FROM secoes WHERE turno=2 AND reboots > 0"
    ).fetchone()[0]
    total_t2 = t2 or 1
    pct_reboot_t2 = reboots_t2 / total_t2 * 100

    store.close()

    logger.info("=" * 60)
    logger.info("REBUILD PARALELO COMPLETO em %.1f horas (%.0f min)", elapsed / 3600, elapsed / 60)
    logger.info("Workers: %d | Rate total: %.0f/min | Novos inseridos: %d", NUM_WORKERS, (compiled + skipped) / (elapsed / 60) if elapsed > 0 else 0, compiled)
    logger.info("Total no banco: %d seções (1T: %d, 2T: %d)", count, t1, t2)
    logger.info("Pulados (resume): %d | Erros: %d", skipped, errors)
    logger.info("Reboots T2: %d seções (%.1f%%) — ANTES era ~96%%", reboots_t2, pct_reboot_t2)
    logger.info("=" * 60)

    if error_samples:
        logger.info("Exemplos de erros:")
        for e in error_samples[:10]:
            logger.info("  %s", e)


if __name__ == "__main__":
    main()
