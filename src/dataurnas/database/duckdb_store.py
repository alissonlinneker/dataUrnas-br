"""Camada de persistencia DuckDB para dados eleitorais."""

import logging
from pathlib import Path

import duckdb

from ..config import DB_DIR, RAW_DIR, REGIOES, TIMEZONE_OFFSETS

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = DB_DIR / "eleicoes_2022.duckdb"

# Mapeamento UF -> Regiao
_UF_REGIAO = {}
for regiao, ufs in REGIOES.items():
    for uf in ufs:
        _UF_REGIAO[uf] = regiao


class DuckDBStore:
    """Gerencia persistencia e consultas em DuckDB."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH, read_only: bool = False):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._read_only = read_only
        self._conn = duckdb.connect(str(db_path), read_only=read_only)
        if not read_only:
            self._create_tables()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _create_tables(self):
        """Cria schema do banco."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS secoes (
                id VARCHAR PRIMARY KEY,
                turno INTEGER DEFAULT 1,
                uf VARCHAR,
                regiao VARCHAR,
                municipio VARCHAR,
                zona VARCHAR,
                secao VARCHAR,
                modelo_urna VARCHAR,
                tipo_urna VARCHAR,
                versao_sw VARCHAR,
                fuso_offset INTEGER,
                -- BU data
                emissao VARCHAR,
                eleitores_aptos INTEGER,
                comparecimento INTEGER,
                lib_codigo INTEGER,
                comp_biometrico INTEGER,
                pct_biometria FLOAT,
                pct_abstencao FLOAT,
                -- Timing (from log)
                hora_abertura VARCHAR,
                hora_encerramento VARCHAR,
                duracao_min INTEGER,
                -- Log stats
                reboots INTEGER DEFAULT 0,
                erros_log INTEGER DEFAULT 0,
                alertas_mesario INTEGER DEFAULT 0,
                votos_log INTEGER DEFAULT 0,
                substituicoes INTEGER DEFAULT 0,
                -- Flags
                is_reserva BOOLEAN DEFAULT FALSE,
                has_issues BOOLEAN DEFAULT FALSE,
                n_issues INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS votos (
                secao_id VARCHAR,
                eleicao_id INTEGER,
                cargo VARCHAR,
                codigo_cargo INTEGER,
                tipo_voto VARCHAR,
                codigo_candidato INTEGER,
                partido INTEGER,
                quantidade INTEGER,
                FOREIGN KEY (secao_id) REFERENCES secoes(id)
            );

            CREATE TABLE IF NOT EXISTS issues (
                id INTEGER,
                secao_id VARCHAR,
                codigo VARCHAR,
                severidade VARCHAR,
                descricao VARCHAR,
                base_legal VARCHAR,
                detalhes VARCHAR,
                FOREIGN KEY (secao_id) REFERENCES secoes(id)
            );

            CREATE TABLE IF NOT EXISTS totais_cargo (
                secao_id VARCHAR,
                eleicao_id INTEGER,
                cargo VARCHAR,
                codigo_cargo INTEGER,
                comparecimento INTEGER,
                nominais INTEGER,
                brancos INTEGER,
                nulos INTEGER,
                legenda INTEGER,
                total INTEGER,
                FOREIGN KEY (secao_id) REFERENCES secoes(id)
            );

            CREATE SEQUENCE IF NOT EXISTS issue_seq START 1;
        """)

    def clear(self):
        """Limpa todos os dados (para rebuild)."""
        self._conn.execute("DELETE FROM votos")
        self._conn.execute("DELETE FROM issues")
        self._conn.execute("DELETE FROM totais_cargo")
        self._conn.execute("DELETE FROM secoes")
        self._conn.execute("DROP SEQUENCE IF EXISTS issue_seq")
        self._conn.execute("CREATE SEQUENCE issue_seq START 1")

    def build_from_analysis(self, batch_results: dict):
        """Popula o banco a partir dos resultados do BatchAnalyzer.

        Args:
            batch_results: dict retornado por BatchAnalyzer.analyze_all()
        """
        self.clear()
        logger.info("Populando DuckDB com %d secoes...", batch_results["total_secoes"])

        from ..analyzer.batch import BatchAnalyzer

        analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
        bu_files = analyzer.find_bu_files()

        for i, bu_file in enumerate(bu_files):
            if (i + 1) % 100 == 0:
                logger.info("DuckDB: %d/%d BUs", i + 1, len(bu_files))
            self._process_section(analyzer, bu_file.parent, bu_file=bu_file)

        self._conn.execute("CHECKPOINT")
        count = self._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
        logger.info("DuckDB populado: %d secoes", count)

    def build_from_raw(self):
        """Popula o banco re-analisando todos os dados brutos."""
        from ..analyzer.batch import BatchAnalyzer

        self.clear()
        analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
        bu_files = analyzer.find_bu_files()
        logger.info("Processando %d arquivos BU para DuckDB...", len(bu_files))

        for i, bu_file in enumerate(bu_files):
            if (i + 1) % 100 == 0:
                logger.info("DuckDB: %d/%d BUs", i + 1, len(bu_files))
            self._process_section(analyzer, bu_file.parent, bu_file=bu_file)

        self._conn.execute("CHECKPOINT")
        count = self._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
        t1 = self._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=1").fetchone()[0]
        t2 = self._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=2").fetchone()[0]
        logger.info("DuckDB populado: %d seções (1T: %d, 2T: %d)", count, t1, t2)
        return count

    def build_incremental(self):
        """Compila apenas BUs novos (não presentes no banco).

        Muito mais rápido que build_from_raw() para atualizações parciais,
        pois pula seções já compiladas.
        """
        from ..analyzer.batch import BatchAnalyzer

        # Pegar secao_ids existentes
        existing = set()
        rows = self._conn.execute("SELECT id FROM secoes").fetchall()
        for row in rows:
            existing.add(row[0])
        logger.info("DuckDB tem %d seções existentes", len(existing))

        analyzer = BatchAnalyzer(raw_dir=RAW_DIR)
        all_bu_files = analyzer.find_bu_files()
        logger.info("Total de BUs no disco: %d", len(all_bu_files))

        # Filtrar apenas novos
        new_bu_files = []
        for bu_file in all_bu_files:
            secao_id = self._bu_file_to_secao_id(bu_file, analyzer)
            if secao_id and secao_id not in existing:
                new_bu_files.append(bu_file)

        logger.info("BUs novos para compilar: %d (pulando %d existentes)",
                     len(new_bu_files), len(all_bu_files) - len(new_bu_files))

        if not new_bu_files:
            logger.info("Nenhum BU novo. Banco já está atualizado.")
            count = self._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
            return count

        processed = 0
        errors = 0
        for i, bu_file in enumerate(new_bu_files):
            if (i + 1) % 100 == 0:
                logger.info("DuckDB incremental: %d/%d novos BUs", i + 1, len(new_bu_files))
            try:
                self._process_section(analyzer, bu_file.parent, bu_file=bu_file)
                processed += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.warning("Erro ao processar %s: %s", bu_file, e)

            # Checkpoint a cada 1000 seções para não acumular WAL
            if (i + 1) % 1000 == 0:
                self._conn.execute("CHECKPOINT")

        self._conn.execute("CHECKPOINT")
        count = self._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
        t1 = self._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=1").fetchone()[0]
        t2 = self._conn.execute("SELECT COUNT(*) FROM secoes WHERE turno=2").fetchone()[0]
        logger.info("DuckDB incremental: +%d seções (%d erros). Total: %d (1T: %d, 2T: %d)",
                     processed, errors, count, t1, t2)
        return count

    @staticmethod
    def _bu_file_to_secao_id(bu_file: Path, analyzer) -> str:
        """Calcula secao_id a partir do path do BU sem parsing ASN.1."""
        # Path: data/raw/{uf}/{mun}/{zona}/{secao}/o00{pleito}-{mun}{zona}{secao}.bu
        parts = bu_file.parent.parts
        try:
            # Encontrar 'raw' no path e pegar os 4 componentes seguintes
            raw_idx = parts.index("raw")
            uf = parts[raw_idx + 1]
            mun = parts[raw_idx + 2]
            zona = parts[raw_idx + 3]
            secao = parts[raw_idx + 4]
        except (ValueError, IndexError):
            return ""

        pleito = analyzer.extract_pleito_from_filename(bu_file)
        turno = analyzer.pleito_to_turno(pleito)
        return f"{turno}T/{uf}/{mun}/{zona}/{secao}"

    def _process_section(self, analyzer, section_dir: Path, bu_file: Path = None):
        """Processa uma secao e insere no DuckDB."""
        result = analyzer._analyze_section(section_dir, bu_file=bu_file)
        uf = result["uf"]
        turno = result.get("turno", 1)
        secao_id = f"{turno}T/{uf}/{result['municipio']}/{result['zona']}/{result['secao']}"

        # Dados basicos
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

        self._conn.execute("""
            INSERT OR REPLACE INTO secoes VALUES (
                ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?
            )
        """, [
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
        ])

        # Issues
        for issue in issues:
            self._conn.execute("""
                INSERT INTO issues VALUES (
                    nextval('issue_seq'), ?, ?, ?, ?, ?, ?
                )
            """, [
                secao_id, issue.codigo, issue.severidade.value,
                issue.descricao, issue.base_legal,
                str(issue.detalhes) if issue.detalhes else None,
            ])

        # Votos e totais por cargo
        bu_obj = result.get("bu_obj")
        if bu_obj:
            for eleicao in bu_obj.resultados_por_eleicao:
                for cargo in eleicao.resultados:
                    nominais = 0
                    brancos = 0
                    nulos = 0
                    legenda = 0

                    for voto in cargo.votos:
                        tipo = voto.tipo_voto.value
                        self._conn.execute("""
                            INSERT INTO votos VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
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
                    self._conn.execute("""
                        INSERT INTO totais_cargo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        secao_id, eleicao.id_eleicao,
                        cargo.nome_cargo, cargo.codigo_cargo,
                        cargo.comparecimento, nominais, brancos, nulos, legenda, total,
                    ])

    # ============================================================
    # Consultas para o Dashboard
    # ============================================================

    def query(self, sql: str, params=None):
        """Executa query e retorna como lista de dicts."""
        result = self._conn.execute(sql, params or [])
        cols = [desc[0] for desc in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    def query_df(self, sql: str, params=None):
        """Executa query e retorna como DataFrame."""
        return self._conn.execute(sql, params or []).fetchdf()

    def get_summary(self) -> dict:
        """Resumo geral para a pagina principal do BI."""
        return {
            "total_secoes": self._conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0],
            "total_issues": self._conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0],
            "secoes_ok": self._conn.execute("SELECT COUNT(*) FROM secoes WHERE NOT has_issues").fetchone()[0],
            "secoes_com_issues": self._conn.execute("SELECT COUNT(*) FROM secoes WHERE has_issues").fetchone()[0],
            "total_eleitores": self._conn.execute("SELECT COALESCE(SUM(eleitores_aptos),0) FROM secoes").fetchone()[0],
            "total_comparecimento": self._conn.execute("SELECT COALESCE(SUM(comparecimento),0) FROM secoes").fetchone()[0],
            "total_reboots": self._conn.execute("SELECT COALESCE(SUM(reboots),0) FROM secoes").fetchone()[0],
            "ufs": self._conn.execute("SELECT COUNT(DISTINCT uf) FROM secoes").fetchone()[0],
        }

    def get_filter_options(self) -> dict:
        """Opcoes disponiveis para filtros do dashboard."""
        return {
            "turnos": [r[0] for r in self._conn.execute("SELECT DISTINCT turno FROM secoes ORDER BY turno").fetchall()],
            "ufs": [r[0] for r in self._conn.execute("SELECT DISTINCT uf FROM secoes ORDER BY uf").fetchall()],
            "regioes": [r[0] for r in self._conn.execute("SELECT DISTINCT regiao FROM secoes ORDER BY regiao").fetchall()],
            "modelos": [r[0] for r in self._conn.execute("SELECT DISTINCT modelo_urna FROM secoes WHERE modelo_urna IS NOT NULL ORDER BY modelo_urna").fetchall()],
            "severidades": [r[0] for r in self._conn.execute("SELECT DISTINCT severidade FROM issues ORDER BY severidade").fetchall()],
            "codigos_issue": [r[0] for r in self._conn.execute("SELECT DISTINCT codigo FROM issues ORDER BY codigo").fetchall()],
        }
