"""Interface de linha de comando do DataUrnas-BR."""

import asyncio
import json
import logging
from pathlib import Path

import click

from .config import JSON_DIR
from .downloader.client import TSEClient
from .downloader.tse_api import TSEApi
from .downloader.manager import DownloadManager
from .parsers.bu import BUParser
from .parsers.log import LogParser
from .analyzer.integrity import IntegrityAnalyzer, ConsistencyAnalyzer


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Modo verboso")
def main(verbose):
    """DataUrnas-BR: Auditoria dos dados eleitorais brasileiros."""
    setup_logging(verbose)


# ============================================================
# Comandos de download
# ============================================================

@main.group()
def download():
    """Comandos de download de dados do TSE."""
    pass


@download.command("list")
def download_list():
    """Lista eleicoes disponiveis no TSE."""

    async def _run():
        async with TSEClient() as client:
            api = TSEApi(client)
            data = await api.list_elections()
            if not data:
                click.echo("Erro: nao foi possivel acessar a API do TSE")
                return

            click.echo(f"Ciclo atual: {data.get('c', '?')}")
            click.echo(f"Gerado em: {data.get('dg', '?')} {data.get('hg', '?')}")
            click.echo()

            for pleito in data.get("pl", []):
                click.echo(f"Pleito {pleito.get('cd', '?')}:")
                click.echo(f"  Turno: {pleito.get('t', '?')}")
                click.echo(f"  Data: {pleito.get('dt', '?')}")
                for eleicao in pleito.get("e", []):
                    click.echo(
                        f"  Eleicao {eleicao.get('cd', '?')}: {eleicao.get('nm', '?')} "
                        f"(turno {eleicao.get('t', '?')})"
                    )
                click.echo()

    asyncio.run(_run())


@download.command("state")
@click.argument("uf")
@click.option("--ciclo", default="ele2022", help="Ciclo eleitoral (default: ele2022)")
@click.option("--pleito", default="406", help="Codigo do pleito (default: 406 = Pres. 2022 1T)")
@click.option("--tipo", multiple=True, help="Tipos de arquivo: bu, rdv, logjez, imgbu, vscmr")
@click.option("--max-secoes", type=int, default=None, help="Maximo de secoes (amostra)")
@click.option("--municipio", multiple=True, help="Filtrar por codigo de municipio")
def download_state(uf, ciclo, pleito, tipo, max_secoes, municipio):
    """Baixa dados de um estado. Ex: dataurnas download state df"""

    file_types = list(tipo) if tipo else None
    mun_filter = list(municipio) if municipio else None

    async def _run():
        async with TSEClient() as client:
            api = TSEApi(client)
            manager = DownloadManager()
            stats = await manager.download_state(
                client, api, ciclo, pleito, uf,
                file_types=file_types,
                max_sections=max_secoes,
                municipio_filter=mun_filter,
            )
            click.echo(f"\nResultado: {json.dumps(stats, indent=2)}")

    asyncio.run(_run())


@download.command("sample")
@click.option("--ciclo", default="ele2022", help="Ciclo eleitoral")
@click.option("--pleito", default="406", help="Codigo do pleito")
@click.option("--secoes", default=20, help="Secoes por estado")
@click.option("--tipo", multiple=True, help="Tipos de arquivo")
@click.option(
    "--estados",
    default="ac,ce,df,sp,rs",
    help="Estados separados por virgula",
)
def download_sample(ciclo, pleito, secoes, tipo, estados):
    """Baixa amostra de dados de multiplos estados."""

    states = [s.strip() for s in estados.split(",")]
    file_types = list(tipo) if tipo else None

    async def _run():
        async with TSEClient() as client:
            api = TSEApi(client)
            manager = DownloadManager()
            stats = await manager.download_sample(
                client, api, ciclo, pleito, states,
                sections_per_state=secoes,
                file_types=file_types,
            )
            click.echo(f"\nResultado: {json.dumps(stats, indent=2)}")

    asyncio.run(_run())


@download.command("info")
@click.argument("uf")
@click.option("--ciclo", default="ele2022", help="Ciclo eleitoral")
@click.option("--pleito", default="406", help="Codigo do pleito")
def download_info(uf, ciclo, pleito):
    """Mostra informacoes de um estado (municipios, zonas, secoes)."""

    async def _run():
        async with TSEClient() as client:
            api = TSEApi(client)
            state = await api.get_state_config(ciclo, pleito, uf)
            if not state:
                click.echo(f"Erro: estado {uf} nao encontrado")
                return

            total = api.count_sections(state)
            click.echo(f"Estado: {state.nome} ({state.uf})")
            click.echo(f"Municipios: {len(state.municipios)}")
            click.echo(f"Total de secoes: {total}")
            click.echo()
            for mun in state.municipios[:20]:
                n_secoes = sum(len(z.get("sec", [])) for z in mun.get("zon", []))
                click.echo(
                    f"  {mun['cd']} {mun['nm']}: "
                    f"{len(mun.get('zon', []))} zonas, {n_secoes} secoes"
                )
            if len(state.municipios) > 20:
                click.echo(f"  ... e mais {len(state.municipios) - 20} municipios")

    asyncio.run(_run())


# ============================================================
# Comandos de analise
# ============================================================

@main.group()
def analyze():
    """Comandos de analise de dados."""
    pass


@analyze.command("bu")
@click.argument("path")
@click.option("--raw", is_flag=True, help="Mostrar dict bruto (debug)")
def analyze_bu(path, raw):
    """Analisa um arquivo BU. Ex: dataurnas analyze bu data/raw/df/.../arquivo.bu"""
    parser = BUParser()
    file_path = Path(path)

    if raw:
        data = parser.parse_to_dict(file_path)
        click.echo(json.dumps(_serialize(data), indent=2, ensure_ascii=False))
        return

    bu = parser.parse(file_path)
    click.echo(f"Municipio: {bu.municipio} | Zona: {bu.zona} | Secao: {bu.secao}")
    click.echo(f"Modelo: {bu.modelo_urna} | Fase: {bu.fase.name}")
    click.echo(f"Emissao: {bu.data_hora_emissao}")
    click.echo(f"Spec: {bu.spec_version.value}")
    click.echo()

    for eleicao in bu.resultados_por_eleicao:
        click.echo(f"Eleicao {eleicao.id_eleicao}: {eleicao.eleitores_aptos} eleitores aptos")
        for resultado in eleicao.resultados:
            click.echo(f"  {resultado.nome_cargo} (comparecimento: {resultado.comparecimento})")
            for voto in resultado.votos:
                if voto.tipo_voto.value == "nominal":
                    click.echo(
                        f"    Candidato {voto.codigo_votavel} "
                        f"(partido {voto.partido}): {voto.quantidade} votos"
                    )
                else:
                    click.echo(f"    {voto.tipo_voto.value.upper()}: {voto.quantidade}")
        click.echo()

    # Analise de consistencia
    analyzer = ConsistencyAnalyzer()
    issues = analyzer.full_analysis(bu)
    if issues:
        click.echo("=== INCONSISTENCIAS DETECTADAS ===")
        for issue in issues:
            click.echo(f"  [{issue.severidade.value}] {issue.codigo}: {issue.descricao}")
    else:
        click.echo("Nenhuma inconsistencia detectada.")


@analyze.command("hashes")
@click.argument("section_dir")
def analyze_hashes(section_dir):
    """Verifica hashes SHA-512 de uma secao."""
    analyzer = IntegrityAnalyzer()
    issues = analyzer.verify_section_hashes(Path(section_dir))

    if not issues:
        click.echo("Todos os hashes estao corretos.")
    else:
        for issue in issues:
            click.echo(f"[{issue.severidade.value}] {issue.codigo}: {issue.descricao}")
            if issue.detalhes:
                for k, v in issue.detalhes.items():
                    click.echo(f"  {k}: {v}")


@analyze.command("log")
@click.argument("path")
def analyze_log(path):
    """Analisa um arquivo de log de urna."""
    parser = LogParser()
    entries = parser.parse(Path(path))
    events = parser.extract_events(entries)

    click.echo(f"Total de entradas: {len(entries)}")
    click.echo(f"Modelo da urna: {events['modelo'] or 'nao identificado'}")

    if events["abertura"]:
        e = events["abertura"]
        click.echo(f"Abertura: {e.data} {e.hora}")
    if events["encerramento"]:
        e = events["encerramento"]
        click.echo(f"Encerramento: {e.data} {e.hora}")

    click.echo(f"Erros no log: {len(events['erros'])}")
    for err in events["erros"][:10]:
        click.echo(f"  [{err.hora}] {err.descricao[:100]}")

    if events["substituicoes"]:
        click.echo(f"Eventos de substituicao: {len(events['substituicoes'])}")
    if events["ajustes_hora"]:
        click.echo(f"Ajustes de hora: {len(events['ajustes_hora'])}")


@analyze.command("section")
@click.argument("section_dir")
def analyze_section(section_dir):
    """Analise completa de uma secao (BU + hashes + log)."""
    section_path = Path(section_dir)

    # BU
    bu_files = list(section_path.glob("*.bu")) + list(section_path.glob("*-bu.dat"))
    if bu_files:
        parser = BUParser()
        bu = parser.parse(bu_files[0])
        click.echo(f"=== BU: Mun {bu.municipio} / Zona {bu.zona} / Secao {bu.secao} ===")
        click.echo(f"Modelo: {bu.modelo_urna} | Emissao: {bu.data_hora_emissao}")
        for eleicao in bu.resultados_por_eleicao:
            click.echo(f"Eleicao {eleicao.id_eleicao}: {eleicao.eleitores_aptos} aptos")
            for r in eleicao.resultados:
                total = sum(v.quantidade for v in r.votos)
                click.echo(f"  {r.nome_cargo}: {total} votos, {r.comparecimento} comp.")

        # Consistencia
        consistency = ConsistencyAnalyzer()
        c_issues = consistency.full_analysis(bu)
    else:
        c_issues = []
        click.echo("Arquivo BU nao encontrado.")

    # Hashes
    integrity = IntegrityAnalyzer()
    h_issues = integrity.verify_section_hashes(section_path)

    # Log
    log_files = list(section_path.glob("*.logjez")) + list(section_path.glob("*-log.jez"))
    log_issues = []
    if log_files:
        log_parser = LogParser()
        entries = log_parser.parse(log_files[0])
        events = log_parser.extract_events(entries)
        click.echo(f"\n=== LOG: {len(entries)} entradas, modelo {events['modelo']} ===")
        if events["erros"]:
            click.echo(f"  {len(events['erros'])} erros registrados")

    # Resumo
    all_issues = c_issues + h_issues
    click.echo(f"\n=== RESULTADO: {len(all_issues)} inconsistencia(s) ===")
    for issue in all_issues:
        click.echo(f"  [{issue.severidade.value}] {issue.codigo}: {issue.descricao}")


@analyze.command("batch")
@click.option("--uf", default=None, help="Filtrar por estado")
@click.option("--output", "-o", default=None, help="Salvar JSON em arquivo")
@click.option("--hashes", is_flag=True, help="Verificar hashes SHA-512 (lento)")
def analyze_batch(uf, output, hashes):
    """Analise em lote de todas as secoes baixadas."""
    from .analyzer.batch import BatchAnalyzer

    analyzer = BatchAnalyzer()
    results = analyzer.analyze_all(uf=uf)
    analyzer.print_report(results)

    out_path = output or str(JSON_DIR / "relatorio_2022.json")
    serializable = json.loads(json.dumps(results, default=str))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)
    click.echo(f"\nRelatorio salvo em: {out_path}")


# ============================================================
# Comandos de banco de dados
# ============================================================

@main.group()
def db():
    """Comandos de banco de dados."""
    pass


@db.command("build")
def db_build():
    """Compila todos os dados brutos em DuckDB."""
    from .database.duckdb_store import DuckDBStore

    click.echo("Compilando dados para DuckDB...")
    with DuckDBStore() as store:
        count = store.build_from_raw()
        click.echo(f"Banco compilado: {count} secoes")
        summary = store.get_summary()
        click.echo(f"  Eleitores: {summary['total_eleitores']}")
        click.echo(f"  Comparecimento: {summary['total_comparecimento']}")
        click.echo(f"  Issues: {summary['total_issues']}")
        click.echo(f"  UFs: {summary['ufs']}")
        click.echo(f"  Reboots: {summary['total_reboots']}")


@db.command("stats")
def db_stats():
    """Mostra estatisticas do banco compilado."""
    from .database.duckdb_store import DuckDBStore, DEFAULT_DB_PATH

    if not DEFAULT_DB_PATH.exists():
        click.echo("Banco nao encontrado. Execute: dataurnas db build")
        return

    with DuckDBStore() as store:
        s = store.get_summary()
        click.echo(f"Total de secoes: {s['total_secoes']}")
        click.echo(f"Secoes OK: {s['secoes_ok']}")
        click.echo(f"Secoes com issues: {s['secoes_com_issues']}")
        click.echo(f"Total issues: {s['total_issues']}")
        click.echo(f"Eleitores: {s['total_eleitores']}")
        click.echo(f"Comparecimento: {s['total_comparecimento']}")
        click.echo(f"UFs: {s['ufs']}")


# ============================================================
# Comando de dashboard
# ============================================================

@main.command("dashboard")
@click.option("--port", default=8501, help="Porta do servidor")
def run_dashboard(port):
    """Inicia o dashboard web (Streamlit)."""
    import subprocess
    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    click.echo(f"Iniciando dashboard em http://localhost:{port}")
    subprocess.run([
        "streamlit", "run", str(dashboard_path),
        "--server.port", str(port),
        "--server.headless", "true",
    ])


def _serialize(obj):
    """Serializa objetos para JSON."""
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    return obj


if __name__ == "__main__":
    main()
