"""Analise em lote de todas as secoes baixadas."""

import logging
from collections import defaultdict
from pathlib import Path

from ..config import RAW_DIR
from ..parsers.bu import BUParser
from ..parsers.log import LogParser
from ..parsers.signature import SignatureParser
from .integrity import ConsistencyAnalyzer, IntegrityAnalyzer
from .log_analyzer import LogAnalyzer
from .statistical import StatisticalAnalyzer, VoteAggregator

logger = logging.getLogger(__name__)


class BatchAnalyzer:
    """Executa analise em lote em todas as secoes baixadas."""

    def __init__(self, raw_dir: Path = RAW_DIR):
        self._raw_dir = raw_dir
        self._bu_parser = BUParser()
        self._log_parser = LogParser()
        self._sig_parser = SignatureParser()
        self._consistency = ConsistencyAnalyzer()
        self._integrity = IntegrityAnalyzer()
        self._log_analyzer = LogAnalyzer()
        self._statistical = StatisticalAnalyzer()
        self._vote_aggregator = VoteAggregator()

    def find_sections(self, uf: str = None) -> list[Path]:
        """Encontra todas as secoes baixadas (diretórios únicos)."""
        sections = []
        base = self._raw_dir
        if uf:
            base = base / uf.lower()

        # Estrutura: data/raw/{uf}/{mun}/{zona}/{secao}/
        for bu_file in sorted(base.rglob("*.bu")):
            sections.append(bu_file.parent)

        # V2 format
        for bu_file in sorted(base.rglob("*-bu.dat")):
            if bu_file.parent not in sections:
                sections.append(bu_file.parent)

        return list(set(sections))

    def find_bu_files(self, uf: str = None) -> list[Path]:
        """Encontra todos os arquivos BU individuais (um por turno/seção)."""
        bu_files = []
        base = self._raw_dir
        if uf:
            base = base / uf.lower()

        bu_files.extend(sorted(base.rglob("*.bu")))
        bu_files.extend(sorted(base.rglob("*-bu.dat")))
        return bu_files

    @staticmethod
    def extract_pleito_from_filename(bu_path: Path) -> str:
        """Extrai o código do pleito do nome do arquivo BU.

        Ex: 'o00406-9701200170097.bu' -> '406'
            'o00407-9701200170097.bu' -> '407'
        """
        name = bu_path.name
        if name.startswith("o00"):
            # Formato v1: o00{pleito}-{resto}.bu
            return name[3:].split("-")[0]
        return ""

    @staticmethod
    def pleito_to_turno(pleito: str) -> int:
        """Converte código de pleito para número do turno."""
        mapping = {"406": 1, "407": 2, "452": 1, "453": 2}
        return mapping.get(pleito, 1)

    @staticmethod
    def pleito_to_date(pleito: str) -> str:
        """Converte código de pleito para data do turno (DD/MM/YYYY).

        Necessario para filtrar logs do T2, que acumulam eventos do T1.
        """
        mapping = {"406": "02/10/2022", "407": "30/10/2022"}
        return mapping.get(pleito, "")

    def analyze_all(self, uf: str = None) -> dict:
        """Analisa todas as secoes e gera relatorio completo."""
        sections = self.find_sections(uf)
        logger.info("Encontradas %d secoes para analise", len(sections))

        results = {
            "total_secoes": len(sections),
            "secoes_ok": 0,
            "secoes_com_issues": 0,
            "total_issues": 0,
            "issues_por_severidade": defaultdict(int),
            "issues_por_codigo": defaultdict(int),
            "issues_por_uf": defaultdict(int),
            "modelos_urna": defaultdict(int),
            "tipos_urna": defaultdict(int),
            "versoes_sw": defaultdict(int),
            "eleitores_por_uf": defaultdict(
                lambda: {"aptos": 0, "comparecimento": 0, "secoes": 0}
            ),
            "biometria_por_uf": defaultdict(
                lambda: {"lib_codigo": 0, "comp_biometrico": 0}
            ),
            "timing": {
                "aberturas": [],
                "encerramentos": [],
                "duracoes_min": [],
            },
            "secoes_analisadas": [],
            "issues": [],
        }

        for i, section_dir in enumerate(sections):
            if (i + 1) % 50 == 0:
                logger.info("Progresso: %d/%d secoes", i + 1, len(sections))

            section_result = self._analyze_section(section_dir)
            self._aggregate(results, section_result)

        results["abstencao_por_uf"] = self._calc_abstencao(results)
        results["biometria_por_uf_pct"] = self._calc_biometria_pct(results)
        results["votacao"] = self._vote_aggregator.get_results()

        # Converter defaultdicts para dicts
        for key in ["issues_por_severidade", "issues_por_codigo", "issues_por_uf",
                     "modelos_urna", "tipos_urna", "versoes_sw"]:
            results[key] = dict(results[key])
        results["eleitores_por_uf"] = {k: dict(v) for k, v in results["eleitores_por_uf"].items()}
        results["biometria_por_uf"] = {k: dict(v) for k, v in results["biometria_por_uf"].items()}

        return results

    def _analyze_section(self, section_dir: Path, bu_file: Path = None) -> dict:
        """Analisa uma secao individual com todos os analyzers.

        Args:
            section_dir: Diretório da seção.
            bu_file: Arquivo BU específico. Se None, usa o primeiro encontrado.
        """
        # Extrair UF/mun/zona/secao do caminho
        parts = section_dir.parts
        raw_idx = None
        for i, p in enumerate(parts):
            if p == "raw":
                raw_idx = i
                break

        if raw_idx is not None and len(parts) > raw_idx + 4:
            uf = parts[raw_idx + 1]
            municipio = parts[raw_idx + 2]
            zona = parts[raw_idx + 3]
            secao = parts[raw_idx + 4]
        else:
            uf = parts[-4] if len(parts) >= 4 else ""
            municipio = parts[-3] if len(parts) >= 3 else ""
            zona = parts[-2] if len(parts) >= 2 else ""
            secao = parts[-1] if len(parts) >= 1 else ""

        # Determinar turno a partir do arquivo BU
        turno = 1
        turno_date = ""
        file_prefix = None
        if bu_file:
            pleito = self.extract_pleito_from_filename(bu_file)
            turno = self.pleito_to_turno(pleito)
            turno_date = self.pleito_to_date(pleito)
            # Prefixo para encontrar arquivos correspondentes (log, vscmr, etc.)
            file_prefix = bu_file.stem  # ex: o00406-9701200170097

        result = {
            "dir": str(section_dir),
            "uf": uf,
            "municipio": municipio,
            "zona": zona,
            "secao": secao,
            "turno": turno,
            "bu": None,
            "bu_obj": None,
            "modelo": None,
            "tipo_urna": None,
            "versao_sw": None,
            "issues": [],
            "log_timing": None,
            "log_events": None,
        }

        # === Parse BU ===
        if bu_file is None:
            bu_files = list(section_dir.glob("*.bu")) + list(section_dir.glob("*-bu.dat"))
            bu_file = bu_files[0] if bu_files else None

        bu = None
        if bu_file and bu_file.exists():
            try:
                bu = self._bu_parser.parse(bu_file)
                result["bu_obj"] = bu
                result["tipo_urna"] = bu.tipo_urna
                result["versao_sw"] = bu.versao_votacao
                result["bu"] = {
                    "municipio": bu.municipio,
                    "zona": bu.zona,
                    "secao": bu.secao,
                    "tipo_urna": bu.tipo_urna,
                    "versao": bu.versao_votacao,
                    "emissao": bu.data_hora_emissao,
                    "lib_codigo": bu.qtd_eleitores_lib_codigo,
                    "comp_biometrico": bu.qtd_eleitores_comp_biometrico,
                    "eleicoes": [],
                }
                for eleicao in bu.resultados_por_eleicao:
                    el_info = {
                        "id": eleicao.id_eleicao,
                        "aptos": eleicao.eleitores_aptos,
                        "cargos": [],
                    }
                    for cargo in eleicao.resultados:
                        nominais = sum(
                            v.quantidade for v in cargo.votos
                            if v.tipo_voto.value == "nominal"
                        )
                        brancos = sum(
                            v.quantidade for v in cargo.votos
                            if v.tipo_voto.value == "branco"
                        )
                        nulos = sum(
                            v.quantidade for v in cargo.votos
                            if v.tipo_voto.value == "nulo"
                        )
                        legenda = sum(
                            v.quantidade for v in cargo.votos
                            if v.tipo_voto.value == "legenda"
                        )
                        total_votos = nominais + brancos + nulos + legenda
                        el_info["cargos"].append({
                            "cargo": cargo.nome_cargo,
                            "codigo": cargo.codigo_cargo,
                            "comparecimento": cargo.comparecimento,
                            "total_votos": total_votos,
                            "nominais": nominais,
                            "brancos": brancos,
                            "nulos": nulos,
                            "legenda": legenda,
                        })
                    result["bu"]["eleicoes"].append(el_info)

                # Consistencia
                issues = self._consistency.full_analysis(bu)
                for issue in issues:
                    issue.uf = result["uf"]
                    issue.municipio = result["municipio"]
                    issue.zona = result["zona"]
                    issue.secao = result["secao"]
                result["issues"].extend(issues)

                # Analise estatistica
                stat_issues = self._statistical.analyze_bu(bu)
                for issue in stat_issues:
                    issue.uf = result["uf"]
                    issue.municipio = result["municipio"]
                    issue.zona = result["zona"]
                    issue.secao = result["secao"]
                result["issues"].extend(stat_issues)

                # Agregar votos por candidato
                self._vote_aggregator.add_bu(bu, uf=uf)

            except Exception as e:
                logger.error("Erro ao parsear BU em %s: %s", section_dir, e)

        # === Modelo da urna: prioridade signature > log ===
        if file_prefix:
            sig_files = list(section_dir.glob(f"{file_prefix}.vscmr")) + \
                        list(section_dir.glob(f"{file_prefix}-vota.vsc"))
        else:
            sig_files = list(section_dir.glob("*.vscmr")) + list(section_dir.glob("*-vota.vsc"))
        if sig_files:
            try:
                sig_data = self._sig_parser.parse(sig_files[0])
                modelo_sig = sig_data.get("modeloUrna", "")
                if modelo_sig:
                    # Normalizar: "ue2010" -> "2010"
                    modelo_clean = modelo_sig.lower().replace("ue", "").strip()
                    result["modelo"] = modelo_clean
            except Exception as e:
                logger.debug("Erro ao extrair modelo de signature em %s: %s", section_dir, e)

        # === Parse Log ===
        if file_prefix:
            log_files = list(section_dir.glob(f"{file_prefix}.logjez")) + \
                        list(section_dir.glob(f"{file_prefix}-log.jez"))
        else:
            log_files = list(section_dir.glob("*.logjez")) + list(section_dir.glob("*-log.jez"))
        if log_files:
            try:
                log_result = self._log_analyzer.analyze_log(
                    log_files[0], bu, uf=uf,
                    turno_date=turno_date or None,
                )
                events = log_result["events"]
                result["log_events"] = {
                    "modelo": events.get("modelo"),
                    "votos_computados": events.get("votos_computados", 0),
                    "erros": len(events.get("erros", [])),
                    "alertas_mesario": len(events.get("alertas_mesario", [])),
                    "substituicoes": len(events.get("substituicoes", [])),
                    "reboots": events.get("reboots", 0),
                    "severidades": events.get("severidades", {}),
                }
                result["log_timing"] = log_result["timing"]

                # Modelo do log como fallback
                if not result["modelo"] and events.get("modelo"):
                    result["modelo"] = events["modelo"]

                # Issues do log
                for issue in log_result["issues"]:
                    issue.uf = result["uf"]
                    issue.municipio = result["municipio"]
                    issue.zona = result["zona"]
                    issue.secao = result["secao"]
                result["issues"].extend(log_result["issues"])

            except Exception as e:
                logger.debug("Erro ao analisar log em %s: %s", section_dir, e)

        # === Hashes (desativado por padrao para performance) ===
        # h_issues = self._integrity.verify_section_hashes(section_dir)
        # result["issues"].extend(h_issues)

        return result

    def _aggregate(self, results: dict, section_result: dict):
        """Agrega resultado de uma secao no resumo geral."""
        uf = section_result["uf"]
        issues = section_result["issues"]

        if issues:
            results["secoes_com_issues"] += 1
            for issue in issues:
                results["total_issues"] += 1
                results["issues_por_severidade"][issue.severidade.value] += 1
                results["issues_por_codigo"][issue.codigo] += 1
                results["issues_por_uf"][uf] += 1
                results["issues"].append({
                    "codigo": issue.codigo,
                    "severidade": issue.severidade.value,
                    "descricao": issue.descricao,
                    "uf": uf,
                    "municipio": section_result["municipio"],
                    "zona": section_result["zona"],
                    "secao": section_result["secao"],
                    "detalhes": issue.detalhes,
                    "base_legal": issue.base_legal,
                })
        else:
            results["secoes_ok"] += 1

        # Modelo de urna (agora vem do .vscmr ou log, nao do BU)
        modelo = section_result.get("modelo")
        if modelo:
            results["modelos_urna"][str(modelo)] += 1

        # Tipo de urna (secao/reservaSecao/contingencia)
        tipo_urna = section_result.get("tipo_urna")
        if tipo_urna:
            results["tipos_urna"][str(tipo_urna)] += 1

        # Versao de SW
        versao = section_result.get("versao_sw")
        if versao:
            results["versoes_sw"][str(versao)] += 1

        # Votos e eleitores (corrigido: sem double-counting)
        bu_info = section_result.get("bu")
        if bu_info:
            # Aptos: pegar da primeira eleicao (e o mesmo caderno)
            eleicoes = bu_info.get("eleicoes", [])
            if eleicoes:
                # Usar aptos da primeira eleicao (mesmo caderno para todos os cargos)
                results["eleitores_por_uf"][uf]["aptos"] += eleicoes[0]["aptos"]
                results["eleitores_por_uf"][uf]["secoes"] += 1

                # Comparecimento: usar o maior valor de comparecimento
                # entre todos os cargos (deve ser igual para todos)
                max_comp = 0
                for eleicao in eleicoes:
                    for cargo in eleicao.get("cargos", []):
                        comp = cargo.get("comparecimento", 0)
                        if comp > max_comp:
                            max_comp = comp
                results["eleitores_por_uf"][uf]["comparecimento"] += max_comp

            # Biometria
            results["biometria_por_uf"][uf]["lib_codigo"] += bu_info.get("lib_codigo", 0)
            results["biometria_por_uf"][uf]["comp_biometrico"] += bu_info.get("comp_biometrico", 0)

        # Timing
        timing = section_result.get("log_timing")
        if timing:
            if timing.get("hora_abertura"):
                results["timing"]["aberturas"].append(timing["hora_abertura"])
            if timing.get("hora_encerramento"):
                results["timing"]["encerramentos"].append(timing["hora_encerramento"])
            if timing.get("duracao_votacao_min") is not None:
                results["timing"]["duracoes_min"].append(timing["duracao_votacao_min"])

        results["secoes_analisadas"].append({
            "uf": uf,
            "municipio": section_result["municipio"],
            "zona": section_result["zona"],
            "secao": section_result["secao"],
            "modelo": modelo,
            "tipo_urna": tipo_urna,
            "ok": len(issues) == 0,
            "n_issues": len(issues),
        })

    def _calc_abstencao(self, results: dict) -> dict:
        """Calcula taxa de abstencao por UF."""
        abstencao = {}
        for uf, dados in results["eleitores_por_uf"].items():
            if dados["aptos"] > 0:
                abstencao[uf] = round(
                    (1 - dados["comparecimento"] / dados["aptos"]) * 100, 2
                )
        return abstencao

    def _calc_biometria_pct(self, results: dict) -> dict:
        """Calcula percentual de liberacao por codigo (sem biometria) por UF."""
        bio_pct = {}
        for uf, dados in results["biometria_por_uf"].items():
            total = dados["lib_codigo"] + dados["comp_biometrico"]
            if total > 0:
                bio_pct[uf] = {
                    "pct_biometria": round(
                        (dados["comp_biometrico"] / total) * 100, 2
                    ),
                    "pct_codigo": round(
                        (dados["lib_codigo"] / total) * 100, 2
                    ),
                }
        return bio_pct

    def print_report(self, results: dict):
        """Imprime relatorio completo no terminal."""
        print("=" * 70)
        print("RELATORIO DE AUDITORIA - ELEICOES PRESIDENCIAIS 2022")
        print("=" * 70)
        print()

        # Resumo geral
        print(f"Total de secoes analisadas: {results['total_secoes']}")
        print(f"Secoes sem inconsistencias: {results['secoes_ok']}")
        print(f"Secoes com inconsistencias: {results['secoes_com_issues']}")
        print(f"Total de inconsistencias: {results['total_issues']}")
        print()

        # Inconsistencias por severidade
        if results.get("issues_por_severidade"):
            print("--- Inconsistencias por Severidade ---")
            order = {"critica": 0, "alta": 1, "media": 2, "informativa": 3}
            for sev, count in sorted(
                results["issues_por_severidade"].items(),
                key=lambda x: order.get(x[0], 99),
            ):
                print(f"  {sev.upper()}: {count}")
            print()

        # Inconsistencias por codigo
        if results.get("issues_por_codigo"):
            print("--- Inconsistencias por Codigo ---")
            for cod, count in sorted(results["issues_por_codigo"].items()):
                print(f"  {cod}: {count}")
            print()

        # Modelos de urna
        if results.get("modelos_urna"):
            print("--- Modelos de Urna ---")
            total_urnas = sum(results["modelos_urna"].values())
            for modelo, count in sorted(results["modelos_urna"].items()):
                pct = (count / total_urnas * 100) if total_urnas > 0 else 0
                print(f"  UE{modelo}: {count} urnas ({pct:.1f}%)")
            print()

        # Versoes de software
        if results.get("versoes_sw"):
            print("--- Versoes de Software ---")
            for versao, count in sorted(results["versoes_sw"].items()):
                print(f"  {versao}: {count} urnas")
            print()

        # Eleitores por UF
        if results.get("eleitores_por_uf"):
            print("--- Eleitores por UF (amostra) ---")
            total_aptos = 0
            total_comp = 0
            for uf in sorted(results["eleitores_por_uf"]):
                dados = results["eleitores_por_uf"][uf]
                aptos = dados["aptos"]
                comp = dados["comparecimento"]
                secoes = dados.get("secoes", 0)
                abstencao = results.get("abstencao_por_uf", {}).get(uf, 0)
                total_aptos += aptos
                total_comp += comp
                print(
                    f"  {uf.upper()}: {aptos:>6} aptos, {comp:>6} comp. "
                    f"(abstencao {abstencao:.1f}%) [{secoes} secoes]"
                )
            if total_aptos > 0:
                total_abs = (1 - total_comp / total_aptos) * 100
                print(f"  TOTAL: {total_aptos:>6} aptos, {total_comp:>6} comp. "
                      f"(abstencao {total_abs:.1f}%)")
            print()

        # Biometria por UF
        bio_pct = results.get("biometria_por_uf_pct", {})
        if bio_pct:
            print("--- Biometria por UF ---")
            for uf in sorted(bio_pct):
                dados = bio_pct[uf]
                print(
                    f"  {uf.upper()}: biometria {dados['pct_biometria']:.1f}%, "
                    f"codigo {dados['pct_codigo']:.1f}%"
                )
            print()

        # Timing
        timing = results.get("timing", {})
        duracoes = timing.get("duracoes_min", [])
        if duracoes:
            avg_dur = sum(duracoes) / len(duracoes)
            min_dur = min(duracoes)
            max_dur = max(duracoes)
            print("--- Timing de Votacao ---")
            print(f"  Duracao media: {avg_dur:.0f} min ({avg_dur/60:.1f}h)")
            print(f"  Menor duracao: {min_dur} min ({min_dur/60:.1f}h)")
            print(f"  Maior duracao: {max_dur} min ({max_dur/60:.1f}h)")
            print()

        # Votos por candidato (Presidente)
        votacao = results.get("votacao", {})
        votos_cand = votacao.get("votos_por_candidato", {})
        if "Presidente" in votos_cand:
            print("--- Votacao para Presidente (amostra) ---")
            totais = votacao.get("totais_por_cargo", {}).get("Presidente", {})
            if totais:
                print(
                    f"  Total: {totais.get('total', 0)} votos "
                    f"(nominais: {totais.get('nominais', 0)}, "
                    f"brancos: {totais.get('brancos', 0)}, "
                    f"nulos: {totais.get('nulos', 0)})"
                )
            for cand in votos_cand["Presidente"][:10]:
                total_nom = totais.get("nominais", 1) or 1
                pct = (cand["votos"] / total_nom * 100)
                print(
                    f"  Candidato {cand['codigo']} "
                    f"(partido {cand['partido']}): "
                    f"{cand['votos']} votos ({pct:.1f}%) "
                    f"[{cand['secoes']} secoes]"
                )
            print()

        # Detalhes das inconsistencias
        issues = results.get("issues", [])
        if issues:
            # Agrupar por severidade
            criticas = [i for i in issues if i.get("severidade") == "critica"]
            altas = [i for i in issues if i.get("severidade") == "alta"]
            medias = [i for i in issues if i.get("severidade") == "media"]
            infos = [i for i in issues if i.get("severidade") == "informativa"]

            if criticas:
                print("--- INCONSISTENCIAS CRITICAS ---")
                for issue in criticas:
                    self._print_issue(issue)
                print()

            if altas:
                print("--- INCONSISTENCIAS ALTAS ---")
                for issue in altas:
                    self._print_issue(issue)
                print()

            if medias:
                print("--- INCONSISTENCIAS MEDIAS ---")
                for issue in medias[:20]:
                    self._print_issue(issue)
                if len(medias) > 20:
                    print(f"  ... e mais {len(medias) - 20} inconsistencias medias")
                print()

            if infos:
                print("--- INFORMATIVAS ---")
                for issue in infos[:20]:
                    self._print_issue(issue)
                if len(infos) > 20:
                    print(f"  ... e mais {len(infos) - 20} informativas")
                print()

        print("=" * 70)

    def _print_issue(self, issue: dict):
        """Imprime uma issue formatada."""
        loc = (
            f"{issue.get('uf', '').upper()}/"
            f"{issue.get('municipio', '')}/"
            f"{issue.get('zona', '')}/"
            f"{issue.get('secao', '')}"
        )
        print(f"  [{issue['codigo']}] ({loc}): {issue['descricao']}")
        if issue.get("base_legal"):
            print(f"         Base legal: {issue['base_legal']}")
