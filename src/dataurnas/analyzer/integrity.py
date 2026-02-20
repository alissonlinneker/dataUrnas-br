"""Verificacao de integridade: hashes SHA-512, assinaturas digitais."""

import hashlib
import logging
from pathlib import Path

from ..models import (
    BoletimUrna,
    Issue,
    IssueSeverity,
)
from ..parsers.signature import SignatureParser

logger = logging.getLogger(__name__)


class IntegrityAnalyzer:
    """Analisa integridade criptografica dos arquivos de urna."""

    def __init__(self):
        self._sig_parser = SignatureParser()

    def verify_section_hashes(self, section_dir: Path) -> list[Issue]:
        """Verifica hashes de todos os arquivos em uma secao.

        Procura o arquivo de assinatura (.vscmr para V1) e verifica
        o SHA-512 de cada arquivo contra o hash armazenado.
        """
        issues = []

        # Encontrar arquivo de assinatura
        sig_files = list(section_dir.glob("*.vscmr")) + list(section_dir.glob("*-vota.vsc"))
        if not sig_files:
            return issues  # Sem arquivo de assinatura para verificar

        sig_file = sig_files[0]

        try:
            results = self._sig_parser.verify_file_hashes(sig_file, section_dir)
        except Exception as e:
            logger.error("Erro ao verificar hashes em %s: %s", section_dir, e)
            issues.append(
                Issue(
                    codigo="C01",
                    severidade=IssueSeverity.CRITICAL,
                    descricao=f"Erro ao processar arquivo de assinatura: {e}",
                    uf="",
                    municipio="",
                    zona="",
                    secao="",
                    detalhes={"arquivo": str(sig_file), "erro": str(e)},
                )
            )
            return issues

        for result in results:
            if not result.valido:
                issues.append(
                    Issue(
                        codigo="C01",
                        severidade=IssueSeverity.CRITICAL,
                        descricao=f"Hash SHA-512 INVALIDO para {result.arquivo}",
                        uf="",
                        municipio="",
                        zona="",
                        secao="",
                        detalhes={
                            "arquivo": result.arquivo,
                            "hash_esperado": result.hash_esperado.hex(),
                            "hash_calculado": result.hash_calculado.hex(),
                        },
                        base_legal="Art. 59, Lei 9.504/97 - Cerimonia de Lacracao",
                    )
                )

        return issues


class ConsistencyAnalyzer:
    """Analisa consistencia logica dos dados de votacao."""

    def check_votes_vs_eligible(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica se total de votos nao excede eleitores aptos."""
        issues = []

        for eleicao in bu.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                total_votos = sum(v.quantidade for v in resultado.votos)
                if total_votos > eleicao.eleitores_aptos:
                    issues.append(
                        Issue(
                            codigo="C05",
                            severidade=IssueSeverity.CRITICAL,
                            descricao=(
                                f"Total de votos ({total_votos}) EXCEDE eleitores "
                                f"aptos ({eleicao.eleitores_aptos}) para {resultado.nome_cargo}"
                            ),
                            uf=str(bu.uf),
                            municipio=str(bu.municipio),
                            zona=str(bu.zona),
                            secao=str(bu.secao),
                            detalhes={
                                "cargo": resultado.nome_cargo,
                                "total_votos": total_votos,
                                "eleitores_aptos": eleicao.eleitores_aptos,
                                "comparecimento": resultado.comparecimento,
                                "excesso": total_votos - eleicao.eleitores_aptos,
                            },
                            base_legal="Art. 59, Lei 9.504/97",
                        )
                    )

                # Verificar comparecimento vs votos
                if resultado.comparecimento > 0 and total_votos > resultado.comparecimento:
                    issues.append(
                        Issue(
                            codigo="A02",
                            severidade=IssueSeverity.HIGH,
                            descricao=(
                                f"Total de votos ({total_votos}) excede comparecimento "
                                f"({resultado.comparecimento}) para {resultado.nome_cargo}"
                            ),
                            uf=str(bu.uf),
                            municipio=str(bu.municipio),
                            zona=str(bu.zona),
                            secao=str(bu.secao),
                            detalhes={
                                "cargo": resultado.nome_cargo,
                                "total_votos": total_votos,
                                "comparecimento": resultado.comparecimento,
                            },
                        )
                    )
        return issues

    def check_cross_election_consistency(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica consistencia entre eleicoes na mesma urna.

        Ex: eleitores aptos para presidente vs deputado federal
        devem ser iguais na mesma secao.
        """
        issues = []

        if len(bu.resultados_por_eleicao) < 2:
            return issues

        # Comparar eleitores aptos entre todas as eleicoes
        aptos_por_eleicao = {}
        for eleicao in bu.resultados_por_eleicao:
            aptos_por_eleicao[eleicao.id_eleicao] = eleicao.eleitores_aptos

        valores_aptos = list(aptos_por_eleicao.values())
        if len(set(valores_aptos)) > 1:
            issues.append(
                Issue(
                    codigo="A01",
                    severidade=IssueSeverity.HIGH,
                    descricao=(
                        f"Divergencia de eleitores aptos entre eleicoes: "
                        f"{aptos_por_eleicao}"
                    ),
                    uf=str(bu.uf),
                    municipio=str(bu.municipio),
                    zona=str(bu.zona),
                    secao=str(bu.secao),
                    detalhes={"aptos_por_eleicao": aptos_por_eleicao},
                    base_legal="Caderno de eleitores unico por secao",
                )
            )

        # Comparar comparecimento entre eleicoes
        comp_por_eleicao = {}
        for eleicao in bu.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                if resultado.comparecimento > 0:
                    key = f"{eleicao.id_eleicao}_{resultado.nome_cargo}"
                    comp_por_eleicao[key] = resultado.comparecimento

        return issues

    def check_comparecimento_consistency(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica se comparecimento e igual entre cargos da mesma eleicao.

        Na mesma urna, o comparecimento deve ser identico para todos os cargos
        da mesma eleicao (o eleitor vota em todos os cargos na mesma sessao).
        """
        issues = []

        for eleicao in bu.resultados_por_eleicao:
            comps = {}
            for resultado in eleicao.resultados:
                if resultado.comparecimento > 0:
                    comps[resultado.nome_cargo] = resultado.comparecimento

            valores = list(comps.values())
            if len(valores) >= 2 and len(set(valores)) > 1:
                issues.append(
                    Issue(
                        codigo="A06",
                        severidade=IssueSeverity.HIGH,
                        descricao=(
                            f"Comparecimento divergente entre cargos da eleicao "
                            f"{eleicao.id_eleicao}: {comps}"
                        ),
                        uf=str(bu.uf),
                        municipio=str(bu.municipio),
                        zona=str(bu.zona),
                        secao=str(bu.secao),
                        detalhes={
                            "eleicao": eleicao.id_eleicao,
                            "comparecimentos": comps,
                        },
                        base_legal="Eleitor vota em todos os cargos na mesma sessao",
                    )
                )

        return issues

    def check_tipo_urna(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica se o tipo de urna e regular (secao)."""
        issues = []

        if bu.tipo_urna and bu.tipo_urna not in ("secao", "votacaoUE"):
            issues.append(
                Issue(
                    codigo="I06",
                    severidade=IssueSeverity.INFO,
                    descricao=f"Urna nao-padrao: tipo={bu.tipo_urna}",
                    uf=str(bu.uf),
                    municipio=str(bu.municipio),
                    zona=str(bu.zona),
                    secao=str(bu.secao),
                    detalhes={"tipo_urna": bu.tipo_urna},
                )
            )

        return issues

    def full_analysis(self, bu: BoletimUrna) -> list[Issue]:
        """Executa todas as verificacoes de consistencia."""
        issues = []
        issues.extend(self.check_votes_vs_eligible(bu))
        issues.extend(self.check_cross_election_consistency(bu))
        issues.extend(self.check_comparecimento_consistency(bu))
        issues.extend(self.check_tipo_urna(bu))
        return issues
