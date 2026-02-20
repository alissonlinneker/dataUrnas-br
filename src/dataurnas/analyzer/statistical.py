"""Analise estatistica de dados de votacao."""

import logging
import math
from collections import defaultdict
from typing import Optional

from ..models import BoletimUrna, Issue, IssueSeverity

logger = logging.getLogger(__name__)

# Limiares para deteccao de anomalias
LIMIAR_NULOS_PCT = 15.0  # % de nulos acima disso e anomalo
LIMIAR_BRANCOS_PCT = 10.0  # % de brancos acima disso e anomalo
LIMIAR_ABSTENCAO_PCT = 40.0  # % de abstencao acima disso para alertar
LIMIAR_BIOMETRIA_PCT = 30.0  # % de liberacao por codigo (sem biometria) acima disso


class StatisticalAnalyzer:
    """Analise estatistica de padroes de votacao."""

    def analyze_bu(self, bu: BoletimUrna) -> list[Issue]:
        """Analisa um BU individual para padroes anomalos."""
        issues = []
        issues.extend(self._check_nulos_brancos(bu))
        issues.extend(self._check_biometria(bu))
        issues.extend(self._check_abstencao(bu))
        return issues

    def _check_nulos_brancos(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica proporcao de votos nulos e brancos."""
        issues = []

        for eleicao in bu.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                total = sum(v.quantidade for v in resultado.votos)
                if total == 0:
                    continue

                nulos = sum(
                    v.quantidade for v in resultado.votos
                    if v.tipo_voto.value == "nulo"
                )
                brancos = sum(
                    v.quantidade for v in resultado.votos
                    if v.tipo_voto.value == "branco"
                )

                pct_nulos = (nulos / total) * 100
                pct_brancos = (brancos / total) * 100

                if pct_nulos > LIMIAR_NULOS_PCT:
                    issues.append(
                        Issue(
                            codigo="I03",
                            severidade=IssueSeverity.INFO,
                            descricao=(
                                f"Proporcao elevada de nulos: {pct_nulos:.1f}% "
                                f"para {resultado.nome_cargo} "
                                f"({nulos}/{total})"
                            ),
                            uf=str(bu.uf),
                            municipio=str(bu.municipio),
                            zona=str(bu.zona),
                            secao=str(bu.secao),
                            detalhes={
                                "cargo": resultado.nome_cargo,
                                "nulos": nulos,
                                "total": total,
                                "percentual": round(pct_nulos, 2),
                            },
                        )
                    )

                if pct_brancos > LIMIAR_BRANCOS_PCT:
                    issues.append(
                        Issue(
                            codigo="I04",
                            severidade=IssueSeverity.INFO,
                            descricao=(
                                f"Proporcao elevada de brancos: {pct_brancos:.1f}% "
                                f"para {resultado.nome_cargo} "
                                f"({brancos}/{total})"
                            ),
                            uf=str(bu.uf),
                            municipio=str(bu.municipio),
                            zona=str(bu.zona),
                            secao=str(bu.secao),
                            detalhes={
                                "cargo": resultado.nome_cargo,
                                "brancos": brancos,
                                "total": total,
                                "percentual": round(pct_brancos, 2),
                            },
                        )
                    )

        return issues

    def _check_biometria(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica proporcao de liberacao por codigo vs biometria."""
        issues = []

        lib_codigo = bu.qtd_eleitores_lib_codigo
        comp_bio = bu.qtd_eleitores_comp_biometrico

        total = lib_codigo + comp_bio
        if total == 0:
            return issues

        pct_codigo = (lib_codigo / total) * 100

        if pct_codigo > LIMIAR_BIOMETRIA_PCT:
            issues.append(
                Issue(
                    codigo="M06",
                    severidade=IssueSeverity.MEDIUM,
                    descricao=(
                        f"Alta taxa de liberacao por codigo (sem biometria): "
                        f"{pct_codigo:.1f}% ({lib_codigo}/{total})"
                    ),
                    uf=str(bu.uf),
                    municipio=str(bu.municipio),
                    zona=str(bu.zona),
                    secao=str(bu.secao),
                    detalhes={
                        "lib_codigo": lib_codigo,
                        "comp_biometrico": comp_bio,
                        "total": total,
                        "percentual": round(pct_codigo, 2),
                    },
                    base_legal="Resolucao TSE 23.669/2021 - Biometria obrigatoria",
                )
            )

        return issues

    def _check_abstencao(self, bu: BoletimUrna) -> list[Issue]:
        """Verifica taxa de abstencao."""
        issues = []

        for eleicao in bu.resultados_por_eleicao:
            aptos = eleicao.eleitores_aptos
            if aptos == 0:
                continue

            # Pegar maior comparecimento entre cargos
            max_comp = max(
                (r.comparecimento for r in eleicao.resultados if r.comparecimento > 0),
                default=0,
            )

            if max_comp == 0:
                continue

            abstencao = ((aptos - max_comp) / aptos) * 100

            if abstencao > LIMIAR_ABSTENCAO_PCT:
                issues.append(
                    Issue(
                        codigo="I05",
                        severidade=IssueSeverity.INFO,
                        descricao=(
                            f"Alta taxa de abstencao: {abstencao:.1f}% "
                            f"({aptos - max_comp}/{aptos}) na eleicao {eleicao.id_eleicao}"
                        ),
                        uf=str(bu.uf),
                        municipio=str(bu.municipio),
                        zona=str(bu.zona),
                        secao=str(bu.secao),
                        detalhes={
                            "eleicao": eleicao.id_eleicao,
                            "aptos": aptos,
                            "comparecimento": max_comp,
                            "abstencao_pct": round(abstencao, 2),
                        },
                    )
                )

        return issues


class VoteAggregator:
    """Agrega votos por candidato atraves de multiplas secoes."""

    def __init__(self):
        self._votos: dict[int, dict[str, dict]] = defaultdict(
            lambda: defaultdict(lambda: {
                "votos": 0,
                "partido": None,
                "secoes": 0,
            })
        )
        self._totais_cargo: dict[str, dict] = defaultdict(
            lambda: {
                "nominais": 0,
                "brancos": 0,
                "nulos": 0,
                "legenda": 0,
                "total": 0,
                "comparecimento": 0,
                "secoes": 0,
            }
        )
        self._por_uf: dict[str, dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )

    def add_bu(self, bu: BoletimUrna, uf: str = ""):
        """Adiciona dados de um BU a agregacao."""
        for eleicao in bu.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                cargo = resultado.nome_cargo
                self._totais_cargo[cargo]["comparecimento"] += resultado.comparecimento
                self._totais_cargo[cargo]["secoes"] += 1

                for voto in resultado.votos:
                    tipo = voto.tipo_voto.value
                    qtd = voto.quantidade
                    self._totais_cargo[cargo]["total"] += qtd

                    if tipo == "nominal":
                        self._totais_cargo[cargo]["nominais"] += qtd
                        cod = voto.codigo_votavel or 0
                        self._votos[cargo][str(cod)]["votos"] += qtd
                        self._votos[cargo][str(cod)]["partido"] = voto.partido
                        self._votos[cargo][str(cod)]["secoes"] += 1

                        if uf:
                            self._por_uf[uf][cargo][str(cod)] += qtd
                    elif tipo == "branco":
                        self._totais_cargo[cargo]["brancos"] += qtd
                    elif tipo == "nulo":
                        self._totais_cargo[cargo]["nulos"] += qtd
                    elif tipo == "legenda":
                        self._totais_cargo[cargo]["legenda"] += qtd

    def get_results(self) -> dict:
        """Retorna resultados agregados."""
        results = {
            "totais_por_cargo": {},
            "votos_por_candidato": {},
            "votos_por_uf": {},
        }

        for cargo, totais in sorted(self._totais_cargo.items()):
            results["totais_por_cargo"][cargo] = dict(totais)

        for cargo, candidatos in sorted(self._votos.items()):
            # Ordenar por votos (descendente)
            sorted_cands = sorted(
                candidatos.items(),
                key=lambda x: x[1]["votos"],
                reverse=True,
            )
            results["votos_por_candidato"][cargo] = [
                {
                    "codigo": cod,
                    "partido": info["partido"],
                    "votos": info["votos"],
                    "secoes": info["secoes"],
                }
                for cod, info in sorted_cands
            ]

        # Agregacao por UF - top candidatos por cargo
        for uf, cargos in sorted(self._por_uf.items()):
            results["votos_por_uf"][uf] = {}
            for cargo, candidatos in sorted(cargos.items()):
                sorted_cands = sorted(
                    candidatos.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )
                results["votos_por_uf"][uf][cargo] = [
                    {"codigo": cod, "votos": qtd}
                    for cod, qtd in sorted_cands[:10]
                ]

        return results


def compute_outlier_scores(values: list[float]) -> list[float]:
    """Calcula z-scores para deteccao de outliers.

    Returns:
        Lista de z-scores (abs). Valores > 2.0 sao potenciais outliers.
    """
    if len(values) < 3:
        return [0.0] * len(values)

    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 1.0

    return [abs((x - mean) / std) for x in values]
