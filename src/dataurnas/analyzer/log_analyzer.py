"""Analise de logs da urna eletronica."""

import logging
from pathlib import Path

from ..config import TIMEZONE_OFFSETS
from ..models import BoletimUrna, Issue, IssueSeverity
from ..parsers.log import LogParser

logger = logging.getLogger(__name__)

# Horarios esperados de votacao (eleicao 2022)
# Votacao e das 8h-17h BRASILIA. Logs usam hora LOCAL.
# As constantes abaixo referem-se ao horario de Brasilia;
# _adjust_for_tz() converte para hora local antes de comparar.
HORA_ABERTURA_BRT = (8, 0)   # 08:00 Brasilia
HORA_ABERTURA_TOL = 30       # Tolerancia em minutos apos 08:00
HORA_ENCERRAMENTO_BRT = (17, 0)  # 17:00 Brasilia
HORA_ENCERRAMENTO_MAX_BRT = (22, 0)  # Maximo razoavel

# Limiar para alertas de mesario (indagacao frequente pode indicar problemas)
LIMIAR_ALERTAS_MESARIO = 20


def _time_to_min(h: int, m: int) -> int:
    """Converte hora:minuto para minutos desde 00:00."""
    return h * 60 + m


def _parse_time(hora_str: str) -> tuple[int, int]:
    """Parseia HH:MM:SS para (hora, minuto)."""
    parts = hora_str.split(":")
    return int(parts[0]), int(parts[1])


def _local_limits(uf: str) -> dict:
    """Retorna horarios limites em hora LOCAL para um estado."""
    offset = TIMEZONE_OFFSETS.get(uf.lower(), 0)

    ab_h, ab_m = HORA_ABERTURA_BRT
    enc_h, enc_m = HORA_ENCERRAMENTO_BRT
    enc_max_h, enc_max_m = HORA_ENCERRAMENTO_MAX_BRT

    return {
        "abertura_min": _time_to_min(ab_h + offset - 1, ab_m),  # 1h antes
        "abertura_max": _time_to_min(ab_h + offset, ab_m + HORA_ABERTURA_TOL),
        "encerramento_min": _time_to_min(enc_h + offset, enc_m),
        "encerramento_max": _time_to_min(enc_max_h + offset, enc_max_m),
    }


class LogAnalyzer:
    """Analisa logs da urna para deteccao de anomalias."""

    def __init__(self):
        self._parser = LogParser()

    def analyze_log(self, log_path: Path, bu: BoletimUrna = None, uf: str = "",
                    turno_date: str = None) -> dict:
        """Analisa um arquivo de log e retorna resultados completos.

        Args:
            log_path: Caminho do arquivo .logjez/.jez.
            bu: Boletim de Urna parseado (para comparacao votos).
            uf: Sigla do estado (para ajuste de fuso horario).
            turno_date: Data do turno (DD/MM/YYYY) para filtrar entradas.
                Necessario para T2, cujo .logjez acumula eventos do T1.

        Returns:
            Dict com eventos, issues, e metricas de timing.
        """
        entries = self._parser.parse(log_path)
        events = self._parser.extract_events(entries, turno_date=turno_date)

        issues = []
        issues.extend(self._check_timing(events, uf))
        issues.extend(self._check_errors(events))
        issues.extend(self._check_mesario_alerts(events))

        if bu:
            issues.extend(self._check_votos_vs_log(bu, events))

        return {
            "events": events,
            "issues": issues,
            "timing": self._extract_timing(events),
        }

    def _check_timing(self, events: dict, uf: str = "") -> list[Issue]:
        """Verifica horarios de abertura e encerramento.

        Ajusta os limites para o fuso horario do estado.
        Logs usam hora local. Votacao e das 8h-17h Brasilia.
        """
        issues = []
        limits = _local_limits(uf)
        offset = TIMEZONE_OFFSETS.get(uf.lower(), 0)
        tz_note = f" (fuso {offset:+d}h)" if offset != 0 else ""

        abertura = events.get("abertura")
        if abertura:
            try:
                ab_h, ab_m = _parse_time(abertura.hora)
                ab_min = _time_to_min(ab_h, ab_m)

                if ab_min < limits["abertura_min"]:
                    issues.append(
                        Issue(
                            codigo="M01",
                            severidade=IssueSeverity.MEDIUM,
                            descricao=(
                                f"Urna pronta antes do horario: {abertura.hora}{tz_note}"
                            ),
                            uf="", municipio="", zona="", secao="",
                            detalhes={
                                "hora_abertura": abertura.hora,
                                "data": abertura.data,
                                "fuso_offset": offset,
                            },
                            base_legal="Art. 59 da Lei 9.504/97 - Abertura as 8h BRT",
                        )
                    )
                elif ab_min > limits["abertura_max"]:
                    issues.append(
                        Issue(
                            codigo="A04",
                            severidade=IssueSeverity.HIGH,
                            descricao=(
                                f"Urna aberta MUITO apos o horario: "
                                f"{abertura.hora}{tz_note}"
                            ),
                            uf="", municipio="", zona="", secao="",
                            detalhes={
                                "hora_abertura": abertura.hora,
                                "data": abertura.data,
                                "fuso_offset": offset,
                            },
                            base_legal="Art. 59 da Lei 9.504/97",
                        )
                    )
            except (ValueError, IndexError):
                pass

        encerramento = events.get("encerramento")
        if encerramento:
            try:
                enc_h, enc_m = _parse_time(encerramento.hora)
                enc_min = _time_to_min(enc_h, enc_m)

                if enc_min < limits["encerramento_min"]:
                    issues.append(
                        Issue(
                            codigo="A05",
                            severidade=IssueSeverity.HIGH,
                            descricao=(
                                f"Votacao encerrada ANTES do horario: "
                                f"{encerramento.hora}{tz_note}"
                            ),
                            uf="", municipio="", zona="", secao="",
                            detalhes={
                                "hora_encerramento": encerramento.hora,
                                "data": encerramento.data,
                                "fuso_offset": offset,
                            },
                            base_legal="Art. 153, CE - Encerramento as 17h BRT",
                        )
                    )
                elif enc_min > limits["encerramento_max"]:
                    issues.append(
                        Issue(
                            codigo="M02",
                            severidade=IssueSeverity.MEDIUM,
                            descricao=(
                                f"Votacao encerrada muito tarde: "
                                f"{encerramento.hora}{tz_note}"
                            ),
                            uf="", municipio="", zona="", secao="",
                            detalhes={
                                "hora_encerramento": encerramento.hora,
                                "data": encerramento.data,
                                "fuso_offset": offset,
                            },
                        )
                    )
            except (ValueError, IndexError):
                pass

        # Sem evento de abertura detectado
        if not abertura and events.get("total_entries", 0) > 100:
            issues.append(
                Issue(
                    codigo="M03",
                    severidade=IssueSeverity.MEDIUM,
                    descricao="Evento de abertura nao encontrado no log",
                    uf="", municipio="", zona="", secao="",
                    detalhes={"total_entries": events.get("total_entries", 0)},
                )
            )

        # Reinicializacoes durante votacao
        reboots = events.get("reboots", 0)
        if reboots > 0:
            aberturas = events.get("aberturas", [])
            horarios = [f"{a.data} {a.hora}" for a in aberturas]
            sev = IssueSeverity.HIGH if reboots >= 2 else IssueSeverity.MEDIUM
            issues.append(
                Issue(
                    codigo="C06",
                    severidade=sev,
                    descricao=(
                        f"Urna reiniciou {reboots}x durante a votacao"
                    ),
                    uf="", municipio="", zona="", secao="",
                    detalhes={
                        "reboots": reboots,
                        "horarios_abertura": horarios,
                        "desligamentos": len(events.get("desligadas", [])),
                    },
                    base_legal="Integridade do processo de votacao - reinicio pode indicar falha de hardware ou software",
                )
            )

        return issues

    def _check_errors(self, events: dict) -> list[Issue]:
        """Verifica erros registrados no log."""
        issues = []
        erros = events.get("erros", [])

        if len(erros) > 10:
            issues.append(
                Issue(
                    codigo="M04",
                    severidade=IssueSeverity.MEDIUM,
                    descricao=f"Numero elevado de erros no log: {len(erros)}",
                    uf="", municipio="", zona="", secao="",
                    detalhes={
                        "total_erros": len(erros),
                        "primeiros_erros": [
                            {"hora": e.hora, "desc": e.descricao[:100]}
                            for e in erros[:5]
                        ],
                    },
                )
            )

        # Substituicoes de urna
        substituicoes = events.get("substituicoes", [])
        if substituicoes:
            issues.append(
                Issue(
                    codigo="I01",
                    severidade=IssueSeverity.INFO,
                    descricao=f"Eventos de substituicao/contingencia: {len(substituicoes)}",
                    uf="", municipio="", zona="", secao="",
                    detalhes={
                        "total_substituicoes": len(substituicoes),
                        "eventos": [
                            {"hora": s.hora, "desc": s.descricao[:100]}
                            for s in substituicoes
                        ],
                    },
                )
            )

        # Ajustes de hora
        ajustes = events.get("ajustes_hora", [])
        if ajustes:
            issues.append(
                Issue(
                    codigo="M05",
                    severidade=IssueSeverity.MEDIUM,
                    descricao=f"Ajustes de hora detectados: {len(ajustes)}",
                    uf="", municipio="", zona="", secao="",
                    detalhes={
                        "total_ajustes": len(ajustes),
                        "eventos": [
                            {"hora": a.hora, "desc": a.descricao[:100]}
                            for a in ajustes
                        ],
                    },
                    base_legal="Integridade da linha do tempo de eventos",
                )
            )

        return issues

    def _check_mesario_alerts(self, events: dict) -> list[Issue]:
        """Verifica quantidade de alertas de mesario."""
        issues = []
        alertas = events.get("alertas_mesario", [])

        if len(alertas) > LIMIAR_ALERTAS_MESARIO:
            issues.append(
                Issue(
                    codigo="I02",
                    severidade=IssueSeverity.INFO,
                    descricao=(
                        f"Numero elevado de alertas de mesario: {len(alertas)} "
                        f"(limiar: {LIMIAR_ALERTAS_MESARIO})"
                    ),
                    uf="", municipio="", zona="", secao="",
                    detalhes={"total_alertas": len(alertas)},
                )
            )

        return issues

    def _check_votos_vs_log(self, bu: BoletimUrna, events: dict) -> list[Issue]:
        """Compara votos computados no log com totais do BU."""
        issues = []
        votos_log = events.get("votos_computados", 0)

        if votos_log == 0:
            return issues

        # Total de comparecimento do BU (usar o maior valor entre cargos)
        max_comparecimento = 0
        for eleicao in bu.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                if resultado.comparecimento > max_comparecimento:
                    max_comparecimento = resultado.comparecimento

        if max_comparecimento > 0 and votos_log != max_comparecimento:
            diff = abs(votos_log - max_comparecimento)
            if diff > 2:  # Tolerancia de 2 por possivel contagem dupla no log
                issues.append(
                    Issue(
                        codigo="A03",
                        severidade=IssueSeverity.HIGH,
                        descricao=(
                            f"Divergencia entre votos no log ({votos_log}) "
                            f"e comparecimento no BU ({max_comparecimento})"
                        ),
                        uf=str(bu.uf),
                        municipio=str(bu.municipio),
                        zona=str(bu.zona),
                        secao=str(bu.secao),
                        detalhes={
                            "votos_log": votos_log,
                            "comparecimento_bu": max_comparecimento,
                            "diferenca": diff,
                        },
                    )
                )

        return issues

    def _extract_timing(self, events: dict) -> dict:
        """Extrai metricas de timing do log."""
        timing = {
            "hora_abertura": None,
            "hora_encerramento": None,
            "hora_ligada": None,
            "duracao_votacao_min": None,
        }

        abertura = events.get("abertura")
        if abertura:
            timing["hora_abertura"] = f"{abertura.data} {abertura.hora}"

        encerramento = events.get("encerramento")
        if encerramento:
            timing["hora_encerramento"] = f"{encerramento.data} {encerramento.hora}"

        ligada = events.get("ligada")
        if ligada:
            timing["hora_ligada"] = f"{ligada.data} {ligada.hora}"

        # Calcular duracao em minutos
        if abertura and encerramento:
            try:
                h1, m1, s1 = map(int, abertura.hora.split(":"))
                h2, m2, s2 = map(int, encerramento.hora.split(":"))
                min1 = h1 * 60 + m1
                min2 = h2 * 60 + m2
                timing["duracao_votacao_min"] = min2 - min1
            except (ValueError, AttributeError):
                pass

        return timing
