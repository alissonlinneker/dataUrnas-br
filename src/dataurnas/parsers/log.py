"""Parser de logs da urna eletronica (.logjez / .jez)."""

import io
import logging
import re
from pathlib import Path
from typing import Optional

from ..models import LogEntry

logger = logging.getLogger(__name__)

# Formato de cada linha do log (separado por TAB):
# DD/MM/YYYY HH:MM:SS\tSEVERIDADE\tID_URNA\tAPLICATIVO\tDESCRICAO\tMAC
_LOG_LINE_PATTERN = re.compile(
    r"^(\d{2}/\d{2}/\d{4}) (\d{2}:\d{2}:\d{2})\t(\w+)\t(\d+)\t(\w+)\t(.+?)\t([A-Fa-f0-9]+)\s*$"
)

# Padroes de eventos relevantes
PATTERN_MODELO_URNA = re.compile(r"(?:Modelo de [Uu]rna|Modelo UE|UE)[\s:]*(\d{4})")
PATTERN_MODELO_VST = re.compile(r"avusrlibue(\d{4})\.vst")
PATTERN_ABERTURA = re.compile(r"Urna pronta para receber votos")
PATTERN_ENCERRAMENTO = re.compile(r"In[ií]cio do Encerramento")
PATTERN_ENCERRAMENTO_CONFIRMADO = re.compile(r"Procedimento de encerramento confirmado")
PATTERN_ERRO = re.compile(r"(?:ERRO|ERROR|FALHA|FAIL)", re.IGNORECASE)
PATTERN_SUBSTITUICAO = re.compile(r"(?:substitui|contingencia|conting[eê]ncia)", re.IGNORECASE)
PATTERN_AJUSTE_HORA = re.compile(r"(?:ajust|acert).*(?:hora|relogio|rel[oó]gio)", re.IGNORECASE)
PATTERN_MESARIO_ALERTA = re.compile(r"Mes[aá]rio indagado")
PATTERN_VOTO_COMPUTADO = re.compile(r"O voto do eleitor foi computado")
PATTERN_LIGADA = re.compile(r"Urna ligada em (.+)")
PATTERN_DESLIGADA = re.compile(r"Urna desligada")
PATTERN_CARGA = re.compile(r"Modo de carga da UE")
PATTERN_REBOOT = re.compile(r"In[ií]cio das opera[çc][oõ]es do logd")


class LogParser:
    """Parser de logs da urna eletronica."""

    def parse(self, file_path: Path) -> list[LogEntry]:
        """Parseia um arquivo de log da urna.

        O arquivo .logjez/.jez e compactado em 7-zip.
        Dentro dele existe um arquivo logd.dat em texto puro.
        """
        file_path = Path(file_path)
        text = self._extract_text(file_path)
        if not text:
            return []

        entries = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            entry = self._parse_line(line)
            if entry:
                entries.append(entry)

        return entries

    def _extract_text(self, file_path: Path) -> Optional[str]:
        """Extrai texto do arquivo 7-zip."""
        try:
            import py7zr
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                with py7zr.SevenZipFile(file_path, mode="r") as archive:
                    archive.extractall(path=tmpdir)
                # Ler o arquivo extraido (geralmente logd.dat)
                tmppath = Path(tmpdir)
                for extracted in tmppath.iterdir():
                    return extracted.read_bytes().decode("latin-1", errors="replace")
        except ImportError:
            logger.error("py7zr nao instalado. Execute: pip install py7zr")
            return None
        except Exception as e:
            logger.error("Erro ao extrair log de %s: %s", file_path, e)
            return None
        return None

    def _parse_line(self, line: str) -> Optional[LogEntry]:
        """Parseia uma linha do log."""
        match = _LOG_LINE_PATTERN.match(line)
        if match:
            return LogEntry(
                data=match.group(1),
                hora=match.group(2),
                severidade=match.group(3),
                id_urna=match.group(4),
                aplicativo=match.group(5),
                descricao=match.group(6),
                mac=match.group(7),
            )
        # Linhas que nao seguem o padrao sao ignoradas silenciosamente
        return None

    def extract_model(self, entries: list[LogEntry]) -> Optional[str]:
        """Extrai o modelo da urna dos eventos de log."""
        for entry in entries:
            match = PATTERN_MODELO_URNA.search(entry.descricao)
            if match:
                return match.group(1)
        return None

    def extract_events(self, entries: list[LogEntry], turno_date: str = None) -> dict:
        """Extrai eventos relevantes do log.

        Args:
            entries: Lista de entradas do log.
            turno_date: Data do turno (DD/MM/YYYY) para filtrar entradas.
                Se fornecido, apenas entradas dessa data sao consideradas.
                Necessario para T2, pois o .logjez acumula eventos do T1.
        """
        # Filtrar por data do turno (corrige T2 que acumula eventos do T1)
        if turno_date:
            entries = [e for e in entries if e.data == turno_date]

        events = {
            "modelo": None,
            "abertura": None,
            "aberturas": [],  # Todas as aberturas (para detectar reboots)
            "encerramento": None,
            "encerramento_confirmado": None,
            "erros": [],
            "substituicoes": [],
            "ajustes_hora": [],
            "alertas_mesario": [],
            "votos_computados": 0,
            "ligada": None,
            "desligadas": [],
            "reboots": 0,  # Reinicializacoes durante votacao
            "total_entries": len(entries),
            "severidades": {},
        }

        for entry in entries:
            desc = entry.descricao

            # Contagem de severidades
            sev = entry.severidade.upper() if entry.severidade else "UNKNOWN"
            events["severidades"][sev] = events["severidades"].get(sev, 0) + 1

            # Modelo - tentar padrao principal e depois vst
            if not events["modelo"]:
                m = PATTERN_MODELO_URNA.search(desc)
                if m:
                    events["modelo"] = m.group(1)
                else:
                    m = PATTERN_MODELO_VST.search(desc)
                    if m:
                        events["modelo"] = m.group(1)

            if PATTERN_ABERTURA.search(desc):
                events["aberturas"].append(entry)
                # Usar a PRIMEIRA abertura como a oficial
                if not events["abertura"]:
                    events["abertura"] = entry

            if PATTERN_ENCERRAMENTO.search(desc):
                events["encerramento"] = entry

            if PATTERN_ENCERRAMENTO_CONFIRMADO.search(desc):
                events["encerramento_confirmado"] = entry

            if PATTERN_ERRO.search(desc):
                events["erros"].append(entry)

            if PATTERN_SUBSTITUICAO.search(desc):
                events["substituicoes"].append(entry)

            if PATTERN_AJUSTE_HORA.search(desc):
                events["ajustes_hora"].append(entry)

            if PATTERN_MESARIO_ALERTA.search(desc):
                events["alertas_mesario"].append(entry)

            if PATTERN_VOTO_COMPUTADO.search(desc):
                events["votos_computados"] += 1

            if not events["ligada"]:
                m = PATTERN_LIGADA.search(desc)
                if m:
                    events["ligada"] = entry

            if PATTERN_DESLIGADA.search(desc):
                events["desligadas"].append(entry)

        # Calcular reboots: multiplas aberturas = reinicializacoes
        events["reboots"] = max(0, len(events["aberturas"]) - 1)

        return events
