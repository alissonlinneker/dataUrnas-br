"""Navegacao da API hierarquica do TSE."""

import logging
from dataclasses import dataclass
from typing import Optional

from ..config import TSE_BASE_URL, TSE_CONFIG_URL
from ..models import Election, Section, SpecVersion, UrnaMeta
from .client import TSEClient

logger = logging.getLogger(__name__)


@dataclass
class StateConfig:
    """Configuracao de um estado com seus municipios, zonas e secoes."""
    uf: str
    nome: str
    municipios: list[dict]  # Lista crua do JSON do TSE


class TSEApi:
    """Interface de navegacao da API hierarquica do TSE."""

    def __init__(self, client: TSEClient):
        self._client = client

    async def list_elections(self) -> list[dict]:
        """Nivel 0: Lista todas as eleicoes disponiveis."""
        data = await self._client.fetch_json(TSE_CONFIG_URL)
        if not data:
            return []
        return data

    async def get_state_config(
        self, ciclo: str, pleito: str, uf: str
    ) -> Optional[StateConfig]:
        """Nivel 1: Obtem municipios, zonas e secoes de um estado."""
        uf = uf.lower()
        url = (
            f"{TSE_BASE_URL}/{ciclo}/arquivo-urna/{pleito}"
            f"/config/{uf}/{uf}-p000{pleito}-cs.json"
        )
        data = await self._client.fetch_json(url)
        if not data or "abr" not in data or not data["abr"]:
            logger.error("Dados de estado nao encontrados para %s", uf)
            return None

        estado = data["abr"][0]
        return StateConfig(
            uf=estado.get("cd", uf),
            nome=estado.get("ds", uf.upper()),
            municipios=estado.get("mu", []),
        )

    async def get_section_meta(
        self, ciclo: str, pleito: str, section: Section
    ) -> list[UrnaMeta]:
        """Nivel 2: Obtem metadados das urnas de uma secao."""
        uf = section.uf.lower()
        mun = section.municipio_codigo
        zona = section.zona
        secao = section.secao
        url = (
            f"{TSE_BASE_URL}/{ciclo}/arquivo-urna/{pleito}"
            f"/dados/{uf}/{mun}/{zona}/{secao}"
            f"/p000{pleito}-{uf}-m{mun}-z{zona}-s{secao}-aux.json"
        )
        data = await self._client.fetch_json(url)
        if not data or "hashes" not in data:
            return []

        result = []
        for h in data["hashes"]:
            hash_val = h.get("hash", "")
            if hash_val == "0" or not hash_val:
                continue

            # V1 usa 'nmarq', V2 usa 'arq'
            if "nmarq" in h:
                arquivos = h["nmarq"]
            elif "arq" in h:
                arquivos = [a["nm"] if isinstance(a, dict) else a for a in h["arq"]]
            else:
                arquivos = []

            result.append(
                UrnaMeta(
                    section=section,
                    hash=hash_val,
                    status=h.get("st", ""),
                    data=h.get("dr", ""),
                    hora=h.get("hr", ""),
                    arquivos=arquivos,
                )
            )
        return result

    def build_file_url(
        self, ciclo: str, pleito: str, section: Section, hash_val: str, filename: str
    ) -> str:
        """Nivel 3: Constroi URL de download de um arquivo de urna."""
        uf = section.uf.lower()
        return (
            f"{TSE_BASE_URL}/{ciclo}/arquivo-urna/{pleito}"
            f"/dados/{uf}/{section.municipio_codigo}/{section.zona}"
            f"/{section.secao}/{hash_val}/{filename}"
        )

    def iter_sections(self, state_config: StateConfig):
        """Itera sobre todas as secoes de um estado."""
        uf = state_config.uf
        for mun in state_config.municipios:
            mun_cd = mun["cd"]
            mun_nm = mun["nm"]
            for zona in mun.get("zon", []):
                zona_cd = zona["cd"]
                for secao in zona.get("sec", []):
                    secao_ns = secao["ns"]
                    yield Section(
                        uf=uf,
                        municipio_codigo=mun_cd,
                        municipio_nome=mun_nm,
                        zona=zona_cd,
                        secao=secao_ns,
                    )

    def count_sections(self, state_config: StateConfig) -> int:
        """Conta total de secoes em um estado."""
        total = 0
        for mun in state_config.municipios:
            for zona in mun.get("zon", []):
                total += len(zona.get("sec", []))
        return total
