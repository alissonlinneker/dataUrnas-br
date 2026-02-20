"""Modelos de dados do dominio."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class TipoVoto(Enum):
    NOMINAL = "nominal"
    BRANCO = "branco"
    NULO = "nulo"
    LEGENDA = "legenda"
    CARGO_SEM_CANDIDATO = "cargoSemCandidato"


class Fase(Enum):
    SIMULADO = 1
    OFICIAL = 2
    TREINAMENTO = 3


class SpecVersion(Enum):
    V1 = "v1"  # Eleicoes 2022
    V2 = "v2"  # Eleicoes 2024+


class IssueSeverity(Enum):
    CRITICAL = "critica"
    HIGH = "alta"
    MEDIUM = "media"
    INFO = "informativa"


@dataclass
class Election:
    """Representa uma eleicao disponivel no TSE."""
    ciclo: str
    pleito: str
    eleicao: str
    nome: str
    turno: int
    data: str
    spec_version: SpecVersion


@dataclass
class Section:
    """Representa uma secao eleitoral."""
    uf: str
    municipio_codigo: str
    municipio_nome: str
    zona: str
    secao: str


@dataclass
class UrnaMeta:
    """Metadados de uma urna (do arquivo auxiliar)."""
    section: Section
    hash: str
    status: str
    data: str
    hora: str
    arquivos: list[str]


@dataclass
class VotoTupla:
    """Uma tupla de voto no BU."""
    tipo_voto: TipoVoto
    quantidade: int
    codigo_votavel: Optional[int] = None
    partido: Optional[int] = None
    hash: Optional[bytes] = None
    ordem_hash: Optional[int] = None


@dataclass
class ResultadoCargo:
    """Resultado de votacao para um cargo."""
    codigo_cargo: int
    nome_cargo: str
    comparecimento: int
    votos: list[VotoTupla] = field(default_factory=list)


@dataclass
class ResultadoEleicao:
    """Resultado de uma eleicao no BU."""
    id_eleicao: int
    eleitores_aptos: int
    eleitores_aptos_secao: Optional[int] = None
    eleitores_aptos_tte: Optional[int] = None
    resultados: list[ResultadoCargo] = field(default_factory=list)
    ultimo_hash: Optional[bytes] = None
    assinatura_ultimo_hash: Optional[bytes] = None


@dataclass
class BoletimUrna:
    """Dados decodificados de um Boletim de Urna."""
    # Identificacao
    uf: str
    municipio: int
    zona: int
    secao: int
    local_votacao: Optional[int] = None

    # Urna
    modelo_urna: Optional[str] = None  # Preenchido a partir do .vscmr ou log
    numero_serie: Optional[int] = None
    numero_interno: Optional[int] = None
    codigo_carga: Optional[str] = None
    tipo_urna: Optional[str] = None  # secao, reservaSecao, contingencia
    tipo_arquivo: Optional[str] = None
    versao_votacao: Optional[str] = None

    # Fase
    fase: Fase = Fase.OFICIAL
    spec_version: SpecVersion = SpecVersion.V1

    # Dados de votacao
    data_hora_emissao: Optional[str] = None
    hora_abertura: Optional[str] = None
    hora_encerramento: Optional[str] = None
    qtd_eleitores_lib_codigo: int = 0
    qtd_eleitores_comp_biometrico: int = 0

    # Resultados
    resultados_por_eleicao: list[ResultadoEleicao] = field(default_factory=list)

    # Historico
    historico_correspondencias: Optional[list] = None

    @property
    def total_votos(self) -> int:
        total = 0
        for eleicao in self.resultados_por_eleicao:
            for resultado in eleicao.resultados:
                total += sum(v.quantidade for v in resultado.votos)
        return total


@dataclass
class LogEntry:
    """Uma entrada de log da urna."""
    data: str
    hora: str
    severidade: str
    id_urna: str
    aplicativo: str
    descricao: str
    mac: Optional[str] = None

    @property
    def timestamp(self) -> Optional[datetime]:
        try:
            return datetime.strptime(f"{self.data}{self.hora}", "%Y%m%d%H%M%S")
        except (ValueError, TypeError):
            return None


@dataclass
class HashVerification:
    """Resultado de verificacao de hash de um arquivo."""
    arquivo: str
    hash_esperado: bytes
    hash_calculado: bytes
    valido: bool

    @property
    def match(self) -> bool:
        return self.hash_esperado == self.hash_calculado


@dataclass
class SignatureVerification:
    """Resultado de verificacao de assinatura digital."""
    arquivo: str
    algoritmo: str
    valido: bool
    certificado_valido: Optional[bool] = None
    modelo_urna: Optional[str] = None


@dataclass
class Issue:
    """Uma inconsistencia detectada."""
    codigo: str
    severidade: IssueSeverity
    descricao: str
    uf: str
    municipio: str
    zona: str
    secao: str
    detalhes: Optional[dict] = None
    base_legal: Optional[str] = None
