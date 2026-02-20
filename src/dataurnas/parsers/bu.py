"""Parser de Boletim de Urna (BU) - suporta V1 (2022) e V2 (2024+)."""

import logging
from pathlib import Path
from typing import Optional

from ..config import CARGOS, MODELOS_URNA
from ..models import (
    BoletimUrna,
    Fase,
    ResultadoCargo,
    ResultadoEleicao,
    SpecVersion,
    TipoVoto,
    VotoTupla,
)
from .asn1_helper import decode_envelope, detect_spec_version

logger = logging.getLogger(__name__)


def _unwrap_tuple(val, index=1):
    """Desempacota tuplas ASN.1 do tipo (nome, valor)."""
    if isinstance(val, tuple) and len(val) > index:
        return val[index]
    return val


def _unwrap_choice(val):
    """Desempacota CHOICE ASN.1 que vem como (nome_str, valor)."""
    if isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], str):
        return val[0], val[1]
    return None, val


# Mapeamento de nomes de cargo ASN.1 para codigo numerico
_CARGO_NAME_MAP = {
    "presidente": 1,
    "vicePresidente": 2,
    "governador": 3,
    "viceGovernador": 4,
    "senador": 5,
    "deputadoFederal": 6,
    "deputadoEstadual": 7,
    "deputadoDistrital": 8,
    "primeiroSuplenteSenador": 9,
    "segundoSuplenteSenador": 10,
    "prefeito": 11,
    "vicePrefeito": 12,
    "vereador": 13,
}

# Mapeamento de tipoVoto do ASN.1 para o enum
_TIPO_VOTO_MAP = {
    "nominal": TipoVoto.NOMINAL,
    "branco": TipoVoto.BRANCO,
    "nulo": TipoVoto.NULO,
    "legenda": TipoVoto.LEGENDA,
    "cargoSemCandidato": TipoVoto.CARGO_SEM_CANDIDATO,
    # Mapeamento numerico (quando numeric_enums=True nao e usado)
    1: TipoVoto.NOMINAL,
    2: TipoVoto.BRANCO,
    3: TipoVoto.NULO,
    4: TipoVoto.LEGENDA,
    5: TipoVoto.CARGO_SEM_CANDIDATO,
}


class BUParser:
    """Parser de Boletim de Urna."""

    def parse(self, file_path: Path) -> BoletimUrna:
        """Parseia um arquivo BU e retorna dados estruturados."""
        file_path = Path(file_path)
        spec_version = detect_spec_version(file_path)

        with open(file_path, "rb") as f:
            raw = f.read()

        envelope, bu = decode_envelope(raw, "EntidadeBoletimUrna", spec_version)

        return self._build_bu(envelope, bu, spec_version)

    def _build_bu(
        self, envelope: dict, bu: dict, spec_version: SpecVersion
    ) -> BoletimUrna:
        """Constroi BoletimUrna a partir dos dicts decodificados."""
        # Identificacao da secao
        ident = envelope.get("identificacao", (None, {}))
        ident_data = ident[1] if isinstance(ident, tuple) else ident
        mz = ident_data.get("municipioZona", {})

        # Dados da urna
        urna_data = bu.get("urna", {}) or envelope.get("urna", {})
        numero_serie = None
        codigo_carga = None
        tipo_arquivo = None
        tipo_urna = None
        numero_interno = None
        versao_votacao = None

        if urna_data:
            corr = urna_data.get("correspondenciaResultado", {})
            if corr:
                carga = corr.get("carga", {})
                codigo_carga = str(carga.get("codigoCarga", "")) if carga else None
                numero_interno = carga.get("numeroInternoUrna", None) if carga else None

            # tipoUrna e o tipo (secao/reservaSecao/contingencia), NAO o modelo de hardware
            tipo_urna_raw = urna_data.get("tipoUrna", None)
            if isinstance(tipo_urna_raw, tuple):
                tipo_urna = tipo_urna_raw[0] if isinstance(tipo_urna_raw[0], str) else str(tipo_urna_raw[1])
            elif tipo_urna_raw:
                tipo_urna = str(tipo_urna_raw)

            versao_votacao = urna_data.get("versaoVotacao", None)

            tipo_arquivo_raw = urna_data.get("tipoArquivo", None)
            if isinstance(tipo_arquivo_raw, tuple):
                tipo_arquivo = tipo_arquivo_raw[0]
            elif tipo_arquivo_raw:
                tipo_arquivo = str(tipo_arquivo_raw)

        # Fase
        fase_raw = bu.get("fase", envelope.get("fase", 2))
        if isinstance(fase_raw, tuple):
            fase_raw = fase_raw[1]
        try:
            fase = Fase(fase_raw) if isinstance(fase_raw, int) else Fase.OFICIAL
        except ValueError:
            fase = Fase.OFICIAL

        # Dados de SA (abertura/encerramento) - vem como tupla ('dadosSecao', {...})
        dados_sa_raw = bu.get("dadosSecaoSA", None)
        dados_sa = _unwrap_tuple(dados_sa_raw) if isinstance(dados_sa_raw, tuple) else (dados_sa_raw or {})
        if not isinstance(dados_sa, dict):
            dados_sa = {}
        hora_abertura = dados_sa.get("dataHoraAbertura", None)
        hora_encerramento = dados_sa.get("dataHoraEncerramento", None)

        # Resultados por eleicao
        resultados = []
        for res_eleicao in bu.get("resultadosVotacaoPorEleicao", []):
            resultados.append(self._parse_resultado_eleicao(res_eleicao, spec_version))

        # UF nao esta diretamente no envelope V1, extrair do path se possivel
        uf = ident_data.get("uf", "")
        if not uf or not isinstance(uf, str):
            uf = ""

        return BoletimUrna(
            uf=uf,
            municipio=mz.get("municipio", 0),
            zona=mz.get("zona", 0),
            secao=ident_data.get("secao", 0),
            local_votacao=ident_data.get("localVotacao", None),
            modelo_urna=None,  # Modelo vem do arquivo .vscmr ou log, nao do BU
            numero_serie=numero_serie,
            numero_interno=numero_interno,
            codigo_carga=codigo_carga,
            tipo_urna=tipo_urna,
            tipo_arquivo=tipo_arquivo,
            versao_votacao=versao_votacao,
            fase=fase,
            spec_version=spec_version,
            data_hora_emissao=bu.get("dataHoraEmissao", None),
            hora_abertura=hora_abertura,
            hora_encerramento=hora_encerramento,
            qtd_eleitores_lib_codigo=bu.get("qtdEleitoresLibCodigo", 0),
            qtd_eleitores_comp_biometrico=bu.get("qtdEleitoresCompBiometrico", 0),
            resultados_por_eleicao=resultados,
            historico_correspondencias=bu.get("historicoCorrespondencias", None),
        )

    def _parse_resultado_eleicao(
        self, res: dict, spec_version: SpecVersion
    ) -> ResultadoEleicao:
        """Parseia resultado de uma eleicao dentro do BU."""
        resultados_cargo = []
        for res_votacao in res.get("resultadosVotacao", []):
            for totais in res_votacao.get("totaisVotosCargo", []):
                codigo_cargo_raw = totais.get("codigoCargo", (None, 0))
                if isinstance(codigo_cargo_raw, tuple):
                    # Pode ser ('cargoConstitucional', 'presidente') ou ('cargoConstitucional', 1)
                    cargo_val = codigo_cargo_raw[1]
                    if isinstance(cargo_val, str):
                        # Mapear nome para codigo numerico
                        codigo_cargo = _CARGO_NAME_MAP.get(cargo_val, 0)
                    else:
                        codigo_cargo = cargo_val
                else:
                    codigo_cargo = codigo_cargo_raw

                votos = []
                for vv in totais.get("votosVotaveis", []):
                    tipo_raw = vv.get("tipoVoto", "nominal")
                    if isinstance(tipo_raw, tuple):
                        tipo_raw = tipo_raw[0] if isinstance(tipo_raw[0], str) else tipo_raw[1]
                    tipo_voto = _TIPO_VOTO_MAP.get(tipo_raw, TipoVoto.NOMINAL)

                    ident_votavel = vv.get("identificacaoVotavel", {}) or {}
                    codigo_votavel = ident_votavel.get("codigo", None)
                    partido = ident_votavel.get("partido", None)

                    votos.append(
                        VotoTupla(
                            tipo_voto=tipo_voto,
                            quantidade=vv.get("quantidadeVotos", 0),
                            codigo_votavel=codigo_votavel,
                            partido=partido,
                            hash=vv.get("hash", None),
                            ordem_hash=vv.get("ordemGeracaoHash", None),
                        )
                    )

                resultados_cargo.append(
                    ResultadoCargo(
                        codigo_cargo=codigo_cargo,
                        nome_cargo=CARGOS.get(codigo_cargo, f"Cargo {codigo_cargo}"),
                        comparecimento=res_votacao.get("qtdComparecimento", 0),
                        votos=votos,
                    )
                )

        # Hash encadeado (V2)
        ultimo_hash = res.get("ultimoHashVotosVotavel", None)
        assinatura = res.get("assinaturaUltimoHashVotosVotavel", None)

        return ResultadoEleicao(
            id_eleicao=res.get("idEleicao", 0),
            eleitores_aptos=res.get("qtdEleitoresAptos", 0),
            eleitores_aptos_secao=res.get("qtdEleitoresAptosSecao", None),
            eleitores_aptos_tte=res.get("qtdEleitoresAptosTTE", None),
            resultados=resultados_cargo,
            ultimo_hash=ultimo_hash,
            assinatura_ultimo_hash=assinatura,
        )

    def parse_to_dict(self, file_path: Path) -> dict:
        """Parseia BU e retorna como dicionario (para debug/exploracao)."""
        file_path = Path(file_path)
        spec_version = detect_spec_version(file_path)

        with open(file_path, "rb") as f:
            raw = f.read()

        envelope, bu = decode_envelope(raw, "EntidadeBoletimUrna", spec_version)
        return {"envelope": envelope, "bu": bu, "spec_version": spec_version.value}
