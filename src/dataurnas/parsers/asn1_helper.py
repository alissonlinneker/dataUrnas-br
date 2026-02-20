"""Utilitarios para decodificacao ASN.1."""

import logging
from pathlib import Path
from typing import Optional

import asn1tools

from ..config import SPEC_DIR
from ..models import SpecVersion

logger = logging.getLogger(__name__)

# Cache de compiladores ASN.1 (evita recompilar a cada arquivo)
_compilers: dict[str, asn1tools.CompiledFile] = {}


def get_compiler(spec_version: SpecVersion, module: str = "bu") -> asn1tools.CompiledFile:
    """Obtem compilador ASN.1 para a versao e modulo especificados.

    Args:
        spec_version: V1 (2022) ou V2 (2024+)
        module: 'bu', 'rdv', ou 'assinatura'

    Returns:
        Compilador ASN.1 pronto para decodificacao
    """
    cache_key = f"{spec_version.value}_{module}"
    if cache_key not in _compilers:
        spec_dir = SPEC_DIR / spec_version.value
        spec_file = spec_dir / f"{module}.asn1"
        if not spec_file.exists():
            raise FileNotFoundError(f"Spec ASN.1 nao encontrada: {spec_file}")
        _compilers[cache_key] = asn1tools.compile_files(
            [str(spec_file)], codec="ber"
        )
    return _compilers[cache_key]


def decode_entity(
    data: bytes, entity_name: str, spec_version: SpecVersion, module: str = "bu"
) -> dict:
    """Decodifica uma entidade ASN.1/BER."""
    compiler = get_compiler(spec_version, module)
    return compiler.decode(entity_name, bytearray(data))


def decode_envelope(
    data: bytes, inner_entity: str, spec_version: SpecVersion, module: str = "bu"
) -> tuple[dict, dict]:
    """Decodifica EntidadeEnvelopeGenerico e extrai entidade interna.

    Returns:
        Tupla (envelope_decoded, inner_decoded)
    """
    compiler = get_compiler(spec_version, module)
    envelope = compiler.decode("EntidadeEnvelopeGenerico", bytearray(data))
    inner_data = envelope["conteudo"]
    inner = compiler.decode(inner_entity, bytearray(inner_data))
    return envelope, inner


def detect_spec_version(file_path: Path) -> SpecVersion:
    """Detecta a versao da spec baseado na extensao do arquivo.

    V1 (2022): .bu, .rdv, .logjez, .imgbu, .vscmr
    V2 (2024+): -bu.dat, -rdv.dat, -log.jez, -imgbu.dat, -vota.vsc
    """
    name = file_path.name
    if name.endswith(".dat") or name.endswith(".vsc"):
        return SpecVersion.V2
    if name.endswith((".bu", ".rdv", ".logjez", ".imgbu", ".vscmr")):
        return SpecVersion.V1
    # Fallback: tentar detectar pelo nome do arquivo
    if "-bu." in name or "-rdv." in name or "-vota." in name:
        return SpecVersion.V2
    return SpecVersion.V1
