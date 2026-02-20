"""Parser de arquivos de assinatura digital (.vscmr / .vsc)."""

import hashlib
import logging
from pathlib import Path
from typing import Optional

from ..config import MODELOS_URNA
from ..models import HashVerification, SignatureVerification, SpecVersion
from .asn1_helper import decode_entity, detect_spec_version, get_compiler

logger = logging.getLogger(__name__)


class SignatureParser:
    """Parser de arquivos de assinatura digital da urna."""

    def parse(self, file_path: Path) -> dict:
        """Parseia arquivo de assinatura e retorna estrutura completa."""
        file_path = Path(file_path)
        spec_version = detect_spec_version(file_path)
        module = "assinatura"

        with open(file_path, "rb") as f:
            raw = f.read()

        # V1 usa EntidadeAssinaturaResultado, V2 usa EntidadeAssinaturaEcourna
        if spec_version == SpecVersion.V2:
            entity_name = "EntidadeAssinaturaEcourna"
        else:
            entity_name = "EntidadeAssinaturaResultado"

        decoded = decode_entity(raw, entity_name, spec_version, module)
        return decoded

    def extract_file_hashes(self, file_path: Path) -> dict[str, bytes]:
        """Extrai hashes SHA-512 de todos os arquivos listados na assinatura.

        Returns:
            Dict mapeando nome_arquivo -> hash SHA-512 (bytes)
        """
        decoded = self.parse(file_path)
        hashes = {}

        for sig_type in ["assinaturaSW", "assinaturaHW"]:
            sig = decoded.get(sig_type, {})
            if not sig:
                continue

            conteudo = sig.get("conteudoAutoAssinado", b"")
            if not conteudo:
                continue

            # Decodificar o conteudo auto-assinado como Assinatura
            spec_version = detect_spec_version(file_path)
            compiler = get_compiler(spec_version, "assinatura")
            try:
                assinatura = compiler.decode("Assinatura", bytearray(conteudo))
                for arq in assinatura.get("assinaturaArquivos", []):
                    nome = arq.get("nomeArquivo", "")
                    hash_val = arq.get("assinatura", {}).get("hash", b"")
                    if nome and hash_val:
                        hashes[nome] = hash_val
            except Exception as e:
                logger.warning("Erro ao decodificar assinatura %s: %s", sig_type, e)

        return hashes

    def verify_file_hashes(
        self, signature_path: Path, data_dir: Path
    ) -> list[HashVerification]:
        """Verifica hashes SHA-512 dos arquivos contra a assinatura.

        Args:
            signature_path: Caminho do arquivo .vscmr ou .vsc
            data_dir: Diretorio contendo os arquivos de dados da urna

        Returns:
            Lista de resultados de verificacao
        """
        expected_hashes = self.extract_file_hashes(signature_path)
        results = []

        for filename, expected_hash in expected_hashes.items():
            file_path = data_dir / filename
            if not file_path.exists():
                # Tentar encontrar o arquivo com nome similar
                candidates = list(data_dir.glob(f"*{filename}*"))
                if candidates:
                    file_path = candidates[0]
                else:
                    results.append(
                        HashVerification(
                            arquivo=filename,
                            hash_esperado=expected_hash,
                            hash_calculado=b"",
                            valido=False,
                        )
                    )
                    continue

            with open(file_path, "rb") as f:
                content = f.read()

            calculated = hashlib.sha512(content).digest()
            results.append(
                HashVerification(
                    arquivo=filename,
                    hash_esperado=expected_hash,
                    hash_calculado=calculated,
                    valido=(expected_hash == calculated),
                )
            )

        return results

    def get_model(self, file_path: Path) -> Optional[str]:
        """Extrai modelo da urna do arquivo de assinatura."""
        try:
            decoded = self.parse(file_path)
            model_raw = decoded.get("modeloEquipamento", None)
            if isinstance(model_raw, tuple):
                model_num = model_raw[1]
            else:
                model_num = model_raw
            return MODELOS_URNA.get(model_num, f"Modelo_{model_num}")
        except Exception as e:
            logger.warning("Erro ao extrair modelo: %s", e)
            return None
