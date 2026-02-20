"""Configuracoes globais e constantes do sistema."""

from pathlib import Path

# Diretorios
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
JSON_DIR = DATA_DIR / "json"
DB_DIR = DATA_DIR / "db"
SPEC_DIR = PROJECT_ROOT / "spec"

# API do TSE
TSE_BASE_URL = "https://resultados.tse.jus.br/oficial"
TSE_CONFIG_URL = f"{TSE_BASE_URL}/comum/config/ele-c.json"

# Limites de download
MAX_RETRIES = 10
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 30
MAX_CONCURRENT_DOWNLOADS = 500
RATE_LIMIT_PER_SECOND = 3000  # Máximo para 500 Mbps

# Eleicoes conhecidas
ELECTIONS = {
    "municipal_2024": {
        "ciclo": "ele2024",
        "pleito_1t": "452",
        "pleito_2t": "453",
        "eleicao_1t": "619",
        "eleicao_2t": "620",
        "cargos": {"prefeito": 11, "vereador": 13},
        "spec_version": "v2",
    },
    "presidencial_2022": {
        "ciclo": "ele2022",
        "pleito_1t": "406",
        "pleito_2t": "407",
        "eleicao_1t": "544",
        "eleicao_2t": "545",
        "cargos": {
            "presidente": 1,
            "governador": 3,
            "senador": 5,
            "deputado_federal": 6,
            "deputado_estadual": 7,
        },
        "spec_version": "v1",
    },
}

# Todos os estados brasileiros
ESTADOS = [
    "ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma",
    "mt", "ms", "mg", "pa", "pb", "pr", "pe", "pi", "rj", "rn",
    "rs", "ro", "rr", "sc", "sp", "se", "to",
]

# Regioes para amostragem
REGIOES = {
    "norte": ["ac", "am", "ap", "pa", "ro", "rr", "to"],
    "nordeste": ["al", "ba", "ce", "ma", "pb", "pe", "pi", "rn", "se"],
    "centro_oeste": ["df", "go", "ms", "mt"],
    "sudeste": ["es", "mg", "rj", "sp"],
    "sul": ["pr", "rs", "sc"],
}

# Modelos de urna
MODELOS_URNA = {
    9: "UE2009",
    10: "UE2010",
    11: "UE2011",
    13: "UE2013",
    15: "UE2015",
    20: "UE2020",
    22: "UE2022",
}

# Tipos de arquivo
FILE_TYPES_V1 = {
    "bu": ".bu",
    "rdv": ".rdv",
    "log": ".logjez",
    "imgbu": ".imgbu",
    "vscmr": ".vscmr",
}

FILE_TYPES_V2 = {
    "bu": "-bu.dat",
    "rdv": "-rdv.dat",
    "log": "-log.jez",
    "imgbu": "-imgbu.dat",
    "vsc": "-vota.vsc",
}

# Severidades de inconsistencia
class Severity:
    CRITICAL = "critica"
    HIGH = "alta"
    MEDIUM = "media"
    INFO = "informativa"

# Codigos de cargo
CARGOS = {
    1: "Presidente",
    3: "Governador",
    5: "Senador",
    6: "Deputado Federal",
    7: "Deputado Estadual",
    11: "Prefeito",
    13: "Vereador",
}

# Offsets de fuso horario em relacao a Brasilia (UTC-3)
# Logs de urna usam hora local. Votacao e das 8h-17h Brasilia.
# Offset negativo = esta atras de Brasilia.
TIMEZONE_OFFSETS = {
    "ac": -2,  # UTC-5 (Acre Time)
    "am": -1,  # UTC-4 (Amazon Time) - exceto leste AM
    "mt": -1,  # UTC-4
    "ms": -1,  # UTC-4
    "ro": -1,  # UTC-4
    "rr": -1,  # UTC-4
    # Fernando de Noronha (PE): +1, mas poucas secoes
    # Demais estados: 0 (UTC-3 = Brasilia)
}

