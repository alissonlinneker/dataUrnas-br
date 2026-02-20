#!/usr/bin/env python3
"""Reconstroi o banco DuckDB a partir dos arquivos Parquet distribuidos."""

import duckdb
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"
DB_DIR = PROJECT_ROOT / "data" / "db"
DB_PATH = DB_DIR / "eleicoes_2022.duckdb"
LIVE_PATH = DB_DIR / "eleicoes_2022_live.duckdb"


def build():
    if not PARQUET_DIR.exists():
        print(f"ERRO: Diretorio {PARQUET_DIR} nao encontrado.")
        print("Certifique-se de que os arquivos Parquet estao em data/parquet/")
        sys.exit(1)

    required = ["secoes.parquet", "issues.parquet", "totais_cargo.parquet", "schema.sql"]
    votos_files = sorted(PARQUET_DIR.glob("votos_*.parquet"))

    for f in required:
        if not (PARQUET_DIR / f).exists():
            print(f"ERRO: Arquivo obrigatorio nao encontrado: {f}")
            sys.exit(1)

    if not votos_files:
        print("ERRO: Nenhum arquivo votos_*.parquet encontrado.")
        sys.exit(1)

    DB_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
    if LIVE_PATH.exists():
        LIVE_PATH.unlink()

    print(f"Construindo banco em {DB_PATH}...")
    start = time.time()

    conn = duckdb.connect(str(DB_PATH))

    # Criar schema
    schema_sql = (PARQUET_DIR / "schema.sql").read_text()
    for stmt in schema_sql.split(";;"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    # Importar secoes
    print("  Importando secoes...")
    conn.execute(f"INSERT INTO secoes SELECT * FROM read_parquet('{PARQUET_DIR}/secoes.parquet')")
    n = conn.execute("SELECT COUNT(*) FROM secoes").fetchone()[0]
    print(f"    {n:,} secoes importadas")

    # Importar issues
    print("  Importando issues...")
    conn.execute(f"INSERT INTO issues SELECT * FROM read_parquet('{PARQUET_DIR}/issues.parquet')")
    n = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
    print(f"    {n:,} issues importadas")

    # Importar totais_cargo
    print("  Importando totais_cargo...")
    conn.execute(f"INSERT INTO totais_cargo SELECT * FROM read_parquet('{PARQUET_DIR}/totais_cargo.parquet')")
    n = conn.execute("SELECT COUNT(*) FROM totais_cargo").fetchone()[0]
    print(f"    {n:,} totais importados")

    # Importar votos (multiplos arquivos)
    print(f"  Importando votos ({len(votos_files)} arquivos)...")
    for vf in votos_files:
        print(f"    {vf.name}...", end=" ", flush=True)
        conn.execute(f"INSERT INTO votos SELECT * FROM read_parquet('{vf}')")
        print("OK")
    n = conn.execute("SELECT COUNT(*) FROM votos").fetchone()[0]
    print(f"    {n:,} votos importados")

    conn.close()

    # Criar copia live para o dashboard
    print("  Criando snapshot live...")
    import shutil
    shutil.copy2(DB_PATH, LIVE_PATH)

    elapsed = time.time() - start
    print(f"\nBanco construido com sucesso em {elapsed:.1f}s")
    print(f"  Principal: {DB_PATH} ({DB_PATH.stat().st_size / 1e9:.1f} GB)")
    print(f"  Live:      {LIVE_PATH}")


if __name__ == "__main__":
    build()
