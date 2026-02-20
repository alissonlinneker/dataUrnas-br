# DataUrnas-BR

[![Demo Online](https://img.shields.io/badge/%F0%9F%97%B3%EF%B8%8F%20Demo-Hugging%20Face%20Spaces-blue)](https://huggingface.co/spaces/alissonlinneker/dataurnas-br)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)

Sistema de auditoria independente dos dados eleitorais brasileiros, focado nas Eleicoes Presidenciais de 2022.

O projeto baixa, decodifica e analisa os **5 arquivos publicos** de cada urna eletronica — Boletim de Urna (BU), Registro Digital do Voto (RDV), Log da Urna, Imagem do BU e Assinaturas Digitais — disponibilizados pelo [TSE](https://resultados.tse.jus.br/oficial/), aplicando verificacoes de integridade criptografica, consistencia logica, deteccao de anomalias estatisticas e analise de padroes operacionais sobre **941.987 secoes eleitorais** em todas as 27 UFs do Brasil.

> **[Acesse o dashboard online](https://huggingface.co/spaces/alissonlinneker/dataurnas-br)** — sem necessidade de instalacao.

---

## Sumario

- [Motivacao](#motivacao)
- [Cobertura dos Dados](#cobertura-dos-dados)
- [Arquitetura do Sistema](#arquitetura-do-sistema)
- [Pipeline de Processamento](#pipeline-de-processamento)
- [Metodologias e Tecnicas](#metodologias-e-tecnicas)
- [Codigos de Inconsistencia](#codigos-de-inconsistencia)
- [Dashboard Interativo](#dashboard-interativo)
- [Instalacao e Uso](#instalacao-e-uso)
- [Deploy Online](#deploy-online)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Fontes de Dados e Legislacao](#fontes-de-dados-e-legislacao)
- [Tecnologias](#tecnologias)
- [Licenca](#licenca)

---

## Motivacao

A urna eletronica brasileira gera, em cada secao eleitoral, um conjunto de arquivos publicos que permitem auditoria independente do processo de votacao. O TSE disponibiliza esses arquivos em sua API publica apos o encerramento da votacao.

Este projeto realiza a **totalizacao paralela independente** — ou seja, refaz todo o processo de contagem a partir dos dados brutos, sem depender dos resultados oficiais publicados pelo TSE — e aplica mais de 50 verificacoes automatizadas cobrindo integridade criptografica, consistencia logica, padroes estatisticos e comportamento operacional das urnas.

O objetivo e responder de forma tecnica e transparente: **os dados publicos das urnas sao consistentes entre si e com os resultados oficiais?**

---

## Cobertura dos Dados

O banco de dados incluido neste repositorio cobre a **totalidade** das secoes eleitorais das Eleicoes Presidenciais de 2022:

| Metrica | Valor |
|---------|-------|
| Secoes eleitorais processadas | 941.987 |
| Votos individuais catalogados | 72.943.229 |
| Inconsistencias detectadas | 511.121 |
| Totais por cargo/secao | 3.074.096 |
| Estados (UFs) | 27 (todos) |
| 1o Turno (02/10/2022) | 470.986 secoes |
| 2o Turno (30/10/2022) | 471.001 secoes |
| Modelos de urna | UE2009, UE2010, UE2011, UE2013, UE2015, UE2020 |
| Versao de software | 8.26.0.0 Onca-pintada (todas) |

Os dados brutos totalizam ~181 GB. Para viabilizar distribuicao via repositorio, os dados processados foram exportados em **Parquet com compressao ZSTD**, totalizando apenas **183 MB**.

---

## Arquitetura do Sistema

```
+------------------------------------------------------------------+
|                    DASHBOARD WEB (Streamlit)                      |
|  22 tabs de analise | Filtros interativos | Exportacao Excel/TXT  |
+------------------------------------------------------------------+
         |                    |                    |
+--------v--------+  +-------v--------+  +-------v---------+
|  51 FUNCOES DE  |  |   SCORE DE     |  |   EXPORTACAO    |
|  ANALISE SQL    |  |   CONFIANCA    |  |   Excel / TXT   |
+-----------------+  +----------------+  +-----------------+
         |                    |                    |
+------------------------------------------------------------------+
|                    MOTOR DE ANALISE                                |
|                                                                    |
|  Integridade:  Hash SHA-512 | Ed25519/ECDSA | Chain hashing       |
|  Consistencia: Votos <= Aptos | Zeresima | Cross-turno            |
|  Logs:         Reboots | Timing | Erros | Substituicoes           |
|  Estatistica:  Benford | Z-score | Chi-quadrado | Cramer's V      |
+------------------------------------------------------------------+
         |                    |                    |
+--------v--------+  +-------v--------+  +-------v---------+
|  PARSER DE BU   |  | PARSER DE LOG  |  | PARSER ASSINAT. |
|  (ASN.1/BER)    |  | (7z → texto)   |  | (ASN.1/BER)     |
+-----------------+  +----------------+  +-----------------+
         |                    |                    |
+------------------------------------------------------------------+
|                DOWNLOADER ASSINCRONO TSE                           |
|  API hierarquica (4 niveis) | Retry | Rate limiting               |
+------------------------------------------------------------------+
         |
+--------v---------------------------------------------------------+
|              BANCO DE DADOS (DuckDB)                              |
|  secoes | votos | issues | totais_cargo                          |
+------------------------------------------------------------------+
```

### Esquema do Banco de Dados

**secoes** — Metadados de cada secao eleitoral:
- Identificacao: `id`, `turno`, `uf`, `regiao`, `municipio`, `zona`, `secao`
- Hardware: `modelo_urna`, `tipo_urna`, `versao_sw`
- Eleitores: `eleitores_aptos`, `comparecimento`, `pct_abstencao`
- Biometria: `lib_codigo`, `comp_biometrico`, `pct_biometria`
- Timing: `hora_abertura`, `hora_encerramento`, `duracao_min`, `fuso_offset`
- Operacional: `reboots`, `erros_log`, `alertas_mesario`, `votos_log`, `substituicoes`
- Flags: `is_reserva`, `has_issues`, `n_issues`

**votos** — Cada voto por secao/cargo/candidato:
- `secao_id`, `eleicao_id`, `cargo`, `codigo_cargo`
- `tipo_voto` (nominal, branco, nulo, legenda)
- `codigo_candidato`, `partido`, `quantidade`

**issues** — Inconsistencias detectadas:
- `secao_id`, `codigo`, `severidade`, `descricao`, `base_legal`, `detalhes`

**totais_cargo** — Agregados por cargo em cada secao:
- `secao_id`, `cargo`, `comparecimento`, `nominais`, `brancos`, `nulos`, `legenda`, `total`

---

## Pipeline de Processamento

O sistema opera em 6 etapas sequenciais:

### 1. Download

Navegacao da API hierarquica do TSE em 4 niveis:

```
ele-c.json → estados → municipios/zonas/secoes → hashes → arquivos
```

Base: `https://resultados.tse.jus.br/oficial`

Para cada secao, baixa 5 arquivos: `.bu`, `.rdv`, `.logjez`, `.imgbu`, `.vscmr`.

O downloader e assincrono (httpx), com retry exponencial, rate limiting e download incremental (skip se o arquivo ja existe com mesmo tamanho).

### 2. Parsing

Cada tipo de arquivo e decodificado por um parser especializado:

| Arquivo | Parser | Decodificacao |
|---------|--------|---------------|
| `.bu` | `parsers.bu` | ASN.1/BER → Resultado oficial (votos por candidato, comparecimento, eleitores aptos) |
| `.logjez` | `parsers.log` | 7z/LZMA → Texto → Eventos extraidos (abertura, encerramento, reboots, erros, votos computados) |
| `.vscmr` | `parsers.signature` | ASN.1/BER → Hashes SHA-512 de cada arquivo + modelo de urna + certificado digital |
| `.rdv` | ASN.1/BER | Votos individuais anonimizados (uso futuro) |
| `.imgbu` | Texto puro | BU impresso em texto + QR Code |

A decodificacao ASN.1/BER usa as especificacoes oficiais do TSE (diretorio `spec/`), compiladas com a biblioteca `asn1tools`.

### 3. Analise

Cada secao passa por verificacoes em 4 eixos:

- **Integridade**: Calcula SHA-512 dos arquivos e compara com os hashes na assinatura digital (`.vscmr`)
- **Consistencia**: Votos totais <= eleitores aptos, comparecimento consistente entre cargos, zeresima
- **Logs**: Extrai eventos do log, detecta reboots, verifica janela horaria, conta erros e alertas
- **Estatistica**: Calcula metricas derivadas (abstencao, biometria, duracao) para deteccao de outliers

### 4. Compilacao

Os resultados de cada secao sao inseridos em DuckDB (4 tabelas). O processo roda em paralelo com `multiprocessing.Pool` (1 `BatchAnalyzer` por worker) e batch inserts via `executemany`.

### 5. Analise SQL

51 funcoes SQL analiticas operam sobre o banco compilado, agrupadas em 10 categorias (detalhadas abaixo).

### 6. Visualizacao

Dashboard Streamlit com 22 tabs, filtros interativos (turno, regiao, UF, municipio, modelo, severidade) e exportacao em Excel/TXT.

---

## Metodologias e Tecnicas

### 1. Lei de Benford

A [Lei de Benford](https://pt.wikipedia.org/wiki/Lei_de_Benford) (ou Lei do Primeiro Digito) estabelece que em conjuntos de dados numericos naturais, o digito 1 aparece como primeiro digito em ~30,1% dos casos, o 2 em ~17,6%, e assim por diante, seguindo `P(d) = log10(1 + 1/d)`.

**Aplicacao no projeto:**

- **1o digito**: Calcula a distribuicao do primeiro digito dos totais de votos por secao para cada candidato e compara com a distribuicao teorica de Benford.
- **2o digito**: Mais sensivel a manipulacao direta. Calcula `P(d) = Σ log10(1 + 1/(10k + d))` para d=0..9.
- **Teste chi-quadrado (χ²)**: Mede a divergencia estatistica entre observado e esperado por Benford.
- **V de Cramer**: Como o chi-quadrado rejeita H0 em amostras muito grandes (~940k secoes), usamos o V de Cramer como medida de efeito: `V = √(χ²/(n·(k-1)))`. Valores < 0.02 indicam conformidade excelente.
- **Granularidade**: Calcula por candidato, por estado, por turno e por cargo.

### 2. Deteccao de Outliers (Z-Score)

Para cada metrica numerica (abstencao, biometria, turnout), calcula o z-score padronizado:

```
z = (x - μ) / σ
```

Secoes com `|z| > 3` sao sinalizadas como outliers (probabilidade < 0.3% sob distribuicao normal).

**Variante local (por UF)**: Calcula media e desvio padrao dentro de cada estado, identificando secoes anomalas no contexto regional.

### 3. Analise de Impacto de Reboots

Investiga se reinicializacoes durante a votacao (C06) correlacionam com algum padrao de voto:

- **Teste chi-quadrado de independencia**: H0 = distribuicao de votos e independente da ocorrencia de reboot.
- **V de Cramer**: Magnitude do efeito da associacao reboot × candidato.
- **Segmentacao**: Compara distribuicao percentual de votos em secoes com reboot vs sem reboot, por estado e modelo.

### 4. Consistencia T1 vs T2

Compara metricas entre 1o e 2o turno para a mesma secao:

- **Variacao de comparecimento**: Mudanca significativa (> 10%) sugere anomalia logistica ou demografica.
- **Migracao de votos**: Percentual de votos redistribuidos entre candidatos do T1 para o T2 por estado.
- **Secoes faltantes**: Secoes presentes em um turno mas ausentes no outro.

### 5. Correlacao Geografica (Issues vs Resultado)

Investiga se a densidade de inconsistencias em um estado correlaciona com o resultado eleitoral:

- **Normalizacao**: Issues por secao em cada UF (evita vies pelo tamanho do estado).
- **Regressao**: Compara taxa de issues com percentual de votos por candidato.
- **Hipotese nula**: Inconsistencias sao uniformemente distribuidas, sem favorecer candidato.

### 6. Analise de Biometria

- **Secoes zero-biometria**: Identifica secoes com 0% de liberacao por biometria (todos por codigo).
- **Correlacao**: Verifica se baixa biometria correlaciona com padrao de voto anomalo.
- **Histograma**: Distribuicao do percentual de biometria em faixas de 5%.

### 7. Analise de Timing

- **Duracao da votacao**: Diferenca entre abertura e encerramento em cada secao.
- **Anomalias (z > 3)**: Secoes com duracao significativamente diferente da media.
- **Faixas**: Histograma de duracao em intervalos de 30 minutos.
- **Fusos horarios**: Ajuste automatico para AC (UTC-5), AM/MT/MS/RO/RR (UTC-4), demais (UTC-3).

### 8. Score de Confianca (0-100)

Score agregado que avalia a confiabilidade do processo eleitoral em 7 categorias:

| Categoria | Peso | O que mede |
|-----------|------|------------|
| Integridade hash (C01) | 20 | % de secoes com hash SHA-512 valido |
| Overflow de votos (C05) | 15 | Secoes com mais votos que eleitores aptos |
| Conformidade Benford | 15 | V de Cramer dos candidatos presidenciais |
| Impacto de reboots | 15 | Correlacao reboot × favorecimento de candidato |
| Consistencia T1/T2 | 15 | Variacao de comparecimento e secoes faltantes |
| Concentracao de outliers | 10 | % de secoes com z-score > 3 |
| Seguranca biometria | 10 | % de secoes com biometria funcional |

O score final e a media ponderada (0 = risco maximo, 100 = confianca maxima).

### 9. Nulos e Brancos

- Calcula percentual de nulos e brancos por secao/cargo
- Detecta outliers (z-score) de nulos/brancos
- Correlaciona taxa de nulos com resultado por candidato

### 10. Analise por Modelo de Hardware

- Normaliza taxa de issues por modelo de urna (issues por secao)
- Compara distribuicao de votos entre modelos
- Identifica se modelos mais antigos apresentam mais anomalias

---

## Codigos de Inconsistencia

### Criticas (possiveis fraudes)

| Codigo | Descricao | Base Legal |
|--------|-----------|------------|
| C01 | Hash SHA-512 do arquivo nao confere com a assinatura digital | Cerimonia de Lacracao |
| C05 | Total de votos excede eleitores aptos na secao | Art. 59, Lei 9.504/97 |
| C06 | Reinicio (reboot) durante a votacao | Procedimento de contingencia |

### Altas (anomalias significativas)

| Codigo | Descricao | Referencia |
|--------|-----------|------------|
| A01 | Divergencia de eleitores aptos entre eleicoes na mesma urna | Caderno de eleitores unico |
| A02 | Votos computados excedem comparecimento | Integridade de contagem |
| A03 | Soma de votos no log diverge do BU | Consistencia interna |
| A04 | Abertura da urna muito tardia (apos 9h) | Calendario eleitoral |
| A05 | Encerramento antes do horario legal (antes 17h) | Calendario eleitoral |
| A06 | Comparecimento divergente entre cargos | Mesma fila de votacao |

### Medias (anomalias operacionais)

| Codigo | Descricao |
|--------|-----------|
| M01 | Abertura antes do horario ajustado por fuso |
| M02 | Encerramento muito tarde |
| M04 | Numero elevado de erros no log |
| M05 | Ajustes de hora detectados |
| M06 | Alta taxa de liberacao sem biometria |

### Informativas

| Codigo | Descricao |
|--------|-----------|
| I01 | Eventos de substituicao/contingencia |
| I02 | Alertas de mesario elevados |
| I03 | Proporcao elevada de nulos |
| I04 | Proporcao elevada de brancos |
| I05 | Abstencao acima de 30% |
| I06 | Urna de reserva (reservaSecao) |

### O que NAO constitui inconsistencia

- **Nomes no log**: Erro de display LCD do terminal do mesario (comportamento documentado pelo TSE)
- **Urna substituida**: Procedimento normal de contingencia (presenca de mais de 1 hash no aux.json)
- **Ajuste de hora**: Permitido antes da abertura da votacao
- **Abstencao**: Direito do eleitor
- **Votos nulos/brancos**: Direito do eleitor

---

## Dashboard Interativo

O dashboard possui **22 tabs** agrupadas em 4 categorias:

### Visao Geral
| Tab | Conteudo |
|-----|----------|
| Veredicto Final | Score 0-100, risco por categoria, comparacao com TSE oficial, veredicto por votos e por auditoria |
| Votacao | Resultados por candidato, cargo, turno |
| Mapa do Brasil | Choropleth com metricas por UF (reboots, biometria, issues, abstencao) |

### Analises Estatisticas
| Tab | Conteudo |
|-----|----------|
| Lei de Benford | 1o e 2o digito, chi-quadrado, V de Cramer, por candidato/estado/cargo |
| Outliers Estatisticos | Z-score > 3, por UF, modelo, tipo de metrica |
| Nulos & Brancos | Outliers por cargo, correlacao com resultado |
| Distribuicao Candidato | Votacao por quartil de comparecimento, extremos por estado |

### Analises Operacionais
| Tab | Conteudo |
|-----|----------|
| Impacto de Reboots | Chi-quadrado reboot × voto, correlacao por UF e modelo |
| Analise T1 vs T2 | Migracao, comparecimento, secoes faltantes |
| Analise A03 | Divergencia log/BU, contexto do artefato T2 |
| Biometria & Seguranca | Zero-biometria, histograma, correlacao com voto |
| Timing Detalhado | Histograma de duracao, anomalias z > 3, faixas |
| Modelos & Hardware | Issues normalizado por modelo, padrao de voto |
| Secoes Reserva | Metricas reserva vs normal |
| Substituicoes & Erros | Erros por modelo, substituicoes por estado |

### Integridade e Detalhe
| Tab | Conteudo |
|-----|----------|
| Integridade Hash | Resumo SHA-512, detalhes por secao |
| Explorador de Secao | Drill-down completo com gauge de risco |
| Ranking de Risco | Score composto por UF |
| Anomalias Geograficas | Densidade issues vs resultado eleitoral |
| Inconsistencias | Distribuicao por codigo/severidade |
| Por Estado | Metricas detalhadas por UF |
| Dados Brutos | Explorador com paginacao e exportacao |

### Filtros Disponiveis
- **Turno**: Ambos / 1o / 2o
- **Regiao**: Norte / Nordeste / Centro-Oeste / Sudeste / Sul
- **Estado (UF)**: Todos os 27
- **Municipio**: Dependente da UF selecionada
- **Modelo de Urna**: UE2009-UE2020
- **Tipo de Urna**: Normal / Reserva
- **Severidade da Issue**: Critica / Alta / Media / Informativa
- **Codigo da Issue**: C01, C05, A03, etc.

### Exportacao
- **Excel (.xlsx)**: Relatorio completo com multiplas abas (KPIs, secoes, votos, issues)
- **Texto (.txt)**: Relatorio legivel para impressao

---

## Instalacao e Uso

### Pre-requisitos

- Python 3.11 ou superior
- ~500 MB de espaco em disco (183 MB Parquet + ~2.8 GB DuckDB gerado)

### Instalacao

```bash
git clone https://github.com/alissonlinneker/dataUrnas-br.git
cd dataUrnas-br
pip install -r requirements.txt
```

### Construir o Banco de Dados

O repositorio inclui os dados em formato Parquet. Para usar o dashboard, construa o DuckDB:

```bash
python scripts/build_db.py
```

Isso cria `data/db/eleicoes_2022.duckdb` (~2.8 GB) a partir dos Parquets em ~30 segundos.

### Iniciar o Dashboard

```bash
streamlit run src/dataurnas/dashboard/app.py
```

Acesse `http://localhost:8501`. Se o banco nao existir, o dashboard o constroi automaticamente na primeira execucao.

### CLI (Interface de Linha de Comando)

```bash
# Download de dados do TSE (requer internet e ~181 GB de espaco)
dataurnas download --estado sp --turno 1

# Analisar secoes baixadas
dataurnas analyze --estado sp

# Compilar banco de dados a partir dos dados brutos
dataurnas db build

# Iniciar dashboard
dataurnas dashboard
```

---

## Deploy Online

### Hugging Face Spaces (Recomendado)

O [Hugging Face Spaces](https://huggingface.co/docs/hub/spaces) e a melhor opcao gratuita para este projeto:

- **16 GB de RAM** — suficiente para o DuckDB completo (2.8 GB)
- **2 vCPU + 50 GB de disco** — processa todas as 51 funcoes de analise sem restricao
- **Docker** — controle total do ambiente
- **Auto-deploy via GitHub** — cada push na main atualiza o Space automaticamente

#### Setup Manual (5 minutos)

1. Crie uma conta em [huggingface.co](https://huggingface.co)
2. Acesse [huggingface.co/new-space](https://huggingface.co/new-space)
3. Preencha:
   - **Space name**: `dataurnas-br`
   - **SDK**: Docker
   - **Visibilidade**: Public
4. Clone o Space e copie os arquivos do projeto:

```bash
git clone https://huggingface.co/spaces/alissonlinneker/dataurnas-br hf-space
cp -r src/ scripts/ data/parquet/ spec/ requirements.txt pyproject.toml Dockerfile hf-space/
cd hf-space
git add . && git commit -m "Deploy inicial" && git push
```

5. O build leva ~3 minutos. Acesse em `https://alissonlinneker-dataurnas-br.hf.space`

#### Deploy Automatico via GitHub Actions

O repositorio inclui um workflow em `.github/workflows/deploy-hf.yml` que sincroniza automaticamente cada push na `main` com o Hugging Face Space.

**Configuracao (unica vez):**

1. No Hugging Face, gere um token de escrita em [Settings → Access Tokens](https://huggingface.co/settings/tokens)
2. No GitHub, va em **Settings → Secrets and Variables → Actions**:
   - Adicione o **Secret**: `HF_TOKEN` = seu token HF
   - Adicione as **Variables**: `HF_USERNAME` = seu usuario HF, `SPACE_NAME` = `dataurnas-br`
3. Pronto. Todo push na `main` atualiza o Space automaticamente.

#### Alternativa: Streamlit Community Cloud

O [Streamlit Community Cloud](https://streamlit.io/cloud) e mais simples (deploy com 1 clique), mas tem **apenas 1 GB de RAM**. Pode funcionar se o `get_store()` for adaptado para consultar Parquets diretamente (`SELECT * FROM read_parquet(...)`) em vez do DuckDB pre-compilado.

### Comparativo de Plataformas Gratuitas

| Caracteristica | Hugging Face Spaces | Streamlit Cloud |
|---------------|---------------------|-----------------|
| RAM | **16 GB** | 1 GB |
| CPU | 2 vCPU | Limitado |
| Disco | 50 GB | Limitado |
| DuckDB completo | **Sim** | Nao (precisa adaptacao) |
| Setup | Docker (~5 min) | 1 clique |
| Auto-deploy GitHub | Sim (Actions) | Sim (nativo) |
| Sleep por inatividade | Sim | Sim |
| Dominio | `*.hf.space` | `*.streamlit.app` |

---

## Observacao sobre Logs do 2o Turno

Os arquivos `.logjez` do 2o turno **acumulam os eventos do 1o turno** — a urna nao e limpa entre turnos. Isso significa que o log do T2 contem todos os eventos do T1 (abertura, votos, encerramento de 02/10/2022) seguidos dos eventos do T2 (30/10/2022).

O parser filtra automaticamente os eventos pela data do turno:
- 1o turno: eventos de 02/10/2022
- 2o turno: eventos de 30/10/2022

**Sem esse filtro**, 96% das secoes do T2 apresentariam:
- Falsos reboots (a abertura do T1 seria contada como reinicializacao)
- Horario de abertura incorreto (mostraria 02/10 em vez de 30/10)
- Contagem duplicada de votos no log (ratio votos_log/comparecimento = 2.0)

---

## Estrutura do Projeto

```
dataUrnas-br/
|
|-- README.md                          # Este documento
|-- pyproject.toml                     # Configuracao do pacote Python
|-- requirements.txt                   # Dependencias
|
|-- data/
|   |-- parquet/                       # Dados pre-processados (incluidos no repo)
|       |-- secoes.parquet             # 941.987 secoes (16 MB)
|       |-- votos_t1_a.parquet         # Votos T1 SP/RJ/MG/BA (72 MB)
|       |-- votos_t1_b.parquet         # Votos T1 demais UFs (65 MB)
|       |-- votos_t2.parquet           # Votos T2 todos (6 MB)
|       |-- issues.parquet             # 511.121 issues (9 MB)
|       |-- totais_cargo.parquet       # 3.074.096 totais (16 MB)
|       |-- schema.sql                 # DDL das tabelas
|
|-- src/dataurnas/
|   |-- __init__.py
|   |-- config.py                      # Constantes (API TSE, estados, modelos, cargos)
|   |-- models.py                      # Dataclasses do dominio
|   |-- cli.py                         # Interface de linha de comando (Click)
|   |
|   |-- downloader/
|   |   |-- client.py                  # Cliente HTTP assincrono (httpx)
|   |   |-- tse_api.py                 # Navegacao da API hierarquica do TSE
|   |   |-- manager.py                 # Orquestrador de downloads
|   |
|   |-- parsers/
|   |   |-- bu.py                      # Parser de BU (ASN.1/BER, V1 + V2)
|   |   |-- log.py                     # Parser de logs (7z → texto → eventos)
|   |   |-- signature.py               # Parser de assinaturas (.vscmr)
|   |   |-- asn1_helper.py             # Utilitarios ASN.1
|   |
|   |-- analyzer/
|   |   |-- batch.py                   # Analise em lote de secoes
|   |   |-- integrity.py               # Verificacao de hashes e assinaturas
|   |   |-- log_analyzer.py            # Analise de eventos de log
|   |   |-- statistical.py             # Analises estatisticas
|   |
|   |-- database/
|   |   |-- duckdb_store.py            # Persistencia DuckDB (CRUD + queries)
|   |
|   |-- dashboard/
|       |-- app.py                     # Aplicacao Streamlit (3.900 linhas, 22 tabs)
|       |-- analysis.py                # Funcoes de analise SQL (3.550 linhas, 51 funcoes)
|
|-- scripts/
|   |-- build_db.py                    # Reconstroi DuckDB a partir dos Parquets
|   |-- download_full.py               # Download completo T1
|   |-- download_full_2t.py            # Download completo T2
|   |-- download_and_build.py          # Download + compilacao simultanea
|   |-- rebuild_db.py                  # Rebuild sequencial
|   |-- rebuild_parallel.py            # Rebuild paralelo (multiprocessing)
|
|-- spec/
    |-- v1/                            # Especificacoes ASN.1 formato 2022
    |   |-- bu.asn1
    |   |-- rdv.asn1
    |   |-- assinatura.asn1
    |-- v2/                            # Especificacoes ASN.1 formato 2024+
        |-- bu.asn1
        |-- rdv.asn1
        |-- assinatura.asn1
```

---

## Fontes de Dados e Legislacao

### Fontes

Todos os dados utilizados sao **publicos** e disponibilizados pelo TSE:

- [API de Resultados do TSE](https://resultados.tse.jus.br/oficial/) — Arquivos de urna (BU, RDV, log, assinaturas)
- [Repositorio de Dados Eleitorais](https://dadosabertos.tse.jus.br/) — Dados abertos complementares
- [Especificacoes ASN.1](https://www.tse.jus.br/eleicoes/urna-eletronica/seguranca-da-urna) — Definicoes de formato dos arquivos

### Legislacao

| Lei/Resolucao | Aspecto |
|---------------|---------|
| Lei 9.504/1997, Art. 59 | Assinatura digital ao arquivo de votos |
| Lei 9.504/1997, Art. 59-A | Auditoria e fiscalizacao dos sistemas eletronicos |
| Lei 9.504/1997, Art. 66 | Direito de fiscalizacao por partidos e coligacoes |
| Lei 10.740/2003 | Instituicao do Registro Digital do Voto (RDV) |
| Resolucao TSE 23.673/2021 | Normas de fiscalizacao e auditoria |
| Resolucao TSE 23.611/2019, Art. 206 | Publicacao de BUs em ate 3 dias |

### Mecanismos de Auditoria Legais

1. **Teste Publico de Seguranca (TPS)**: Hacking etico por especialistas convidados pelo TSE
2. **Teste de Integridade**: Votacao simulada em urnas sorteadas em ambiente controlado
3. **Zeresima**: Comprovante de zero votos antes da abertura da votacao
4. **Inspecao de Codigo-Fonte**: Aberta 12 meses antes da eleicao para entidades fiscalizadoras
5. **Cerimonia de Lacracao**: Assinatura digital e distribuicao de hashes dos sistemas
6. **Verificacao independente**: BUs publicos permitem totalizacao paralela (o que este projeto faz)

---

## Tecnologias

| Componente | Tecnologia | Funcao |
|------------|------------|--------|
| Linguagem | Python 3.11+ | Base do projeto |
| ASN.1/BER | asn1tools | Decodificacao de BU, RDV e assinaturas |
| Criptografia | ecpy, cryptography | Validacao Ed25519, ECDSA, SHA-512 |
| HTTP | httpx | Download assincrono com retry |
| Descompressao | py7zr | Logs da urna (7z/LZMA) |
| Banco de dados | DuckDB | OLAP colunar, queries analiticas |
| Armazenamento | Parquet + ZSTD | Compressao eficiente para distribuicao |
| Dashboard | Streamlit | Interface web interativa |
| Graficos | Plotly | Visualizacoes interativas (choropleth, gauges, barras) |
| Estatistica | scipy, numpy | Chi-quadrado, z-score, V de Cramer |
| Exportacao | openpyxl | Geracao de relatorios Excel |
| CLI | Click | Interface de linha de comando |

---

## Licenca

MIT — Livre para uso, modificacao e distribuicao.
