# DataUrnas-BR

[![Demo Online](https://img.shields.io/badge/%F0%9F%97%B3%EF%B8%8F%20Demo-Hugging%20Face%20Spaces-blue)](https://huggingface.co/spaces/alissonlinneker/dataurnas-br)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)

Sistema de auditoria independente dos dados eleitorais brasileiros, focado nas Eleições Presidenciais de 2022.

O projeto baixa, decodifica e analisa os **5 arquivos públicos** de cada urna eletrônica — Boletim de Urna (BU), Registro Digital do Voto (RDV), Log da Urna, Imagem do BU e Assinaturas Digitais — disponibilizados pelo [TSE](https://resultados.tse.jus.br/oficial/), aplicando verificações de integridade criptográfica, consistência lógica, detecção de anomalias estatísticas e análise de padrões operacionais sobre **941.987 seções eleitorais** em todas as 27 UFs do Brasil.

> **[Acesse o dashboard online](https://huggingface.co/spaces/alissonlinneker/dataurnas-br)** — sem necessidade de instalação.

---

## Sumário

- [Motivação](#motivação)
- [Cobertura dos Dados](#cobertura-dos-dados)
- [Arquitetura do Sistema](#arquitetura-do-sistema)
- [Pipeline de Processamento](#pipeline-de-processamento)
- [Metodologias e Técnicas](#metodologias-e-técnicas)
- [Códigos de Inconsistência](#códigos-de-inconsistência)
- [Dashboard Interativo](#dashboard-interativo)
- [Instalação e Uso](#instalação-e-uso)
- [Deploy Online](#deploy-online)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Fontes de Dados e Legislação](#fontes-de-dados-e-legislação)
- [Tecnologias](#tecnologias)
- [Licença](#licença)

---

## Motivação

A urna eletrônica brasileira gera, em cada seção eleitoral, um conjunto de arquivos públicos que permitem auditoria independente do processo de votação. O TSE disponibiliza esses arquivos em sua API pública após o encerramento da votação.

Este projeto realiza a **totalização paralela independente** — ou seja, refaz todo o processo de contagem a partir dos dados brutos, sem depender dos resultados oficiais publicados pelo TSE — e aplica mais de 50 verificações automatizadas cobrindo integridade criptográfica, consistência lógica, padrões estatísticos e comportamento operacional das urnas.

O objetivo é responder de forma técnica e transparente: **os dados públicos das urnas são consistentes entre si e com os resultados oficiais?**

---

## Cobertura dos Dados

O banco de dados incluído neste repositório cobre a **totalidade** das seções eleitorais das Eleições Presidenciais de 2022:

| Métrica | Valor |
|---------|-------|
| Seções eleitorais processadas | 941.987 |
| Votos individuais catalogados | 72.943.229 |
| Inconsistências detectadas | 511.121 |
| Totais por cargo/seção | 3.074.096 |
| Estados (UFs) | 27 (todos) |
| 1o Turno (02/10/2022) | 470.986 seções |
| 2o Turno (30/10/2022) | 471.001 seções |
| Modelos de urna | UE2009, UE2010, UE2011, UE2013, UE2015, UE2020 |
| Versão de software | 8.26.0.0 Onça-pintada (todas) |

Os dados brutos totalizam ~181 GB. Para viabilizar distribuição via repositório, os dados processados foram exportados em **Parquet com compressão ZSTD**, totalizando apenas **183 MB**.

---

## Arquitetura do Sistema

```
+------------------------------------------------------------------+
|                    DASHBOARD WEB (Streamlit)                      |
|  22 tabs de análise | Filtros interativos | Exportação Excel/TXT  |
+------------------------------------------------------------------+
         |                    |                    |
+--------v--------+  +-------v--------+  +-------v---------+
|  51 FUNÇÕES DE  |  |   SCORE DE     |  |   EXPORTAÇÃO    |
|  ANÁLISE SQL    |  |   CONFIANÇA    |  |   Excel / TXT   |
+-----------------+  +----------------+  +-----------------+
         |                    |                    |
+------------------------------------------------------------------+
|                    MOTOR DE ANÁLISE                                |
|                                                                    |
|  Integridade:  Hash SHA-512 | Ed25519/ECDSA | Chain hashing       |
|  Consistência: Votos <= Aptos | Zerésima | Cross-turno            |
|  Logs:         Reboots | Timing | Erros | Substituições           |
|  Estatística:  Benford | Z-score | Chi-quadrado | Cramer's V      |
+------------------------------------------------------------------+
         |                    |                    |
+--------v--------+  +-------v--------+  +-------v---------+
|  PARSER DE BU   |  | PARSER DE LOG  |  | PARSER ASSINAT. |
|  (ASN.1/BER)    |  | (7z → texto)   |  | (ASN.1/BER)     |
+-----------------+  +----------------+  +-----------------+
         |                    |                    |
+------------------------------------------------------------------+
|                DOWNLOADER ASSÍNCRONO TSE                           |
|  API hierárquica (4 níveis) | Retry | Rate limiting               |
+------------------------------------------------------------------+
         |
+--------v---------------------------------------------------------+
|              BANCO DE DADOS (DuckDB)                              |
|  secoes | votos | issues | totais_cargo                          |
+------------------------------------------------------------------+
```

### Esquema do Banco de Dados

**seções** — Metadados de cada seção eleitoral:
- Identificação: `id`, `turno`, `uf`, `regiao`, `municipio`, `zona`, `secao`
- Hardware: `modelo_urna`, `tipo_urna`, `versao_sw`
- Eleitores: `eleitores_aptos`, `comparecimento`, `pct_abstencao`
- Biometria: `lib_codigo`, `comp_biometrico`, `pct_biometria`
- Timing: `hora_abertura`, `hora_encerramento`, `duracao_min`, `fuso_offset`
- Operacional: `reboots`, `erros_log`, `alertas_mesario`, `votos_log`, `substituicoes`
- Flags: `is_reserva`, `has_issues`, `n_issues`

**votos** — Cada voto por seção/cargo/candidato:
- `secao_id`, `eleicao_id`, `cargo`, `codigo_cargo`
- `tipo_voto` (nominal, branco, nulo, legenda)
- `codigo_candidato`, `partido`, `quantidade`

**issues** — Inconsistências detectadas:
- `secao_id`, `codigo`, `severidade`, `descricao`, `base_legal`, `detalhes`

**totais_cargo** — Agregados por cargo em cada seção:
- `secao_id`, `cargo`, `comparecimento`, `nominais`, `brancos`, `nulos`, `legenda`, `total`

---

## Pipeline de Processamento

O sistema opera em 6 etapas sequenciais:

### 1. Download

Navegação da API hierárquica do TSE em 4 níveis:

```
ele-c.json → estados → municipios/zonas/secoes → hashes → arquivos
```

Base: `https://resultados.tse.jus.br/oficial`

Para cada seção, baixa 5 arquivos: `.bu`, `.rdv`, `.logjez`, `.imgbu`, `.vscmr`.

O downloader é assíncrono (httpx), com retry exponencial, rate limiting e download incremental (skip se o arquivo já existe com mesmo tamanho).

### 2. Parsing

Cada tipo de arquivo é decodificado por um parser especializado:

| Arquivo | Parser | Decodificação |
|---------|--------|---------------|
| `.bu` | `parsers.bu` | ASN.1/BER → Resultado oficial (votos por candidato, comparecimento, eleitores aptos) |
| `.logjez` | `parsers.log` | 7z/LZMA → Texto → Eventos extraídos (abertura, encerramento, reboots, erros, votos computados) |
| `.vscmr` | `parsers.signature` | ASN.1/BER → Hashes SHA-512 de cada arquivo + modelo de urna + certificado digital |
| `.rdv` | ASN.1/BER | Votos individuais anonimizados (uso futuro) |
| `.imgbu` | Texto puro | BU impresso em texto + QR Code |

A decodificação ASN.1/BER usa as especificações oficiais do TSE (diretório `spec/`), compiladas com a biblioteca `asn1tools`.

### 3. Análise

Cada seção passa por verificações em 4 eixos:

- **Integridade**: Calcula SHA-512 dos arquivos e compara com os hashes na assinatura digital (`.vscmr`)
- **Consistência**: Votos totais <= eleitores aptos, comparecimento consistente entre cargos, zerésima
- **Logs**: Extrai eventos do log, detecta reboots, verifica janela horária, conta erros e alertas
- **Estatística**: Calcula métricas derivadas (abstenção, biometria, duração) para detecção de outliers

### 4. Compilação

Os resultados de cada seção são inseridos em DuckDB (4 tabelas). O processo roda em paralelo com `multiprocessing.Pool` (1 `BatchAnalyzer` por worker) e batch inserts via `executemany`.

### 5. Análise SQL

51 funções SQL analíticas operam sobre o banco compilado, agrupadas em 10 categorias (detalhadas abaixo).

### 6. Visualização

Dashboard Streamlit com 22 tabs, filtros interativos (turno, região, UF, município, modelo, severidade) e exportação em Excel/TXT.

---

## Metodologias e Técnicas

### 1. Lei de Benford

A [Lei de Benford](https://pt.wikipedia.org/wiki/Lei_de_Benford) (ou Lei do Primeiro Dígito) estabelece que em conjuntos de dados numéricos naturais, o dígito 1 aparece como primeiro dígito em ~30,1% dos casos, o 2 em ~17,6%, e assim por diante, seguindo `P(d) = log10(1 + 1/d)`.

**Aplicação no projeto:**

- **1o dígito**: Calcula a distribuição do primeiro dígito dos totais de votos por seção para cada candidato e compara com a distribuição teórica de Benford.
- **2o dígito**: Mais sensível a manipulação direta. Calcula `P(d) = Σ log10(1 + 1/(10k + d))` para d=0..9.
- **Teste chi-quadrado (χ²)**: Mede a divergência estatística entre observado e esperado por Benford.
- **V de Cramer**: Como o chi-quadrado rejeita H0 em amostras muito grandes (~940k seções), usamos o V de Cramer como medida de efeito: `V = √(χ²/(n·(k-1)))`. Valores < 0.02 indicam conformidade excelente.
- **Granularidade**: Calcula por candidato, por estado, por turno e por cargo.

### 2. Detecção de Outliers (Z-Score)

Para cada métrica numérica (abstenção, biometria, turnout), calcula o z-score padronizado:

```
z = (x - μ) / σ
```

Seções com `|z| > 3` são sinalizadas como outliers (probabilidade < 0.3% sob distribuição normal).

**Variante local (por UF)**: Calcula média e desvio padrão dentro de cada estado, identificando seções anômalas no contexto regional.

### 3. Análise de Impacto de Reboots

Investiga se reinicializações durante a votação (C06) correlacionam com algum padrão de voto:

- **Teste chi-quadrado de independência**: H0 = distribuição de votos é independente da ocorrência de reboot.
- **V de Cramer**: Magnitude do efeito da associação reboot × candidato.
- **Segmentação**: Compara distribuição percentual de votos em seções com reboot vs sem reboot, por estado e modelo.

### 4. Consistência T1 vs T2

Compara métricas entre 1o e 2o turno para a mesma seção:

- **Variação de comparecimento**: Mudança significativa (> 10%) sugere anomalia logística ou demográfica.
- **Migração de votos**: Percentual de votos redistribuídos entre candidatos do T1 para o T2 por estado.
- **Seções faltantes**: Seções presentes em um turno mas ausentes no outro.

### 5. Correlação Geográfica (Issues vs Resultado)

Investiga se a densidade de inconsistências em um estado correlaciona com o resultado eleitoral:

- **Normalização**: Issues por seção em cada UF (evita viés pelo tamanho do estado).
- **Regressão**: Compara taxa de issues com percentual de votos por candidato.
- **Hipótese nula**: Inconsistências são uniformemente distribuídas, sem favorecer candidato.

### 6. Análise de Biometria

- **Seções zero-biometria**: Identifica seções com 0% de liberação por biometria (todos por código).
- **Correlação**: Verifica se baixa biometria correlaciona com padrão de voto anômalo.
- **Histograma**: Distribuição do percentual de biometria em faixas de 5%.

### 7. Análise de Timing

- **Duração da votação**: Diferença entre abertura e encerramento em cada seção.
- **Anomalias (z > 3)**: Seções com duração significativamente diferente da média.
- **Faixas**: Histograma de duração em intervalos de 30 minutos.
- **Fusos horários**: Ajuste automático para AC (UTC-5), AM/MT/MS/RO/RR (UTC-4), demais (UTC-3).

### 8. Score de Confiança (0-100)

Score agregado que avalia a confiabilidade do processo eleitoral em 7 categorias:

| Categoria | Peso | O que mede |
|-----------|------|------------|
| Integridade hash (C01) | 20 | % de seções com hash SHA-512 válido |
| Overflow de votos (C05) | 15 | Seções com mais votos que eleitores aptos |
| Conformidade Benford | 15 | V de Cramer dos candidatos presidenciais |
| Impacto de reboots | 15 | Correlação reboot × favorecimento de candidato |
| Consistência T1/T2 | 15 | Variação de comparecimento e seções faltantes |
| Concentração de outliers | 10 | % de seções com z-score > 3 |
| Segurança biometria | 10 | % de seções com biometria funcional |

O score final é a média ponderada (0 = risco máximo, 100 = confiança máxima).

### 9. Nulos e Brancos

- Calcula percentual de nulos e brancos por seção/cargo
- Detecta outliers (z-score) de nulos/brancos
- Correlaciona taxa de nulos com resultado por candidato

### 10. Análise por Modelo de Hardware

- Normaliza taxa de issues por modelo de urna (issues por seção)
- Compara distribuição de votos entre modelos
- Identifica se modelos mais antigos apresentam mais anomalias

---

## Códigos de Inconsistência

### Críticas (possíveis fraudes)

| Código | Descrição | Base Legal |
|--------|-----------|------------|
| C01 | Hash SHA-512 do arquivo não confere com a assinatura digital | Cerimônia de Lacração |
| C05 | Total de votos excede eleitores aptos na seção | Art. 59, Lei 9.504/97 |
| C06 | Reinício (reboot) durante a votação | Procedimento de contingência |

### Altas (anomalias significativas)

| Código | Descrição | Referência |
|--------|-----------|------------|
| A01 | Divergência de eleitores aptos entre eleições na mesma urna | Caderno de eleitores único |
| A02 | Votos computados excedem comparecimento | Integridade de contagem |
| A03 | Soma de votos no log diverge do BU | Consistência interna |
| A04 | Abertura da urna muito tardia (após 9h) | Calendário eleitoral |
| A05 | Encerramento antes do horário legal (antes 17h) | Calendário eleitoral |
| A06 | Comparecimento divergente entre cargos | Mesma fila de votação |

### Médias (anomalias operacionais)

| Código | Descrição |
|--------|-----------|
| M01 | Abertura antes do horário ajustado por fuso |
| M02 | Encerramento muito tarde |
| M04 | Número elevado de erros no log |
| M05 | Ajustes de hora detectados |
| M06 | Alta taxa de liberação sem biometria |

### Informativas

| Código | Descrição |
|--------|-----------|
| I01 | Eventos de substituição/contingência |
| I02 | Alertas de mesário elevados |
| I03 | Proporção elevada de nulos |
| I04 | Proporção elevada de brancos |
| I05 | Abstenção acima de 30% |
| I06 | Urna de reserva (reservaSecao) |

### O que NÃO constitui inconsistência

- **Nomes no log**: Erro de display LCD do terminal do mesário (comportamento documentado pelo TSE)
- **Urna substituída**: Procedimento normal de contingência (presença de mais de 1 hash no aux.json)
- **Ajuste de hora**: Permitido antes da abertura da votação
- **Abstenção**: Direito do eleitor
- **Votos nulos/brancos**: Direito do eleitor

---

## Dashboard Interativo

O dashboard possui **22 tabs** agrupadas em 4 categorias:

### Visão Geral
| Tab | Conteúdo |
|-----|----------|
| Veredicto Final | Score 0-100, risco por categoria, comparação com TSE oficial, veredicto por votos e por auditoria |
| Votação | Resultados por candidato, cargo, turno |
| Mapa do Brasil | Choropleth com métricas por UF (reboots, biometria, issues, abstenção) |

### Análises Estatísticas
| Tab | Conteúdo |
|-----|----------|
| Lei de Benford | 1o e 2o dígito, chi-quadrado, V de Cramer, por candidato/estado/cargo |
| Outliers Estatísticos | Z-score > 3, por UF, modelo, tipo de métrica |
| Nulos & Brancos | Outliers por cargo, correlação com resultado |
| Distribuição Candidato | Votação por quartil de comparecimento, extremos por estado |

### Análises Operacionais
| Tab | Conteúdo |
|-----|----------|
| Impacto de Reboots | Chi-quadrado reboot × voto, correlação por UF e modelo |
| Análise T1 vs T2 | Migração, comparecimento, seções faltantes |
| Análise A03 | Divergência log/BU, contexto do artefato T2 |
| Biometria & Segurança | Zero-biometria, histograma, correlação com voto |
| Timing Detalhado | Histograma de duração, anomalias z > 3, faixas |
| Modelos & Hardware | Issues normalizado por modelo, padrão de voto |
| Seções Reserva | Métricas reserva vs normal |
| Substituições & Erros | Erros por modelo, substituições por estado |

### Integridade e Detalhe
| Tab | Conteúdo |
|-----|----------|
| Integridade Hash | Resumo SHA-512, detalhes por seção |
| Explorador de Seção | Drill-down completo com gauge de risco |
| Ranking de Risco | Score composto por UF |
| Anomalias Geográficas | Densidade issues vs resultado eleitoral |
| Inconsistências | Distribuição por código/severidade |
| Por Estado | Métricas detalhadas por UF |
| Dados Brutos | Explorador com paginação e exportação |

### Filtros Disponíveis
- **Turno**: Ambos / 1o / 2o
- **Região**: Norte / Nordeste / Centro-Oeste / Sudeste / Sul
- **Estado (UF)**: Todos os 27
- **Município**: Dependente da UF selecionada
- **Modelo de Urna**: UE2009-UE2020
- **Tipo de Urna**: Normal / Reserva
- **Severidade da Issue**: Crítica / Alta / Média / Informativa
- **Código da Issue**: C01, C05, A03, etc.

### Exportação
- **Excel (.xlsx)**: Relatório completo com múltiplas abas (KPIs, seções, votos, issues)
- **Texto (.txt)**: Relatório legível para impressão

---

## Instalação e Uso

### Pré-requisitos

- Python 3.11 ou superior
- ~500 MB de espaço em disco (183 MB Parquet + ~2.8 GB DuckDB gerado)

### Instalação

```bash
git clone https://github.com/alissonlinneker/dataUrnas-br.git
cd dataUrnas-br
pip install -r requirements.txt
```

### Construir o Banco de Dados

O repositório inclui os dados em formato Parquet. Para usar o dashboard, construa o DuckDB:

```bash
python scripts/build_db.py
```

Isso cria `data/db/eleicoes_2022.duckdb` (~2.8 GB) a partir dos Parquets em ~30 segundos.

### Iniciar o Dashboard

```bash
streamlit run src/dataurnas/dashboard/app.py
```

Acesse `http://localhost:8501`. Se o banco não existir, o dashboard o constrói automaticamente na primeira execução.

### CLI (Interface de Linha de Comando)

```bash
# Download de dados do TSE (requer internet e ~181 GB de espaço)
dataurnas download --estado sp --turno 1

# Analisar seções baixadas
dataurnas analyze --estado sp

# Compilar banco de dados a partir dos dados brutos
dataurnas db build

# Iniciar dashboard
dataurnas dashboard
```

---

## Deploy Online

### Hugging Face Spaces (Recomendado)

O [Hugging Face Spaces](https://huggingface.co/docs/hub/spaces) é a melhor opção gratuita para este projeto:

- **16 GB de RAM** — suficiente para o DuckDB completo (2.8 GB)
- **2 vCPU + 50 GB de disco** — processa todas as 51 funções de análise sem restrição
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

#### Deploy Automático via GitHub Actions

O repositório inclui um workflow em `.github/workflows/deploy-hf.yml` que sincroniza automaticamente cada push na `main` com o Hugging Face Space.

**Configuração (única vez):**

1. No Hugging Face, gere um token de escrita em [Settings → Access Tokens](https://huggingface.co/settings/tokens)
2. No GitHub, vá em **Settings → Secrets and Variables → Actions**:
   - Adicione o **Secret**: `HF_TOKEN` = seu token HF
   - Adicione as **Variables**: `HF_USERNAME` = seu usuário HF, `SPACE_NAME` = `dataurnas-br`
3. Pronto. Todo push na `main` atualiza o Space automaticamente.

#### Alternativa: Streamlit Community Cloud

O [Streamlit Community Cloud](https://streamlit.io/cloud) é mais simples (deploy com 1 clique), mas tem **apenas 1 GB de RAM**. Pode funcionar se o `get_store()` for adaptado para consultar Parquets diretamente (`SELECT * FROM read_parquet(...)`) em vez do DuckDB pré-compilado.

### Comparativo de Plataformas Gratuitas

| Característica | Hugging Face Spaces | Streamlit Cloud |
|---------------|---------------------|-----------------|
| RAM | **16 GB** | 1 GB |
| CPU | 2 vCPU | Limitado |
| Disco | 50 GB | Limitado |
| DuckDB completo | **Sim** | Não (precisa adaptação) |
| Setup | Docker (~5 min) | 1 clique |
| Auto-deploy GitHub | Sim (Actions) | Sim (nativo) |
| Sleep por inatividade | Sim | Sim |
| Domínio | `*.hf.space` | `*.streamlit.app` |

---

## Observação sobre Logs do 2o Turno

Os arquivos `.logjez` do 2o turno **acumulam os eventos do 1o turno** — a urna não é limpa entre turnos. Isso significa que o log do T2 contém todos os eventos do T1 (abertura, votos, encerramento de 02/10/2022) seguidos dos eventos do T2 (30/10/2022).

O parser filtra automaticamente os eventos pela data do turno:
- 1o turno: eventos de 02/10/2022
- 2o turno: eventos de 30/10/2022

**Sem esse filtro**, 96% das seções do T2 apresentariam:
- Falsos reboots (a abertura do T1 seria contada como reinicialização)
- Horário de abertura incorreto (mostraria 02/10 em vez de 30/10)
- Contagem duplicada de votos no log (ratio votos_log/comparecimento = 2.0)

---

## Estrutura do Projeto

```
dataUrnas-br/
|
|-- README.md                          # Este documento
|-- pyproject.toml                     # Configuração do pacote Python
|-- requirements.txt                   # Dependências
|
|-- data/
|   |-- parquet/                       # Dados pré-processados (incluídos no repo)
|       |-- secoes.parquet             # 941.987 seções (16 MB)
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
|   |-- models.py                      # Dataclasses do domínio
|   |-- cli.py                         # Interface de linha de comando (Click)
|   |
|   |-- downloader/
|   |   |-- client.py                  # Cliente HTTP assíncrono (httpx)
|   |   |-- tse_api.py                 # Navegação da API hierárquica do TSE
|   |   |-- manager.py                 # Orquestrador de downloads
|   |
|   |-- parsers/
|   |   |-- bu.py                      # Parser de BU (ASN.1/BER, V1 + V2)
|   |   |-- log.py                     # Parser de logs (7z → texto → eventos)
|   |   |-- signature.py               # Parser de assinaturas (.vscmr)
|   |   |-- asn1_helper.py             # Utilitários ASN.1
|   |
|   |-- analyzer/
|   |   |-- batch.py                   # Análise em lote de seções
|   |   |-- integrity.py               # Verificação de hashes e assinaturas
|   |   |-- log_analyzer.py            # Análise de eventos de log
|   |   |-- statistical.py             # Análises estatísticas
|   |
|   |-- database/
|   |   |-- duckdb_store.py            # Persistência DuckDB (CRUD + queries)
|   |
|   |-- dashboard/
|       |-- app.py                     # Aplicação Streamlit (3.900 linhas, 22 tabs)
|       |-- analysis.py                # Funções de análise SQL (3.550 linhas, 51 funções)
|
|-- scripts/
|   |-- build_db.py                    # Reconstrói DuckDB a partir dos Parquets
|   |-- download_full.py               # Download completo T1
|   |-- download_full_2t.py            # Download completo T2
|   |-- download_and_build.py          # Download + compilação simultânea
|   |-- rebuild_db.py                  # Rebuild sequencial
|   |-- rebuild_parallel.py            # Rebuild paralelo (multiprocessing)
|
|-- spec/
    |-- v1/                            # Especificações ASN.1 formato 2022
    |   |-- bu.asn1
    |   |-- rdv.asn1
    |   |-- assinatura.asn1
    |-- v2/                            # Especificações ASN.1 formato 2024+
        |-- bu.asn1
        |-- rdv.asn1
        |-- assinatura.asn1
```

---

## Fontes de Dados e Legislação

### Fontes

Todos os dados utilizados são **públicos** e disponibilizados pelo TSE:

- [API de Resultados do TSE](https://resultados.tse.jus.br/oficial/) — Arquivos de urna (BU, RDV, log, assinaturas)
- [Repositório de Dados Eleitorais](https://dadosabertos.tse.jus.br/) — Dados abertos complementares
- [Especificações ASN.1](https://www.tse.jus.br/eleicoes/urna-eletronica/seguranca-da-urna) — Definições de formato dos arquivos

### Legislação

| Lei/Resolução | Aspecto |
|---------------|---------|
| Lei 9.504/1997, Art. 59 | Assinatura digital ao arquivo de votos |
| Lei 9.504/1997, Art. 59-A | Auditoria e fiscalização dos sistemas eletrônicos |
| Lei 9.504/1997, Art. 66 | Direito de fiscalização por partidos e coligações |
| Lei 10.740/2003 | Instituição do Registro Digital do Voto (RDV) |
| Resolução TSE 23.673/2021 | Normas de fiscalização e auditoria |
| Resolução TSE 23.611/2019, Art. 206 | Publicação de BUs em até 3 dias |

### Mecanismos de Auditoria Legais

1. **Teste Público de Segurança (TPS)**: Hacking ético por especialistas convidados pelo TSE
2. **Teste de Integridade**: Votação simulada em urnas sorteadas em ambiente controlado
3. **Zerésima**: Comprovante de zero votos antes da abertura da votação
4. **Inspeção de Código-Fonte**: Aberta 12 meses antes da eleição para entidades fiscalizadoras
5. **Cerimônia de Lacração**: Assinatura digital e distribuição de hashes dos sistemas
6. **Verificação independente**: BUs públicos permitem totalização paralela (o que este projeto faz)

---

## Tecnologias

| Componente | Tecnologia | Função |
|------------|------------|--------|
| Linguagem | Python 3.11+ | Base do projeto |
| ASN.1/BER | asn1tools | Decodificação de BU, RDV e assinaturas |
| Criptografia | ecpy, cryptography | Validação Ed25519, ECDSA, SHA-512 |
| HTTP | httpx | Download assíncrono com retry |
| Descompressão | py7zr | Logs da urna (7z/LZMA) |
| Banco de dados | DuckDB | OLAP colunar, queries analíticas |
| Armazenamento | Parquet + ZSTD | Compressão eficiente para distribuição |
| Dashboard | Streamlit | Interface web interativa |
| Gráficos | Plotly | Visualizações interativas (choropleth, gauges, barras) |
| Estatística | scipy, numpy | Chi-quadrado, z-score, V de Cramer |
| Exportação | openpyxl | Geração de relatórios Excel |
| CLI | Click | Interface de linha de comando |

---

## Licença

MIT — Livre para uso, modificação e distribuição.
