"""Funções de análise SQL para o dashboard de auditoria eleitoral.

Cada função recebe (store, f) onde:
- store: DuckDBStore com métodos query_df(sql, params) e query(sql, params)
- f: dict com filtros do dashboard:
    f['where']        -> cláusula WHERE para tabela seções (alias s.)
    f['params']       -> parâmetros para f['where']
    f['issue_where']  -> cláusula WHERE para join issues (alias i.) + seções (s.)
    f['issue_params'] -> parâmetros para f['issue_where']
    f['turno']        -> None (ambos), 1 ou 2
"""

import math
import textwrap
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
from scipy import stats

# ============================================================
# Constantes
# ============================================================

BENFORD_EXPECTED = {d: math.log10(1 + 1 / d) for d in range(1, 10)}

# Benford 2o digito: P(d) = sum_{k=1}^{9} log10(1 + 1/(10k + d)), d = 0..9
BENFORD_SECOND_EXPECTED = {}
for _d in range(0, 10):
    _prob = sum(math.log10(1 + 1 / (10 * _k + _d)) for _k in range(1, 10))
    BENFORD_SECOND_EXPECTED[_d] = _prob

# Resultados oficiais TSE 2022
OFICIAL_1T = {
    "turno": 1,
    "eleitores_aptos": 156454011,
    "comparecimento": 123682372,
    "votos_validos": 118229719,
    "lula_pct": 48.43,
    "bolso_pct": 43.20,
    "diferenca": 6187159,
}

OFICIAL_2T = {
    "turno": 2,
    "eleitores_aptos": 156454011,
    "comparecimento": 124252796,
    "votos_validos": 118556778,
    "lula_pct": 50.90,
    "bolso_pct": 49.10,
    "diferenca": 2139645,
}

ISSUE_DESCRICOES = {
    "C01": "Hash SHA-512 invalido",
    "C05": "Total de votos > eleitores aptos",
    "C06": "Reinicio durante votacao",
    "A01": "Divergencia de eleitores aptos entre eleicoes",
    "A02": "Votos > comparecimento",
    "A03": "Divergencia de votos no log vs BU",
    "A04": "Abertura muito tardia",
    "A05": "Encerramento antes do horario",
    "A06": "Comparecimento divergente entre cargos",
    "I01": "Eventos de substituicao/contingencia",
    "I02": "Alertas de mesario elevados",
    "I03": "Proporcao elevada de nulos",
    "I04": "Proporcao elevada de brancos",
    "I05": "Abstencao acima de 30%",
    "I06": "Urna nao padrao (reservaSecao)",
    "M01": "Abertura antes do horario ajustado",
    "M02": "Encerramento muito tarde",
    "M04": "Numero elevado de erros no log",
    "M05": "Ajustes de hora detectados",
    "M06": "Alta taxa de liberacao sem biometria",
}

_EMPTY_DF_CACHE = {}


def _empty_df(columns):
    """Retorna DataFrame vazio com as colunas especificadas."""
    key = tuple(columns)
    if key not in _EMPTY_DF_CACHE:
        _EMPTY_DF_CACHE[key] = pd.DataFrame(columns=columns)
    return _EMPTY_DF_CACHE[key].copy()


def _cramers_v(chi2, n, k):
    """Calcula Cramer's V a partir de chi2, n (total de observacoes), k (categorias).

    V = sqrt(chi2 / (n * (k - 1)))
    """
    if n == 0 or k <= 1:
        return 0.0
    return math.sqrt(chi2 / (n * (k - 1)))


def _benford_cramers_v_score(cramers_v_value):
    """Converte Cramer's V em score (0-100) para o confidence score.

    V < 0.05:  conforme     -> score 100
    V < 0.10:  aceitavel    -> score 75
    V < 0.20:  atencao      -> score 50
    V >= 0.20: nao conforme -> score 0
    """
    if cramers_v_value < 0.05:
        return 100.0
    elif cramers_v_value < 0.10:
        return 75.0
    elif cramers_v_value < 0.20:
        return 50.0
    else:
        return 0.0


# ============================================================
# 1. Lei de Benford
# ============================================================

def benford_first_digit(store, f) -> pd.DataFrame:
    """Distribuição do primeiro dígito dos votos por candidato (Presidente).

    Returns:
        DataFrame com: candidato, digito (1-9), contagem, pct_observado, pct_esperado
    """
    cols = ["candidato", "digito", "contagem", "pct_observado", "pct_esperado"]

    df = store.query_df(f"""
        SELECT
            v.codigo_candidato AS candidato,
            v.quantidade
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.quantidade > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    # Extrair primeiro dígito
    df["digito"] = df["quantidade"].astype(str).str[0].astype(int)
    df = df[df["digito"].between(1, 9)]

    if df.empty:
        return _empty_df(cols)

    # Contar por candidato e dígito
    grouped = df.groupby(["candidato", "digito"]).size().reset_index(name="contagem")

    # Calcular percentual observado por candidato
    totals = grouped.groupby("candidato")["contagem"].transform("sum")
    grouped["pct_observado"] = np.where(
        totals > 0,
        (grouped["contagem"] / totals * 100).round(4),
        0.0,
    )

    # Adicionar percentual esperado (Benford)
    grouped["pct_esperado"] = grouped["digito"].map(
        lambda d: round(BENFORD_EXPECTED[d] * 100, 4)
    )

    return grouped[cols].sort_values(["candidato", "digito"]).reset_index(drop=True)


def benford_chi_squared(store, f) -> pd.DataFrame:
    """Teste chi-quadrado por candidato vs distribuição esperada de Benford.

    Returns:
        DataFrame com: candidato, chi2, p_value, conforme (bool: p_value > 0.05)
    """
    cols = ["candidato", "chi2", "p_value", "conforme"]

    digit_df = benford_first_digit(store, f)
    if digit_df.empty:
        return _empty_df(cols)

    results = []
    for candidato, grp in digit_df.groupby("candidato"):
        # Garantir que todos os dígitos 1-9 estejam presentes
        full = pd.DataFrame({"digito": range(1, 10)})
        merged = full.merge(grp[["digito", "contagem"]], on="digito", how="left").fillna(0)
        observed = merged["contagem"].values.astype(float)
        total = observed.sum()

        if total == 0:
            continue

        expected = np.array([BENFORD_EXPECTED[d] * total for d in range(1, 10)])

        # Evitar expected zeros
        mask = expected > 0
        if mask.sum() == 0:
            continue

        chi2_stat, p_val = stats.chisquare(observed[mask], f_exp=expected[mask])

        results.append({
            "candidato": candidato,
            "chi2": round(float(chi2_stat), 4),
            "p_value": round(float(p_val), 6),
            "conforme": bool(p_val > 0.05),
        })

    if not results:
        return _empty_df(cols)

    return pd.DataFrame(results)[cols]


def benford_by_state(store, f) -> pd.DataFrame:
    """Conformidade Benford por estado para Presidente (candidatos 13 e 22).

    Returns:
        DataFrame com: uf, chi2_13, chi2_22, conforme_13, conforme_22
    """
    cols = ["uf", "chi2_13", "chi2_22", "conforme_13", "conforme_22"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            v.codigo_candidato AS candidato,
            v.quantidade
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.quantidade > 0
            AND v.codigo_candidato IN (13, 22)
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["digito"] = df["quantidade"].astype(str).str[0].astype(int)
    df = df[df["digito"].between(1, 9)]

    if df.empty:
        return _empty_df(cols)

    results = []
    for uf, uf_grp in df.groupby("uf"):
        row = {"uf": uf}
        for cand_id, suffix in [(13, "13"), (22, "22")]:
            cand_data = uf_grp[uf_grp["candidato"] == cand_id]
            if cand_data.empty:
                row[f"chi2_{suffix}"] = None
                row[f"conforme_{suffix}"] = None
                continue

            digit_counts = cand_data.groupby("digito").size()
            observed = np.array([digit_counts.get(d, 0) for d in range(1, 10)], dtype=float)
            total = observed.sum()

            if total < 10:  # Amostra muito pequena
                row[f"chi2_{suffix}"] = None
                row[f"conforme_{suffix}"] = None
                continue

            expected = np.array([BENFORD_EXPECTED[d] * total for d in range(1, 10)])
            mask = expected > 0
            if mask.sum() == 0:
                row[f"chi2_{suffix}"] = None
                row[f"conforme_{suffix}"] = None
                continue

            chi2_stat, p_val = stats.chisquare(observed[mask], f_exp=expected[mask])
            row[f"chi2_{suffix}"] = round(float(chi2_stat), 4)
            row[f"conforme_{suffix}"] = bool(p_val > 0.05)

        results.append(row)

    if not results:
        return _empty_df(cols)

    return pd.DataFrame(results)[cols].sort_values("uf").reset_index(drop=True)


# ============================================================
# 1b. Lei de Benford — Segundo Dígito
# ============================================================

def benford_second_digit(store, f) -> pd.DataFrame:
    """Distribuição do segundo dígito dos votos por candidato (Presidente).

    A distribuição esperada do 2o dígito de Benford é:
        P(d) = sum_{k=1}^{9} log10(1 + 1/(10k + d)), para d = 0..9

    Votos com apenas 1 dígito são excluídos (não possuem segundo dígito).

    Returns:
        DataFrame com: candidato, digito, quantidade, percentual
    """
    cols = ["candidato", "digito", "quantidade", "percentual"]

    df = store.query_df(f"""
        SELECT
            v.codigo_candidato AS candidato,
            v.quantidade
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.quantidade >= 10
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    # Extrair segundo dígito: posição [1] da representação string
    df["digito"] = df["quantidade"].astype(str).str[1].astype(int)

    if df.empty:
        return _empty_df(cols)

    # Contar por candidato e dígito
    grouped = df.groupby(["candidato", "digito"]).size().reset_index(name="quantidade")

    # Calcular percentual observado por candidato
    totals = grouped.groupby("candidato")["quantidade"].transform("sum")
    grouped["percentual"] = np.where(
        totals > 0,
        (grouped["quantidade"] / totals * 100).round(4),
        0.0,
    )

    return grouped[cols].sort_values(["candidato", "digito"]).reset_index(drop=True)


def benford_second_digit_chi(store, f) -> list:
    """Teste chi-quadrado + KS + Cramer's V por candidato vs Benford 2o dígito.

    Returns:
        Lista de dicts com: candidato, chi2, p_chi2, ks_stat, p_ks, cramers_v, conforme
    """
    digit_df = benford_second_digit(store, f)
    if digit_df.empty:
        return []

    results = []
    for candidato, grp in digit_df.groupby("candidato"):
        # Garantir que todos os dígitos 0-9 estejam presentes
        full = pd.DataFrame({"digito": range(0, 10)})
        merged = full.merge(
            grp[["digito", "quantidade"]], on="digito", how="left"
        ).fillna(0)
        observed = merged["quantidade"].values.astype(float)
        total = observed.sum()

        if total == 0:
            continue

        expected = np.array(
            [BENFORD_SECOND_EXPECTED[d] * total for d in range(0, 10)]
        )

        # Evitar expected zeros
        mask = expected > 0
        if mask.sum() == 0:
            continue

        # Chi-quadrado
        chi2_stat, p_chi2 = stats.chisquare(observed[mask], f_exp=expected[mask])

        # Kolmogorov-Smirnov: comparar distribuições acumuladas empíricas
        obs_freq = observed / total
        exp_freq = np.array([BENFORD_SECOND_EXPECTED[d] for d in range(0, 10)])
        exp_freq = exp_freq / exp_freq.sum()

        obs_cdf = np.cumsum(obs_freq)
        exp_cdf = np.cumsum(exp_freq)

        ks_stat = float(np.max(np.abs(obs_cdf - exp_cdf)))
        sample = np.repeat(np.arange(10), observed.astype(int))
        if len(sample) > 0:
            ks_result = stats.kstest(
                sample,
                lambda x: np.interp(x, np.arange(10), exp_cdf),
            )
            p_ks = float(ks_result.pvalue)
        else:
            p_ks = np.nan

        # Cramer's V
        k = int(mask.sum())
        n = total
        v = _cramers_v(float(chi2_stat), n, k)

        conforme = bool(p_chi2 > 0.05) and (v < 0.10)

        results.append({
            "candidato": candidato,
            "chi2": round(float(chi2_stat), 4),
            "p_chi2": round(float(p_chi2), 6),
            "ks_stat": round(ks_stat, 6),
            "p_ks": round(p_ks, 6) if not np.isnan(p_ks) else np.nan,
            "cramers_v": round(v, 6),
            "conforme": conforme,
        })

    return results


def benford_all_offices(store, f) -> pd.DataFrame:
    """Aplica Benford 1o dígito para TODOS os cargos (não apenas Presidente).

    Returns:
        DataFrame com: cargo, chi2, p_value, cramers_v, conforme
    """
    cols = ["cargo", "chi2", "p_value", "cramers_v", "conforme"]

    df = store.query_df(f"""
        SELECT
            v.cargo,
            v.quantidade
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.tipo_voto = 'nominal'
            AND v.quantidade > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["digito"] = df["quantidade"].astype(str).str[0].astype(int)
    df = df[df["digito"].between(1, 9)]

    if df.empty:
        return _empty_df(cols)

    results = []
    for cargo, grp in df.groupby("cargo"):
        digit_counts = grp.groupby("digito").size()
        observed = np.array(
            [digit_counts.get(d, 0) for d in range(1, 10)], dtype=float
        )
        total = observed.sum()

        if total < 10:
            continue

        expected = np.array([BENFORD_EXPECTED[d] * total for d in range(1, 10)])
        mask = expected > 0
        if mask.sum() == 0:
            continue

        chi2_stat, p_val = stats.chisquare(observed[mask], f_exp=expected[mask])

        k = int(mask.sum())
        v = _cramers_v(float(chi2_stat), total, k)

        results.append({
            "cargo": cargo,
            "chi2": round(float(chi2_stat), 4),
            "p_value": round(float(p_val), 6),
            "cramers_v": round(v, 6),
            "conforme": bool(p_val > 0.05),
        })

    if not results:
        return _empty_df(cols)

    return pd.DataFrame(results)[cols].sort_values("cargo").reset_index(drop=True)


# ============================================================
# 2. Detecção de Outliers
# ============================================================

def zscore_outliers(store, f, threshold=3.0) -> pd.DataFrame:
    """Seções com z-score > threshold em abstenção, biometria ou duração.

    Returns:
        DataFrame com: id, uf, municipio, zona, secao, metric, value, zscore
    """
    cols = ["id", "uf", "municipio", "zona", "secao", "metric", "value", "zscore"]

    metrics = [
        ("pct_abstencao", "abstencao"),
        ("pct_biometria", "biometria"),
        ("duracao_min", "duracao"),
    ]

    all_outliers = []
    for col_name, metric_label in metrics:
        df = store.query_df(f"""
            SELECT
                s.id, s.uf, s.municipio, s.zona, s.secao,
                s.{col_name} AS value,
                (s.{col_name} - AVG(s.{col_name}) OVER())
                    / NULLIF(STDDEV(s.{col_name}) OVER(), 0) AS zscore
            FROM secoes s
            WHERE {f['where']} AND s.{col_name} IS NOT NULL
        """, f["params"])

        if df.empty:
            continue

        outliers = df[df["zscore"].abs() > threshold].copy()
        if not outliers.empty:
            outliers["metric"] = metric_label
            outliers["zscore"] = outliers["zscore"].round(4)
            outliers["value"] = outliers["value"].round(4)
            all_outliers.append(outliers[cols])

    if not all_outliers:
        return _empty_df(cols)

    return pd.concat(all_outliers, ignore_index=True).sort_values(
        "zscore", ascending=False, key=abs
    ).reset_index(drop=True)


def outlier_summary(store, f) -> pd.DataFrame:
    """Resumo de outliers por estado e métrica.

    Returns:
        DataFrame com: uf, outliers_abstencao, outliers_biometria, outliers_duracao, total
    """
    cols = ["uf", "outliers_abstencao", "outliers_biometria", "outliers_duracao", "total"]

    outliers_df = zscore_outliers(store, f, threshold=3.0)
    if outliers_df.empty:
        return _empty_df(cols)

    pivot = outliers_df.groupby(["uf", "metric"]).size().unstack(fill_value=0)

    result = pd.DataFrame({"uf": pivot.index})
    result["outliers_abstencao"] = pivot.get("abstencao", pd.Series(0, index=pivot.index)).values
    result["outliers_biometria"] = pivot.get("biometria", pd.Series(0, index=pivot.index)).values
    result["outliers_duracao"] = pivot.get("duracao", pd.Series(0, index=pivot.index)).values
    result["total"] = result["outliers_abstencao"] + result["outliers_biometria"] + result["outliers_duracao"]

    return result[cols].sort_values("total", ascending=False).reset_index(drop=True)


# ============================================================
# 3. Análise de Impacto de Reboots
# ============================================================

def reboot_vote_distribution(store, f) -> pd.DataFrame:
    """Distribuição de votos para Presidente em seções com/sem reboot.

    Returns:
        DataFrame com: grupo, candidato, votos, pct
    """
    cols = ["grupo", "candidato", "votos", "pct"]

    df = store.query_df(f"""
        SELECT
            CASE WHEN s.reboots > 0 THEN 'Com Reboot' ELSE 'Sem Reboot' END AS grupo,
            v.codigo_candidato AS candidato,
            SUM(v.quantidade) AS votos
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY grupo, v.codigo_candidato
        ORDER BY grupo, votos DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    # Calcular percentual por grupo
    totals_by_group = df.groupby("grupo")["votos"].transform("sum")
    df["pct"] = np.where(
        totals_by_group > 0,
        (df["votos"] / totals_by_group * 100).round(2),
        0.0,
    )

    return df[cols].reset_index(drop=True)


def reboot_by_state(store, f) -> pd.DataFrame:
    """Taxa de reboot e impacto por estado.

    Returns:
        DataFrame com: uf, secoes, secoes_reboot, pct_reboot,
                       pct_lula_reboot, pct_lula_normal, diff_pp
    """
    cols = [
        "uf", "secoes", "secoes_reboot", "pct_reboot",
        "pct_lula_reboot", "pct_lula_normal", "diff_pp",
    ]

    # Seções por estado com/sem reboot
    secoes_df = store.query_df(f"""
        SELECT
            s.uf,
            COUNT(*) AS secoes,
            SUM(CASE WHEN s.reboots > 0 THEN 1 ELSE 0 END) AS secoes_reboot,
            ROUND(SUM(CASE WHEN s.reboots > 0 THEN 1.0 ELSE 0 END)
                  / NULLIF(COUNT(*), 0) * 100, 2) AS pct_reboot
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf
    """, f["params"])

    if secoes_df.empty:
        return _empty_df(cols)

    # Votos Lula por grupo reboot/normal por UF
    votos_df = store.query_df(f"""
        SELECT
            s.uf,
            CASE WHEN s.reboots > 0 THEN 'reboot' ELSE 'normal' END AS grupo,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.uf, grupo
    """, f["params"])

    if votos_df.empty:
        secoes_df["pct_lula_reboot"] = np.nan
        secoes_df["pct_lula_normal"] = np.nan
        secoes_df["diff_pp"] = np.nan
        return secoes_df[cols].sort_values("uf").reset_index(drop=True)

    votos_df["pct_lula"] = np.where(
        votos_df["total"] > 0,
        (votos_df["lula"] / votos_df["total"] * 100).round(2),
        np.nan,
    )

    pivot = votos_df.pivot_table(
        index="uf", columns="grupo", values="pct_lula", aggfunc="first"
    )

    result = secoes_df.merge(
        pivot.rename(columns={"reboot": "pct_lula_reboot", "normal": "pct_lula_normal"}),
        on="uf", how="left",
    )

    result["diff_pp"] = np.where(
        result["pct_lula_reboot"].notna() & result["pct_lula_normal"].notna(),
        (result["pct_lula_reboot"] - result["pct_lula_normal"]).round(2),
        np.nan,
    )

    return result[cols].sort_values("uf").reset_index(drop=True)


def reboot_candidate_correlation(store, f) -> dict:
    """Teste chi-quadrado: reboots favorecem algum candidato?

    Returns:
        dict com chi2, p_value, conclusion
    """
    df = reboot_vote_distribution(store, f)
    if df.empty or len(df) < 4:
        return {
            "chi2": None,
            "p_value": None,
            "conclusion": "Dados insuficientes para teste estatístico.",
        }

    # Montar tabela de contingência: grupo x candidato
    try:
        pivot = df.pivot_table(
            index="grupo", columns="candidato", values="votos", fill_value=0
        )

        if pivot.shape[0] < 2 or pivot.shape[1] < 2:
            return {
                "chi2": None,
                "p_value": None,
                "conclusion": "Dados insuficientes para tabela de contingência.",
            }

        chi2_stat, p_val, dof, expected = stats.chi2_contingency(pivot.values)
        chi2_stat = round(float(chi2_stat), 4)
        p_val = round(float(p_val), 6)

        if p_val > 0.05:
            conclusion = (
                f"NÃO há evidência estatística de que reboots favorecem um candidato "
                f"(chi2={chi2_stat}, p={p_val}, p > 0.05). "
                f"A distribuição de votos em seções com e sem reboot é estatisticamente similar."
            )
        else:
            conclusion = (
                f"Há diferença estatisticamente significativa na distribuição de votos "
                f"entre seções com e sem reboot (chi2={chi2_stat}, p={p_val}, p <= 0.05). "
                f"Isso pode refletir distribuição geográfica e não necessariamente causalidade."
            )

        return {"chi2": chi2_stat, "p_value": p_val, "conclusion": conclusion}

    except Exception as e:
        return {
            "chi2": None,
            "p_value": None,
            "conclusion": f"Erro no teste estatístico: {e}",
        }


def reboot_patterns(store, f) -> pd.DataFrame:
    """Padrões de reboot por modelo, UF.

    Returns:
        DataFrame com: modelo, uf, total_reboots, secoes_com_reboot, media_reboots
    """
    cols = ["modelo", "uf", "total_reboots", "secoes_com_reboot", "media_reboots"]

    df = store.query_df(f"""
        SELECT
            s.modelo_urna AS modelo,
            s.uf,
            SUM(s.reboots) AS total_reboots,
            SUM(CASE WHEN s.reboots > 0 THEN 1 ELSE 0 END) AS secoes_com_reboot,
            ROUND(AVG(CASE WHEN s.reboots > 0 THEN s.reboots ELSE NULL END), 2)
                AS media_reboots
        FROM secoes s
        WHERE {f['where']} AND s.reboots > 0
        GROUP BY s.modelo_urna, s.uf
        ORDER BY total_reboots DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


# ============================================================
# 4. Consistência Entre Turnos
# ============================================================

def _build_cross_turno_base_where(f):
    """Remove filtro de turno do WHERE para comparar ambos os turnos.

    Retorna (where_clause, params) sem restrição de turno.
    """
    base_where = f["where"]
    base_params = list(f["params"])

    if f.get("turno") is not None:
        # Remover "s.turno = ? AND " ou "s.turno = ?" isolado
        base_where = base_where.replace("s.turno = ? AND ", "")
        base_where = base_where.replace("s.turno = ?", "1=1")
        # Remover o parâmetro de turno
        turno_val = f["turno"]
        if turno_val in base_params:
            base_params.remove(turno_val)

    return base_where, base_params


def cross_turno_attendance(store, f) -> pd.DataFrame:
    """Variação de comparecimento entre T1 e T2 para mesmas seções.

    O pareamento usa uf/municipio/zona/secao (sem prefixo de turno).

    Returns:
        DataFrame com: uf, secoes_pareadas, comp_t1, comp_t2, variacao_pct
    """
    cols = ["uf", "secoes_pareadas", "comp_t1", "comp_t2", "variacao_pct"]

    base_where, base_params = _build_cross_turno_base_where(f)

    df = store.query_df(f"""
        SELECT
            t1.uf,
            COUNT(*) AS secoes_pareadas,
            SUM(t1.comparecimento) AS comp_t1,
            SUM(t2.comparecimento) AS comp_t2,
            ROUND(
                (SUM(t2.comparecimento) - SUM(t1.comparecimento)) * 100.0
                / NULLIF(SUM(t1.comparecimento), 0),
                2
            ) AS variacao_pct
        FROM secoes t1
        JOIN secoes t2
            ON t1.uf = t2.uf
            AND t1.municipio = t2.municipio
            AND t1.zona = t2.zona
            AND t1.secao = t2.secao
            AND t1.turno = 1
            AND t2.turno = 2
        WHERE {base_where.replace('s.', 't1.')}
        GROUP BY t1.uf
        ORDER BY t1.uf
    """, base_params)

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def cross_turno_missing(store, f) -> pd.DataFrame:
    """Seções presentes em apenas um turno.

    Returns:
        DataFrame com: turno_presente, uf, count
    """
    cols = ["turno_presente", "uf", "count"]

    base_where, base_params = _build_cross_turno_base_where(f)

    # Seções no T1 sem T2
    only_t1 = store.query_df(f"""
        SELECT
            1 AS turno_presente,
            t1.uf,
            COUNT(*) AS count
        FROM secoes t1
        LEFT JOIN secoes t2
            ON t1.uf = t2.uf
            AND t1.municipio = t2.municipio
            AND t1.zona = t2.zona
            AND t1.secao = t2.secao
            AND t2.turno = 2
        WHERE t1.turno = 1
            AND t2.id IS NULL
            AND {base_where.replace('s.', 't1.')}
        GROUP BY t1.uf
    """, base_params)

    # Seções no T2 sem T1
    only_t2 = store.query_df(f"""
        SELECT
            2 AS turno_presente,
            t2.uf,
            COUNT(*) AS count
        FROM secoes t2
        LEFT JOIN secoes t1
            ON t1.uf = t2.uf
            AND t1.municipio = t2.municipio
            AND t1.zona = t2.zona
            AND t1.secao = t2.secao
            AND t1.turno = 1
        WHERE t2.turno = 2
            AND t1.id IS NULL
            AND {base_where.replace('s.', 't2.')}
        GROUP BY t2.uf
    """, base_params)

    parts = [df for df in [only_t1, only_t2] if not df.empty]
    if not parts:
        return _empty_df(cols)

    result = pd.concat(parts, ignore_index=True)
    return result[cols].sort_values(["turno_presente", "uf"]).reset_index(drop=True)


def cross_turno_vote_migration(store, f) -> pd.DataFrame:
    """Migração de votos entre T1 e T2 para Presidente.

    Returns:
        DataFrame com: uf, lula_t1_pct, lula_t2_pct, bolso_t1_pct, bolso_t2_pct, swing_lula
    """
    cols = ["uf", "lula_t1_pct", "lula_t2_pct", "bolso_t1_pct", "bolso_t2_pct", "swing_lula"]

    base_where, base_params = _build_cross_turno_base_where(f)

    df = store.query_df(f"""
        WITH votos_turno AS (
            SELECT
                s.uf,
                s.turno,
                SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
                SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
                SUM(v.quantidade) AS total
            FROM votos v
            JOIN secoes s ON v.secao_id = s.id
            WHERE {base_where}
                AND v.cargo = 'Presidente'
                AND v.tipo_voto = 'nominal'
                AND v.codigo_candidato IN (13, 22)
            GROUP BY s.uf, s.turno
        )
        SELECT
            t1.uf,
            ROUND(t1.lula * 100.0 / NULLIF(t1.total, 0), 2) AS lula_t1_pct,
            ROUND(t2.lula * 100.0 / NULLIF(t2.total, 0), 2) AS lula_t2_pct,
            ROUND(t1.bolso * 100.0 / NULLIF(t1.total, 0), 2) AS bolso_t1_pct,
            ROUND(t2.bolso * 100.0 / NULLIF(t2.total, 0), 2) AS bolso_t2_pct,
            ROUND(
                (t2.lula * 100.0 / NULLIF(t2.total, 0))
                - (t1.lula * 100.0 / NULLIF(t1.total, 0)),
                2
            ) AS swing_lula
        FROM votos_turno t1
        JOIN votos_turno t2 ON t1.uf = t2.uf AND t1.turno = 1 AND t2.turno = 2
        ORDER BY t1.uf
    """, base_params)

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


# ============================================================
# 5. Anomalias Geográficas
# ============================================================

def issue_density_by_state(store, f) -> pd.DataFrame:
    """Densidade de issues (issues/seção) por estado, normalizada com z-score.

    Returns:
        DataFrame com: uf, secoes, issues, density, z_density
    """
    cols = ["uf", "secoes", "issues", "density", "z_density"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            COUNT(DISTINCT s.id) AS secoes,
            COUNT(i.id) AS issues,
            ROUND(
                COUNT(i.id) * 1.0 / NULLIF(COUNT(DISTINCT s.id), 0),
                4
            ) AS density
        FROM secoes s
        LEFT JOIN issues i ON i.secao_id = s.id
        WHERE {f['where']}
        GROUP BY s.uf
        ORDER BY s.uf
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    # Calcular z-score da densidade
    mean_d = df["density"].mean()
    std_d = df["density"].std()
    df["z_density"] = np.where(
        std_d > 0,
        ((df["density"] - mean_d) / std_d).round(4),
        0.0,
    )

    return df[cols].reset_index(drop=True)


def geographic_issue_vs_result(store, f) -> pd.DataFrame:
    """Correlação entre densidade de issues e resultado eleitoral por estado.

    Returns:
        DataFrame com: uf, density, pct_lula, pct_bolso
    """
    cols = ["uf", "density", "pct_lula", "pct_bolso"]

    density_df = issue_density_by_state(store, f)
    if density_df.empty:
        return _empty_df(cols)

    votos_df = store.query_df(f"""
        SELECT
            s.uf,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.uf
    """, f["params"])

    if votos_df.empty:
        density_df["pct_lula"] = np.nan
        density_df["pct_bolso"] = np.nan
        return density_df[["uf", "density", "pct_lula", "pct_bolso"]].reset_index(drop=True)

    votos_df["pct_lula"] = np.where(
        votos_df["total"] > 0,
        (votos_df["lula"] / votos_df["total"] * 100).round(2),
        np.nan,
    )
    votos_df["pct_bolso"] = np.where(
        votos_df["total"] > 0,
        (votos_df["bolso"] / votos_df["total"] * 100).round(2),
        np.nan,
    )

    result = density_df[["uf", "density"]].merge(
        votos_df[["uf", "pct_lula", "pct_bolso"]],
        on="uf", how="left",
    )

    return result[cols].sort_values("uf").reset_index(drop=True)


# ============================================================
# 6. Análise de Divergência A03
# ============================================================

def a03_by_turno(store, f) -> pd.DataFrame:
    """Issues A03 (divergência votos log vs BU) por turno.

    Returns:
        DataFrame com: turno, count, pct_of_secoes
    """
    cols = ["turno", "count", "pct_of_secoes"]

    # Build a WHERE clause for the subquery that counts total secoes per turno,
    # applying the same filters (with s2. alias instead of s.)
    sub_where = f['where'].replace('s.', 's2.')

    df = store.query_df(f"""
        SELECT
            s.turno,
            COUNT(i.id) AS count,
            ROUND(
                COUNT(i.id) * 100.0
                / NULLIF((
                    SELECT COUNT(*)
                    FROM secoes s2
                    WHERE s2.turno = s.turno AND {sub_where}
                ), 0),
                4
            ) AS pct_of_secoes
        FROM issues i
        JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']} AND i.codigo = 'A03'
        GROUP BY s.turno
        ORDER BY s.turno
    """, f["params"] + f["issue_params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def a03_detail(store, f) -> pd.DataFrame:
    """Detalhamento das divergências A03.

    Returns:
        DataFrame com: id, uf, municipio, zona, secao, turno, votos_log,
                       comparecimento, ratio
    """
    cols = ["id", "uf", "municipio", "zona", "secao", "turno",
            "votos_log", "comparecimento", "ratio"]

    df = store.query_df(f"""
        SELECT
            s.id,
            s.uf,
            s.municipio,
            s.zona,
            s.secao,
            s.turno,
            s.votos_log,
            s.comparecimento,
            ROUND(
                s.votos_log * 1.0 / NULLIF(s.comparecimento, 0),
                4
            ) AS ratio
        FROM secoes s
        JOIN issues i ON i.secao_id = s.id
        WHERE {f['issue_where']} AND i.codigo = 'A03'
        ORDER BY ratio DESC
    """, f["issue_params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


# ============================================================
# 7. Biometria e Segurança
# ============================================================

def zero_biometry_sections(store, f) -> pd.DataFrame:
    """Seções com 0% de verificação biométrica.

    Returns:
        DataFrame com: id, uf, municipio, zona, secao, modelo, comparecimento, lib_codigo
    """
    cols = ["id", "uf", "municipio", "zona", "secao", "modelo",
            "comparecimento", "lib_codigo"]

    df = store.query_df(f"""
        SELECT
            s.id,
            s.uf,
            s.municipio,
            s.zona,
            s.secao,
            s.modelo_urna AS modelo,
            s.comparecimento,
            s.lib_codigo
        FROM secoes s
        WHERE {f['where']}
            AND (s.pct_biometria IS NULL OR s.pct_biometria = 0)
            AND s.comparecimento > 0
        ORDER BY s.comparecimento DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def biometry_vs_vote(store, f) -> pd.DataFrame:
    """Correlação entre taxa de biometria e padrão de voto por quartil.

    Returns:
        DataFrame com: quartil, secoes, pct_bio_media, pct_lula, pct_bolso
    """
    cols = ["quartil", "secoes", "pct_bio_media", "pct_lula", "pct_bolso"]

    df = store.query_df(f"""
        SELECT
            s.id,
            s.pct_biometria
        FROM secoes s
        WHERE {f['where']} AND s.pct_biometria IS NOT NULL
    """, f["params"])

    if df.empty or len(df) < 4:
        return _empty_df(cols)

    # Calcular quartis
    try:
        df["quartil"] = pd.qcut(
            df["pct_biometria"], q=4, labels=["Q1 (baixa)", "Q2", "Q3", "Q4 (alta)"],
            duplicates="drop",
        )
    except ValueError:
        # Se não há variação suficiente para 4 quartis
        return _empty_df(cols)

    secao_quartil = df[["id", "quartil", "pct_biometria"]]

    # Buscar votos para Presidente
    votos_df = store.query_df(f"""
        SELECT
            v.secao_id,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY v.secao_id
    """, f["params"])

    if votos_df.empty:
        return _empty_df(cols)

    merged = secao_quartil.merge(votos_df, left_on="id", right_on="secao_id", how="inner")

    if merged.empty:
        return _empty_df(cols)

    result = merged.groupby("quartil", observed=True).agg(
        secoes=("id", "count"),
        pct_bio_media=("pct_biometria", "mean"),
        lula=("lula", "sum"),
        bolso=("bolso", "sum"),
        total=("total", "sum"),
    ).reset_index()

    result["pct_bio_media"] = result["pct_bio_media"].round(2)
    result["pct_lula"] = np.where(
        result["total"] > 0,
        (result["lula"] / result["total"] * 100).round(2),
        0.0,
    )
    result["pct_bolso"] = np.where(
        result["total"] > 0,
        (result["bolso"] / result["total"] * 100).round(2),
        0.0,
    )

    return result[cols].reset_index(drop=True)


# ============================================================
# 8. Análise de Tempo
# ============================================================

def duration_distribution(store, f) -> pd.DataFrame:
    """Distribuição de duração de votação por estado.

    Returns:
        DataFrame com: uf, media_min, mediana_min, min_min, max_min, desvio_padrao
    """
    cols = ["uf", "media_min", "mediana_min", "min_min", "max_min", "desvio_padrao"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            ROUND(AVG(s.duracao_min), 2) AS media_min,
            ROUND(MEDIAN(s.duracao_min), 2) AS mediana_min,
            MIN(s.duracao_min) AS min_min,
            MAX(s.duracao_min) AS max_min,
            ROUND(STDDEV(s.duracao_min), 2) AS desvio_padrao
        FROM secoes s
        WHERE {f['where']} AND s.duracao_min IS NOT NULL
        GROUP BY s.uf
        ORDER BY s.uf
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def abnormal_duration_sections(store, f) -> pd.DataFrame:
    """Seções com duração de votação anormal (z-score > 3).

    Returns:
        DataFrame com: id, uf, municipio, zona, secao, duracao_min, zscore
    """
    cols = ["id", "uf", "municipio", "zona", "secao", "duracao_min", "zscore"]

    df = store.query_df(f"""
        SELECT
            s.id,
            s.uf,
            s.municipio,
            s.zona,
            s.secao,
            s.duracao_min,
            (s.duracao_min - AVG(s.duracao_min) OVER())
                / NULLIF(STDDEV(s.duracao_min) OVER(), 0) AS zscore
        FROM secoes s
        WHERE {f['where']} AND s.duracao_min IS NOT NULL
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df = df[df["zscore"].abs() > 3.0].copy()
    if df.empty:
        return _empty_df(cols)

    df["zscore"] = df["zscore"].round(4)

    return df[cols].sort_values("zscore", ascending=False, key=abs).reset_index(drop=True)


def duration_vs_vote(store, f) -> pd.DataFrame:
    """Correlação entre duração de votação e padrão de voto por quartil.

    Returns:
        DataFrame com: quartil, secoes, duracao_media, pct_lula, pct_bolso
    """
    cols = ["quartil", "secoes", "duracao_media", "pct_lula", "pct_bolso"]

    df = store.query_df(f"""
        SELECT
            s.id,
            s.duracao_min
        FROM secoes s
        WHERE {f['where']} AND s.duracao_min IS NOT NULL
    """, f["params"])

    if df.empty or len(df) < 4:
        return _empty_df(cols)

    try:
        df["quartil"] = pd.qcut(
            df["duracao_min"], q=4, labels=["Q1 (curta)", "Q2", "Q3", "Q4 (longa)"],
            duplicates="drop",
        )
    except ValueError:
        return _empty_df(cols)

    secao_quartil = df[["id", "quartil", "duracao_min"]]

    votos_df = store.query_df(f"""
        SELECT
            v.secao_id,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY v.secao_id
    """, f["params"])

    if votos_df.empty:
        return _empty_df(cols)

    merged = secao_quartil.merge(votos_df, left_on="id", right_on="secao_id", how="inner")

    if merged.empty:
        return _empty_df(cols)

    result = merged.groupby("quartil", observed=True).agg(
        secoes=("id", "count"),
        duracao_media=("duracao_min", "mean"),
        lula=("lula", "sum"),
        bolso=("bolso", "sum"),
        total=("total", "sum"),
    ).reset_index()

    result["duracao_media"] = result["duracao_media"].round(2)
    result["pct_lula"] = np.where(
        result["total"] > 0,
        (result["lula"] / result["total"] * 100).round(2),
        0.0,
    )
    result["pct_bolso"] = np.where(
        result["total"] > 0,
        (result["bolso"] / result["total"] * 100).round(2),
        0.0,
    )

    return result[cols].reset_index(drop=True)


# ============================================================
# 9. Análise de Hardware/Modelo
# ============================================================

def model_issue_rate(store, f) -> pd.DataFrame:
    """Taxa de issues por modelo de urna, normalizada por seção.

    Returns:
        DataFrame com: modelo, secoes, issues, rate, rate_critica, rate_alta
    """
    cols = ["modelo", "secoes", "issues", "rate", "rate_critica", "rate_alta"]

    df = store.query_df(f"""
        SELECT
            s.modelo_urna AS modelo,
            COUNT(DISTINCT s.id) AS secoes,
            COUNT(i.id) AS issues,
            ROUND(
                COUNT(i.id) * 1.0 / NULLIF(COUNT(DISTINCT s.id), 0),
                4
            ) AS rate,
            ROUND(
                SUM(CASE WHEN i.severidade = 'critica' THEN 1 ELSE 0 END) * 1.0
                / NULLIF(COUNT(DISTINCT s.id), 0),
                4
            ) AS rate_critica,
            ROUND(
                SUM(CASE WHEN i.severidade = 'alta' THEN 1 ELSE 0 END) * 1.0
                / NULLIF(COUNT(DISTINCT s.id), 0),
                4
            ) AS rate_alta
        FROM secoes s
        LEFT JOIN issues i ON i.secao_id = s.id
        WHERE {f['where']} AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna
        ORDER BY rate DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def model_vote_pattern(store, f) -> pd.DataFrame:
    """Padrão de voto por modelo de urna para Presidente.

    Returns:
        DataFrame com: modelo, secoes, pct_lula, pct_bolso, pct_brancos, pct_nulos
    """
    cols = ["modelo", "secoes", "pct_lula", "pct_bolso", "pct_brancos", "pct_nulos"]

    df = store.query_df(f"""
        SELECT
            s.modelo_urna AS modelo,
            COUNT(DISTINCT s.id) AS secoes,
            SUM(CASE WHEN v.codigo_candidato = 13 AND v.tipo_voto = 'nominal'
                THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 AND v.tipo_voto = 'nominal'
                THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(CASE WHEN v.tipo_voto = 'branco'
                THEN v.quantidade ELSE 0 END) AS brancos,
            SUM(CASE WHEN v.tipo_voto = 'nulo'
                THEN v.quantidade ELSE 0 END) AS nulos,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna
        ORDER BY s.modelo_urna
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = np.where(
        df["total"] > 0, (df["lula"] / df["total"] * 100).round(2), 0.0
    )
    df["pct_bolso"] = np.where(
        df["total"] > 0, (df["bolso"] / df["total"] * 100).round(2), 0.0
    )
    df["pct_brancos"] = np.where(
        df["total"] > 0, (df["brancos"] / df["total"] * 100).round(2), 0.0
    )
    df["pct_nulos"] = np.where(
        df["total"] > 0, (df["nulos"] / df["total"] * 100).round(2), 0.0
    )

    return df[cols].reset_index(drop=True)


# ============================================================
# 10. Nulos e Brancos
# ============================================================

def null_blank_by_section(store, f) -> pd.DataFrame:
    """Seções outliers em percentual de nulos ou brancos por cargo.

    Returns:
        DataFrame com: secao_id, uf, cargo, comparecimento, nulos, brancos,
                       pct_nulos, pct_brancos, zscore_nulos, zscore_brancos
    """
    cols = [
        "secao_id", "uf", "cargo", "comparecimento", "nulos", "brancos",
        "pct_nulos", "pct_brancos", "zscore_nulos", "zscore_brancos",
    ]

    df = store.query_df(f"""
        SELECT
            t.secao_id,
            s.uf,
            t.cargo,
            t.comparecimento,
            t.nulos,
            t.brancos,
            ROUND(t.nulos * 100.0 / NULLIF(t.total, 0), 4) AS pct_nulos,
            ROUND(t.brancos * 100.0 / NULLIF(t.total, 0), 4) AS pct_brancos
        FROM totais_cargo t
        JOIN secoes s ON t.secao_id = s.id
        WHERE {f['where']} AND t.total > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    all_outliers = []
    for cargo, grp in df.groupby("cargo"):
        mean_nulos = grp["pct_nulos"].mean()
        std_nulos = grp["pct_nulos"].std()
        mean_brancos = grp["pct_brancos"].mean()
        std_brancos = grp["pct_brancos"].std()

        grp = grp.copy()
        grp["zscore_nulos"] = np.where(
            std_nulos > 0,
            ((grp["pct_nulos"] - mean_nulos) / std_nulos).round(4),
            0.0,
        )
        grp["zscore_brancos"] = np.where(
            std_brancos > 0,
            ((grp["pct_brancos"] - mean_brancos) / std_brancos).round(4),
            0.0,
        )

        mask = (grp["zscore_nulos"].abs() > 3) | (grp["zscore_brancos"].abs() > 3)
        outliers = grp[mask]
        if not outliers.empty:
            all_outliers.append(outliers[cols])

    if not all_outliers:
        return _empty_df(cols)

    result = pd.concat(all_outliers, ignore_index=True)
    result["_max_zscore"] = result[["zscore_nulos", "zscore_brancos"]].abs().max(axis=1)
    result = result.sort_values("_max_zscore", ascending=False).head(1000)
    result = result.drop(columns=["_max_zscore"])

    return result[cols].reset_index(drop=True)


def null_blank_summary(store, f) -> pd.DataFrame:
    """Resumo por UF: total de seções com outliers de nulos e brancos.

    Returns:
        DataFrame com: uf, outliers_nulos, outliers_brancos, total_secoes
    """
    cols = ["uf", "outliers_nulos", "outliers_brancos", "total_secoes"]

    outliers_df = null_blank_by_section(store, f)

    secoes_df = store.query_df(f"""
        SELECT s.uf, COUNT(*) AS total_secoes
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf
    """, f["params"])

    if secoes_df.empty:
        return _empty_df(cols)

    if outliers_df.empty:
        secoes_df["outliers_nulos"] = 0
        secoes_df["outliers_brancos"] = 0
        return secoes_df[cols].sort_values("uf").reset_index(drop=True)

    nulos_outliers = outliers_df[outliers_df["zscore_nulos"].abs() > 3]
    nulos_by_uf = nulos_outliers.groupby("uf")["secao_id"].nunique().reset_index()
    nulos_by_uf.columns = ["uf", "outliers_nulos"]

    brancos_outliers = outliers_df[outliers_df["zscore_brancos"].abs() > 3]
    brancos_by_uf = brancos_outliers.groupby("uf")["secao_id"].nunique().reset_index()
    brancos_by_uf.columns = ["uf", "outliers_brancos"]

    result = secoes_df.merge(nulos_by_uf, on="uf", how="left")
    result = result.merge(brancos_by_uf, on="uf", how="left")
    result["outliers_nulos"] = result["outliers_nulos"].fillna(0).astype(int)
    result["outliers_brancos"] = result["outliers_brancos"].fillna(0).astype(int)

    return result[cols].sort_values("uf").reset_index(drop=True)


def null_blank_vs_result(store, f) -> pd.DataFrame:
    """Divide seções em quartis de pct_nulos para Presidente.

    Returns:
        DataFrame com: quartil, faixa, secoes, pct_lula, pct_bolso
    """
    cols = ["quartil", "faixa", "secoes", "pct_lula", "pct_bolso"]

    df = store.query_df(f"""
        SELECT
            t.secao_id,
            ROUND(t.nulos * 100.0 / NULLIF(t.total, 0), 4) AS pct_nulos
        FROM totais_cargo t
        JOIN secoes s ON t.secao_id = s.id
        WHERE {f['where']}
            AND t.cargo = 'Presidente'
            AND t.total > 0
    """, f["params"])

    if df.empty or len(df) < 4:
        return _empty_df(cols)

    try:
        df["quartil"] = pd.qcut(
            df["pct_nulos"], q=4,
            labels=["Q1 (baixo)", "Q2", "Q3", "Q4 (alto)"],
            duplicates="drop",
        )
    except ValueError:
        return _empty_df(cols)

    quartil_stats = df.groupby("quartil", observed=True)["pct_nulos"].agg(["min", "max"])
    faixas = {
        q: f"{row['min']:.2f}%-{row['max']:.2f}%"
        for q, row in quartil_stats.iterrows()
    }

    secao_quartil = df[["secao_id", "quartil"]]

    votos_df = store.query_df(f"""
        SELECT
            v.secao_id,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY v.secao_id
    """, f["params"])

    if votos_df.empty:
        return _empty_df(cols)

    merged = secao_quartil.merge(votos_df, on="secao_id", how="inner")
    if merged.empty:
        return _empty_df(cols)

    result = merged.groupby("quartil", observed=True).agg(
        secoes=("secao_id", "count"),
        lula=("lula", "sum"),
        bolso=("bolso", "sum"),
        total=("total", "sum"),
    ).reset_index()

    result["faixa"] = result["quartil"].map(faixas).fillna("")
    result["pct_lula"] = np.where(
        result["total"] > 0,
        (result["lula"] / result["total"] * 100).round(2),
        0.0,
    )
    result["pct_bolso"] = np.where(
        result["total"] > 0,
        (result["bolso"] / result["total"] * 100).round(2),
        0.0,
    )

    return result[cols].reset_index(drop=True)


# ============================================================
# 11. Distribuição por Candidato
# ============================================================

def candidate_section_distribution(store, f) -> pd.DataFrame:
    """Seções com z-score > 3 na distribuição de pct_lula por UF.

    Returns:
        DataFrame com: secao_id, uf, municipio, zona, secao,
                       votos_13, votos_22, pct_lula, zscore
    """
    cols = [
        "secao_id", "uf", "municipio", "zona", "secao",
        "votos_13", "votos_22", "pct_lula", "zscore",
    ]

    df = store.query_df(f"""
        SELECT
            s.id AS secao_id,
            s.uf,
            s.municipio,
            s.zona,
            s.secao,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS votos_13,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS votos_22
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.id, s.uf, s.municipio, s.zona, s.secao
        HAVING (SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END)
              + SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END)) > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = (df["votos_13"] / (df["votos_13"] + df["votos_22"]) * 100).round(4)

    all_outliers = []
    for uf, grp in df.groupby("uf"):
        mean_pct = grp["pct_lula"].mean()
        std_pct = grp["pct_lula"].std()

        grp = grp.copy()
        grp["zscore"] = np.where(
            std_pct > 0,
            ((grp["pct_lula"] - mean_pct) / std_pct).round(4),
            0.0,
        )

        outliers = grp[grp["zscore"].abs() > 3]
        if not outliers.empty:
            all_outliers.append(outliers[cols])

    if not all_outliers:
        return _empty_df(cols)

    result = pd.concat(all_outliers, ignore_index=True)
    result = result.sort_values("zscore", ascending=False, key=abs).head(500)

    return result[cols].reset_index(drop=True)


def candidate_extreme_sections(store, f) -> pd.DataFrame:
    """Seções onde um candidato (13 ou 22) tem >= 95% ou <= 5% dos votos válidos.

    Returns:
        DataFrame com: secao_id, uf, municipio, comparecimento,
                       votos_13, votos_22, pct_lula, tipo_extremo
    """
    cols = [
        "secao_id", "uf", "municipio", "comparecimento",
        "votos_13", "votos_22", "pct_lula", "tipo_extremo",
    ]

    df = store.query_df(f"""
        SELECT
            s.id AS secao_id,
            s.uf,
            s.municipio,
            s.comparecimento,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS votos_13,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS votos_22
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.id, s.uf, s.municipio, s.comparecimento
        HAVING (SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END)
              + SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END)) > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    total_validos = df["votos_13"] + df["votos_22"]
    df["pct_lula"] = (df["votos_13"] / total_validos * 100).round(2)

    mask_dom_13 = df["pct_lula"] >= 95
    mask_dom_22 = df["pct_lula"] <= 5

    extremos = df[mask_dom_13 | mask_dom_22].copy()
    if extremos.empty:
        return _empty_df(cols)

    extremos["tipo_extremo"] = np.where(
        extremos["pct_lula"] >= 95,
        "dominancia_13",
        "dominancia_22",
    )

    extremos = extremos.sort_values("pct_lula", ascending=False).head(500)

    return extremos[cols].reset_index(drop=True)


def candidate_distribution_by_state(store, f) -> pd.DataFrame:
    """Estatísticas de pct_lula por UF.

    Returns:
        DataFrame com: uf, media, mediana, std, min, max, secoes
    """
    cols = ["uf", "media", "mediana", "std", "min", "max", "secoes"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS votos_13,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS votos_22
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.id, s.uf
        HAVING (SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END)
              + SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END)) > 0
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = (df["votos_13"] / (df["votos_13"] + df["votos_22"]) * 100).round(4)

    result = df.groupby("uf")["pct_lula"].agg(
        media="mean",
        mediana="median",
        std="std",
        min="min",
        max="max",
        secoes="count",
    ).reset_index()

    result["media"] = result["media"].round(2)
    result["mediana"] = result["mediana"].round(2)
    result["std"] = result["std"].round(2)
    result["min"] = result["min"].round(2)
    result["max"] = result["max"].round(2)

    return result[cols].sort_values("uf").reset_index(drop=True)


# ============================================================
# 12. Mapa Geográfico
# ============================================================

UF_IBGE = {
    'ac': 12, 'al': 27, 'am': 13, 'ap': 16, 'ba': 29,
    'ce': 23, 'df': 53, 'es': 32, 'go': 52, 'ma': 21,
    'mg': 31, 'ms': 50, 'mt': 51, 'pa': 15, 'pb': 25,
    'pe': 26, 'pi': 22, 'pr': 41, 'rj': 33, 'rn': 24,
    'ro': 11, 'rr': 14, 'rs': 43, 'sc': 42, 'se': 28,
    'sp': 35, 'to': 17,
}


def map_state_metrics(store, f) -> pd.DataFrame:
    """Agrega métricas por UF para visualização no mapa.

    Returns:
        DataFrame com: uf, secoes, pct_reboots, pct_issues, pct_abstencao,
                       media_biometria, density_issues, z_density
    """
    cols = [
        "uf", "secoes", "pct_reboots", "pct_issues", "pct_abstencao",
        "media_biometria", "density_issues", "z_density",
    ]

    df = store.query_df(f"""
        SELECT
            s.uf,
            COUNT(*) AS secoes,
            ROUND(
                SUM(CASE WHEN s.reboots > 0 THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS pct_reboots,
            ROUND(
                SUM(CASE WHEN s.has_issues THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS pct_issues,
            ROUND(
                (1 - SUM(s.comparecimento) * 1.0
                 / NULLIF(SUM(s.eleitores_aptos), 0)) * 100, 2
            ) AS pct_abstencao,
            ROUND(AVG(s.pct_biometria), 2) AS media_biometria
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf
        ORDER BY s.uf
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    density_df = store.query_df(f"""
        SELECT
            s.uf,
            COUNT(DISTINCT s.id) AS secoes_d,
            COUNT(i.id) AS issues,
            ROUND(
                COUNT(i.id) * 1.0 / NULLIF(COUNT(DISTINCT s.id), 0), 4
            ) AS density_issues
        FROM secoes s
        LEFT JOIN issues i ON i.secao_id = s.id
        WHERE {f['where']}
        GROUP BY s.uf
    """, f["params"])

    if not density_df.empty:
        df = df.merge(
            density_df[["uf", "density_issues"]],
            on="uf", how="left",
        )
        df["density_issues"] = df["density_issues"].fillna(0.0)
    else:
        df["density_issues"] = 0.0

    mean_d = df["density_issues"].mean()
    std_d = df["density_issues"].std()
    df["z_density"] = np.where(
        std_d > 0,
        ((df["density_issues"] - mean_d) / std_d).round(4),
        0.0,
    )

    return df[cols].reset_index(drop=True)


def map_state_votes(store, f) -> pd.DataFrame:
    """Votos por UF para Presidente (candidatos 13 e 22).

    Returns:
        DataFrame com: uf, votos_13, votos_22, pct_lula, pct_bolso
    """
    cols = ["uf", "votos_13", "votos_22", "pct_lula", "pct_bolso"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS votos_13,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS votos_22,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.uf
        ORDER BY s.uf
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = np.where(
        df["total"] > 0,
        (df["votos_13"] / df["total"] * 100).round(2),
        np.nan,
    )
    df["pct_bolso"] = np.where(
        df["total"] > 0,
        (df["votos_22"] / df["total"] * 100).round(2),
        np.nan,
    )

    return df[cols].reset_index(drop=True)


# ============================================================
# 13. Seções Reserva
# ============================================================

def reserve_vs_normal(store, f) -> pd.DataFrame:
    """Compara métricas entre seções normais e reserva.

    Returns:
        DataFrame com: tipo, secoes, media_reboots, media_erros,
                       media_duracao, pct_issues, media_comparecimento
    """
    cols = [
        "tipo", "secoes", "media_reboots", "media_erros",
        "media_duracao", "pct_issues", "media_comparecimento",
    ]

    df = store.query_df(f"""
        SELECT
            s.tipo_urna AS tipo,
            COUNT(*) AS secoes,
            ROUND(AVG(s.reboots), 4) AS media_reboots,
            ROUND(AVG(s.erros_log), 4) AS media_erros,
            ROUND(AVG(s.duracao_min), 2) AS media_duracao,
            ROUND(
                SUM(CASE WHEN s.has_issues THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS pct_issues,
            ROUND(AVG(s.comparecimento), 2) AS media_comparecimento
        FROM secoes s
        WHERE {f['where']}
            AND s.tipo_urna IS NOT NULL
        GROUP BY s.tipo_urna
        ORDER BY s.tipo_urna
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def reserve_a03_check(store, f) -> dict:
    """Verifica se TODAS as issues A03 são em seções reserva.

    Returns:
        dict com: total_a03, a03_reserva, a03_normal, pct_reserva
    """
    df = store.query_df(f"""
        SELECT
            COUNT(*) AS total_a03,
            SUM(CASE WHEN s.is_reserva THEN 1 ELSE 0 END) AS a03_reserva,
            SUM(CASE WHEN NOT s.is_reserva THEN 1 ELSE 0 END) AS a03_normal
        FROM issues i
        JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
            AND i.codigo = 'A03'
    """, f["issue_params"])

    if df.empty or int(df.iloc[0]["total_a03"]) == 0:
        return {
            "total_a03": 0,
            "a03_reserva": 0,
            "a03_normal": 0,
            "pct_reserva": 0.0,
        }

    row = df.iloc[0]
    total = int(row["total_a03"])
    reserva = int(row["a03_reserva"])
    normal = int(row["a03_normal"])

    return {
        "total_a03": total,
        "a03_reserva": reserva,
        "a03_normal": normal,
        "pct_reserva": round(reserva / total * 100, 2) if total > 0 else 0.0,
    }


def reserve_vote_pattern(store, f) -> pd.DataFrame:
    """Padrão de votação em seções reserva vs normais para Presidente.

    Returns:
        DataFrame com: tipo, secoes, pct_lula, pct_bolso, pct_nulos, pct_brancos
    """
    cols = ["tipo", "secoes", "pct_lula", "pct_bolso", "pct_nulos", "pct_brancos"]

    df = store.query_df(f"""
        SELECT
            s.tipo_urna AS tipo,
            COUNT(DISTINCT s.id) AS secoes,
            SUM(CASE WHEN v.codigo_candidato = 13 AND v.tipo_voto = 'nominal'
                THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 AND v.tipo_voto = 'nominal'
                THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(CASE WHEN v.tipo_voto = 'nulo'
                THEN v.quantidade ELSE 0 END) AS nulos,
            SUM(CASE WHEN v.tipo_voto = 'branco'
                THEN v.quantidade ELSE 0 END) AS brancos,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND s.tipo_urna IS NOT NULL
        GROUP BY s.tipo_urna
        ORDER BY s.tipo_urna
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = np.where(
        df["total"] > 0, (df["lula"] / df["total"] * 100).round(2), np.nan
    )
    df["pct_bolso"] = np.where(
        df["total"] > 0, (df["bolso"] / df["total"] * 100).round(2), np.nan
    )
    df["pct_nulos"] = np.where(
        df["total"] > 0, (df["nulos"] / df["total"] * 100).round(2), np.nan
    )
    df["pct_brancos"] = np.where(
        df["total"] > 0, (df["brancos"] / df["total"] * 100).round(2), np.nan
    )

    return df[cols].reset_index(drop=True)


# ============================================================
# 14. Substituições e Erros
# ============================================================

def substitution_by_state(store, f) -> pd.DataFrame:
    """Distribuição de substituições por estado.

    Returns:
        DataFrame com: uf, secoes_com_substituicao, total_substituicoes,
                       pct_secoes, media_substituicoes
    """
    cols = [
        "uf", "secoes_com_substituicao", "total_substituicoes",
        "pct_secoes", "media_substituicoes",
    ]

    df = store.query_df(f"""
        SELECT
            s.uf,
            SUM(CASE WHEN s.substituicoes > 0 THEN 1 ELSE 0 END)
                AS secoes_com_substituicao,
            SUM(s.substituicoes) AS total_substituicoes,
            ROUND(
                SUM(CASE WHEN s.substituicoes > 0 THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS pct_secoes,
            ROUND(
                AVG(CASE WHEN s.substituicoes > 0
                    THEN s.substituicoes ELSE NULL END), 2
            ) AS media_substituicoes
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf
        HAVING SUM(s.substituicoes) > 0
        ORDER BY total_substituicoes DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


def substitution_vs_result(store, f) -> pd.DataFrame:
    """Comparar votação em seções com substituição vs sem.

    Returns:
        DataFrame com: grupo, secoes, pct_lula, pct_bolso
    """
    cols = ["grupo", "secoes", "pct_lula", "pct_bolso"]

    df = store.query_df(f"""
        SELECT
            CASE WHEN s.substituicoes > 0
                THEN 'com_substituição'
                ELSE 'sem_substituição'
            END AS grupo,
            COUNT(DISTINCT s.id) AS secoes,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) AS lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) AS bolso,
            SUM(v.quantidade) AS total
        FROM votos v
        JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY grupo
        ORDER BY grupo
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_lula"] = np.where(
        df["total"] > 0,
        (df["lula"] / df["total"] * 100).round(2),
        np.nan,
    )
    df["pct_bolso"] = np.where(
        df["total"] > 0,
        (df["bolso"] / df["total"] * 100).round(2),
        np.nan,
    )

    return df[cols].reset_index(drop=True)


def error_log_by_model(store, f) -> pd.DataFrame:
    """Erros de log por modelo de urna.

    Returns:
        DataFrame com: modelo_urna, secoes, media_erros, max_erros,
                       secoes_com_erros, pct_com_erros
    """
    cols = [
        "modelo_urna", "secoes", "media_erros", "max_erros",
        "secoes_com_erros", "pct_com_erros",
    ]

    df = store.query_df(f"""
        SELECT
            s.modelo_urna,
            COUNT(*) AS secoes,
            ROUND(AVG(s.erros_log), 2) AS media_erros,
            MAX(s.erros_log) AS max_erros,
            SUM(CASE WHEN s.erros_log > 0 THEN 1 ELSE 0 END)
                AS secoes_com_erros,
            ROUND(
                SUM(CASE WHEN s.erros_log > 0 THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS pct_com_erros
        FROM secoes s
        WHERE {f['where']}
            AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna
        ORDER BY media_erros DESC
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


# ============================================================
# 15. Integridade e Assinaturas
# ============================================================

def signature_integrity_summary(store, f) -> dict:
    """Resumo de verificação de integridade de hash SHA-512.

    Returns:
        dict com: total_secoes, secoes_hash_ok, secoes_hash_falha, pct_ok
    """
    total_df = store.query_df(f"""
        SELECT COUNT(*) AS total
        FROM secoes s
        WHERE {f['where']}
    """, f["params"])

    total_secoes = int(total_df.iloc[0]["total"]) if not total_df.empty else 0

    if total_secoes == 0:
        return {
            "total_secoes": 0,
            "secoes_hash_ok": 0,
            "secoes_hash_falha": 0,
            "pct_ok": 0.0,
        }

    falha_df = store.query_df(f"""
        SELECT COUNT(DISTINCT i.secao_id) AS falhas
        FROM issues i
        JOIN secoes s ON i.secao_id = s.id
        WHERE {f['where']} AND i.codigo = 'C01'
    """, f["params"])

    secoes_hash_falha = int(falha_df.iloc[0]["falhas"]) if not falha_df.empty else 0
    secoes_hash_ok = total_secoes - secoes_hash_falha

    pct_ok = round(secoes_hash_ok / total_secoes * 100, 2) if total_secoes > 0 else 0.0

    return {
        "total_secoes": total_secoes,
        "secoes_hash_ok": secoes_hash_ok,
        "secoes_hash_falha": secoes_hash_falha,
        "pct_ok": pct_ok,
    }


def signature_detail(store, f) -> pd.DataFrame:
    """Lista seções com issue C01 (falha de hash SHA-512).

    Returns:
        DataFrame com: secao_id, uf, municipio, zona, secao, modelo_urna, detalhes
    """
    cols = ["secao_id", "uf", "municipio", "zona", "secao", "modelo_urna", "detalhes"]

    df = store.query_df(f"""
        SELECT
            s.id AS secao_id,
            s.uf,
            s.municipio,
            s.zona,
            s.secao,
            s.modelo_urna,
            i.detalhes
        FROM issues i
        JOIN secoes s ON i.secao_id = s.id
        WHERE {f['where']} AND i.codigo = 'C01'
        ORDER BY s.uf, s.municipio, s.zona, s.secao
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    return df[cols].reset_index(drop=True)


# ============================================================
# 16. Drill-down por Seção
# ============================================================

def section_detail(store, secao_id) -> dict:
    """Retorna TODOS os dados de uma seção específica.

    Returns:
        dict com:
        - info: dict com dados da seção
        - votos: DataFrame de votos dessa seção
        - issues: DataFrame de issues dessa seção
        - totais: DataFrame de totais_cargo dessa seção
    """
    info_df = store.query_df("""
        SELECT *
        FROM secoes
        WHERE id = ?
    """, [secao_id])

    info = info_df.iloc[0].to_dict() if not info_df.empty else {}

    votos_df = store.query_df("""
        SELECT
            v.secao_id,
            v.eleicao_id,
            v.cargo,
            v.codigo_cargo,
            v.tipo_voto,
            v.codigo_candidato,
            v.partido,
            v.quantidade
        FROM votos v
        WHERE v.secao_id = ?
        ORDER BY v.cargo, v.tipo_voto, v.quantidade DESC
    """, [secao_id])

    issues_df = store.query_df("""
        SELECT
            i.id,
            i.secao_id,
            i.codigo,
            i.severidade,
            i.descricao,
            i.base_legal,
            i.detalhes
        FROM issues i
        WHERE i.secao_id = ?
        ORDER BY
            CASE i.severidade
                WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                WHEN 'media' THEN 3 ELSE 4
            END,
            i.codigo
    """, [secao_id])

    totais_df = store.query_df("""
        SELECT
            t.secao_id,
            t.eleicao_id,
            t.cargo,
            t.codigo_cargo,
            t.comparecimento,
            t.nominais,
            t.brancos,
            t.nulos,
            t.legenda,
            t.total
        FROM totais_cargo t
        WHERE t.secao_id = ?
        ORDER BY t.cargo
    """, [secao_id])

    return {
        "info": info,
        "votos": votos_df,
        "issues": issues_df,
        "totais": totais_df,
    }


def section_risk_score(store, secao_id) -> dict:
    """Calcula um mini-score de risco para UMA seção.

    Returns:
        dict com:
        - score: int (0-100, onde 100 = mais risco)
        - nivel: str ('OK' | 'ATENÇÃO' | 'ALTO')
        - detalhes: list de strings
    """
    info_df = store.query_df("""
        SELECT
            s.reboots,
            s.has_issues,
            s.n_issues,
            s.pct_biometria,
            s.duracao_min,
            s.is_reserva,
            s.substituicoes
        FROM secoes s
        WHERE s.id = ?
    """, [secao_id])

    if info_df.empty:
        return {
            "score": 0,
            "nivel": "OK",
            "detalhes": ["Seção não encontrada."],
        }

    row = info_df.iloc[0]
    score = 0
    detalhes = []

    reboots = int(row["reboots"]) if pd.notna(row["reboots"]) else 0
    if reboots > 0:
        pontos = min(reboots * 10, 30)
        score += pontos
        detalhes.append(f"Reboots: {reboots} (+{pontos} pts)")

    n_issues = int(row["n_issues"]) if pd.notna(row["n_issues"]) else 0
    if n_issues > 0:
        pontos = min(n_issues * 5, 25)
        score += pontos
        detalhes.append(f"Issues: {n_issues} (+{pontos} pts)")

    pct_bio = float(row["pct_biometria"]) if pd.notna(row["pct_biometria"]) else np.nan
    if pd.notna(pct_bio) and pct_bio < 50:
        score += 20
        detalhes.append(f"Biometria baixa: {pct_bio:.1f}% (+20 pts)")

    duracao = int(row["duracao_min"]) if pd.notna(row["duracao_min"]) else np.nan
    if pd.notna(duracao) and duracao < 360:
        score += 15
        detalhes.append(f"Duração curta: {duracao} min (+15 pts)")

    is_reserva = bool(row["is_reserva"]) if pd.notna(row["is_reserva"]) else False
    if is_reserva:
        score += 5
        detalhes.append("Urna reserva (+5 pts)")

    substituicoes = int(row["substituicoes"]) if pd.notna(row["substituicoes"]) else 0
    if substituicoes > 0:
        score += 10
        detalhes.append(f"Substituições: {substituicoes} (+10 pts)")

    score = min(score, 100)

    if score >= 40:
        nivel = "ALTO"
    elif score >= 15:
        nivel = "ATENÇÃO"
    else:
        nivel = "OK"

    if not detalhes:
        detalhes.append("Nenhum fator de risco identificado.")

    return {
        "score": score,
        "nivel": nivel,
        "detalhes": detalhes,
    }


def state_risk_ranking(store, f) -> pd.DataFrame:
    """Ranking de estados por score de risco agregado.

    Returns:
        DataFrame com: uf, score_risco, rank, pct_reboots, pct_issues, pct_zero_bio
    """
    cols = ["uf", "score_risco", "rank", "pct_reboots", "pct_issues", "pct_zero_bio"]

    df = store.query_df(f"""
        SELECT
            s.uf,
            COUNT(*) AS secoes,
            SUM(CASE WHEN s.reboots > 0 THEN 1 ELSE 0 END) AS secoes_reboot,
            SUM(CASE WHEN s.has_issues THEN 1 ELSE 0 END) AS secoes_issues,
            SUM(CASE WHEN (s.pct_biometria IS NULL OR s.pct_biometria = 0)
                AND s.comparecimento > 0 THEN 1 ELSE 0 END) AS secoes_zero_bio
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf
        HAVING COUNT(*) > 0
        ORDER BY s.uf
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    df["pct_reboots"] = np.where(
        df["secoes"] > 0,
        (df["secoes_reboot"] / df["secoes"] * 100).round(2),
        0.0,
    )
    df["pct_issues"] = np.where(
        df["secoes"] > 0,
        (df["secoes_issues"] / df["secoes"] * 100).round(2),
        0.0,
    )
    df["pct_zero_bio"] = np.where(
        df["secoes"] > 0,
        (df["secoes_zero_bio"] / df["secoes"] * 100).round(2),
        0.0,
    )

    def _normalize(series):
        smin = series.min()
        smax = series.max()
        if smax == smin:
            return pd.Series(0.0, index=series.index)
        return ((series - smin) / (smax - smin) * 100).round(2)

    norm_reboots = _normalize(df["pct_reboots"])
    norm_issues = _normalize(df["pct_issues"])
    norm_zero_bio = _normalize(df["pct_zero_bio"])

    df["score_risco"] = (
        norm_reboots * 0.30
        + norm_issues * 0.40
        + norm_zero_bio * 0.30
    ).round(2)

    df["rank"] = df["score_risco"].rank(ascending=False, method="min").astype(int)

    return (
        df[cols]
        .sort_values("rank")
        .reset_index(drop=True)
    )


# ============================================================
# 17. Melhorias Estatísticas
# ============================================================

def zscore_outliers_by_state(store, f, threshold=3.0) -> pd.DataFrame:
    """Z-score de outliers calculado POR ESTADO (não global).

    Returns:
        DataFrame com: secao_id, uf, metrica, valor, zscore, media_uf, std_uf
    """
    cols = ["secao_id", "uf", "metrica", "valor", "zscore", "media_uf", "std_uf"]

    metrics = [
        ("pct_abstencao", "abstencao"),
        ("pct_biometria", "biometria"),
        ("duracao_min", "duracao"),
    ]

    all_outliers = []
    for col_name, metric_label in metrics:
        df = store.query_df(f"""
            SELECT
                s.id AS secao_id,
                s.uf,
                s.{col_name} AS valor,
                AVG(s.{col_name}) OVER(PARTITION BY s.uf) AS media_uf,
                STDDEV(s.{col_name}) OVER(PARTITION BY s.uf) AS std_uf,
                (s.{col_name} - AVG(s.{col_name}) OVER(PARTITION BY s.uf))
                    / NULLIF(STDDEV(s.{col_name}) OVER(PARTITION BY s.uf), 0) AS zscore
            FROM secoes s
            WHERE {f['where']} AND s.{col_name} IS NOT NULL
        """, f["params"])

        if df.empty:
            continue

        outliers = df[df["zscore"].abs() > threshold].copy()
        if not outliers.empty:
            outliers["metrica"] = metric_label
            outliers["valor"] = outliers["valor"].round(4)
            outliers["zscore"] = outliers["zscore"].round(4)
            outliers["media_uf"] = outliers["media_uf"].round(4)
            outliers["std_uf"] = outliers["std_uf"].round(4)
            all_outliers.append(outliers[cols])

    if not all_outliers:
        return _empty_df(cols)

    return pd.concat(all_outliers, ignore_index=True).sort_values(
        "zscore", ascending=False, key=abs
    ).reset_index(drop=True)


def duration_histogram_data(store, f) -> pd.DataFrame:
    """Dados para histograma de duração (bins de 30 minutos).

    Returns:
        DataFrame com: faixa, quantidade, percentual
    """
    cols = ["faixa", "quantidade", "percentual"]

    df = store.query_df(f"""
        SELECT s.duracao_min
        FROM secoes s
        WHERE {f['where']} AND s.duracao_min IS NOT NULL
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    duracao = df["duracao_min"]
    total = len(duracao)

    max_dur = int(duracao.max())
    bin_size = 30
    upper_limit = ((max_dur // bin_size) + 1) * bin_size + bin_size
    bins = list(range(0, upper_limit + 1, bin_size))

    labels = [f"{bins[i]}-{bins[i+1]}min" for i in range(len(bins) - 1)]

    duracao_cut = pd.cut(
        duracao,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    )

    contagem = duracao_cut.value_counts().sort_index()

    result = pd.DataFrame({
        "faixa": contagem.index.astype(str),
        "quantidade": contagem.values,
    })

    result = result[result["quantidade"] > 0].copy()

    if result.empty:
        return _empty_df(cols)

    result["percentual"] = np.where(
        total > 0,
        (result["quantidade"] / total * 100).round(2),
        0.0,
    )

    return result[cols].reset_index(drop=True)


def biometry_histogram_data(store, f) -> pd.DataFrame:
    """Dados para histograma de biometria (bins de 5%).

    Returns:
        DataFrame com: faixa, quantidade, percentual
    """
    cols = ["faixa", "quantidade", "percentual"]

    df = store.query_df(f"""
        SELECT s.pct_biometria
        FROM secoes s
        WHERE {f['where']} AND s.pct_biometria IS NOT NULL
    """, f["params"])

    if df.empty:
        return _empty_df(cols)

    bio = df["pct_biometria"]
    total = len(bio)

    bin_size = 5
    bins = list(range(0, 105, bin_size))
    labels = [f"{bins[i]}-{bins[i+1]}%" for i in range(len(bins) - 1)]

    bio_cut = pd.cut(
        bio,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    )

    contagem = bio_cut.value_counts().sort_index()

    result = pd.DataFrame({
        "faixa": contagem.index.astype(str),
        "quantidade": contagem.values,
    })

    result = result[result["quantidade"] > 0].copy()

    if result.empty:
        return _empty_df(cols)

    result["percentual"] = np.where(
        total > 0,
        (result["quantidade"] / total * 100).round(2),
        0.0,
    )

    return result[cols].reset_index(drop=True)


# ============================================================
# 18. Exportação de Relatórios
# ============================================================

def generate_excel_report(store, f) -> BytesIO:
    """Gera relatorio Excel em memoria (BytesIO) com multiplas planilhas.

    Planilhas:
        - Resumo: KPIs gerais (secoes, votos, reboots, issues, comparecimento)
        - Por Estado: metricas por UF
        - Issues: todas as issues (secao_id, codigo, severidade, descricao)
        - Outliers: secoes outliers (z-score > 3)
        - Reboots: secoes com reboots > 0

    Returns:
        BytesIO com o arquivo Excel pronto para download.
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # --- Planilha 1: Resumo ---
        resumo_df = store.query_df(f"""
            SELECT
                COUNT(*) AS total_secoes,
                COALESCE(SUM(eleitores_aptos), 0) AS eleitores_aptos,
                COALESCE(SUM(comparecimento), 0) AS comparecimento,
                ROUND(
                    COALESCE(SUM(comparecimento), 0) * 100.0
                    / NULLIF(COALESCE(SUM(eleitores_aptos), 0), 0),
                    2
                ) AS pct_comparecimento,
                COALESCE(SUM(reboots), 0) AS total_reboots,
                SUM(CASE WHEN reboots > 0 THEN 1 ELSE 0 END) AS secoes_com_reboot,
                SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) AS secoes_com_issues,
                COALESCE(SUM(votos_computados), 0) AS total_votos,
                COUNT(DISTINCT uf) AS ufs_cobertas
            FROM secoes s
            WHERE {f['where']}
        """, f["params"])

        if resumo_df.empty:
            resumo_df = pd.DataFrame([{
                "total_secoes": 0, "eleitores_aptos": 0,
                "comparecimento": 0, "pct_comparecimento": 0.0,
                "total_reboots": 0, "secoes_com_reboot": 0,
                "secoes_com_issues": 0, "total_votos": 0,
                "ufs_cobertas": 0,
            }])

        # Transpor para formato de KPIs (metrica / valor)
        row = resumo_df.iloc[0]
        kpis = pd.DataFrame({
            "Metrica": [
                "Total de Secoes",
                "UFs Cobertas",
                "Eleitores Aptos",
                "Comparecimento",
                "% Comparecimento",
                "Total de Votos",
                "Total de Reboots",
                "Secoes com Reboot",
                "Secoes com Issues",
            ],
            "Valor": [
                int(row["total_secoes"]),
                int(row["ufs_cobertas"]),
                int(row["eleitores_aptos"]),
                int(row["comparecimento"]),
                float(row["pct_comparecimento"]) if row["pct_comparecimento"] is not None else 0.0,
                int(row["total_votos"]),
                int(row["total_reboots"]),
                int(row["secoes_com_reboot"]),
                int(row["secoes_com_issues"]),
            ],
        })

        # Adicionar data/hora de geracao
        meta = pd.DataFrame({
            "Metrica": ["Data de Geracao", "Filtros Aplicados"],
            "Valor": [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                f"Turno: {f.get('turno', 'Ambos')} | WHERE: {f['where']}",
            ],
        })
        resumo_final = pd.concat([kpis, meta], ignore_index=True)
        resumo_final.to_excel(writer, sheet_name="Resumo", index=False)

        # --- Planilha 2: Por Estado ---
        por_estado_df = store.query_df(f"""
            SELECT
                UPPER(s.uf) AS uf,
                COUNT(*) AS secoes,
                COALESCE(SUM(s.eleitores_aptos), 0) AS eleitores_aptos,
                COALESCE(SUM(s.comparecimento), 0) AS comparecimento,
                ROUND(
                    COALESCE(SUM(s.comparecimento), 0) * 100.0
                    / NULLIF(COALESCE(SUM(s.eleitores_aptos), 0), 0),
                    2
                ) AS pct_comparecimento,
                COALESCE(SUM(s.reboots), 0) AS total_reboots,
                SUM(CASE WHEN s.reboots > 0 THEN 1 ELSE 0 END) AS secoes_com_reboot,
                SUM(CASE WHEN s.has_issues THEN 1 ELSE 0 END) AS secoes_com_issues,
                ROUND(AVG(s.pct_biometria), 2) AS media_biometria,
                ROUND(AVG(s.duracao_min), 2) AS media_duracao_min
            FROM secoes s
            WHERE {f['where']}
            GROUP BY s.uf
            ORDER BY s.uf
        """, f["params"])

        if por_estado_df.empty:
            por_estado_df = _empty_df([
                "uf", "secoes", "eleitores_aptos", "comparecimento",
                "pct_comparecimento", "total_reboots", "secoes_com_reboot",
                "secoes_com_issues", "media_biometria", "media_duracao_min",
            ])
        por_estado_df.to_excel(writer, sheet_name="Por Estado", index=False)

        # --- Planilha 3: Issues ---
        issues_df = store.query_df(f"""
            SELECT
                i.secao_id,
                i.codigo,
                i.severidade,
                i.descricao,
                s.uf,
                s.municipio,
                s.zona,
                s.secao
            FROM issues i
            JOIN secoes s ON i.secao_id = s.id
            WHERE {f['issue_where']}
            ORDER BY
                CASE i.severidade
                    WHEN 'critica' THEN 1
                    WHEN 'alta' THEN 2
                    WHEN 'media' THEN 3
                    ELSE 4
                END,
                i.codigo, s.uf
        """, f["issue_params"])

        if issues_df.empty:
            issues_df = _empty_df([
                "secao_id", "codigo", "severidade", "descricao",
                "uf", "municipio", "zona", "secao",
            ])
        issues_df.to_excel(writer, sheet_name="Issues", index=False)

        # --- Planilha 4: Outliers ---
        try:
            outliers_df = zscore_outliers(store, f, threshold=3.0)
        except Exception:
            outliers_df = _empty_df([
                "id", "uf", "municipio", "zona", "secao",
                "metric", "value", "zscore",
            ])

        outliers_df.to_excel(writer, sheet_name="Outliers", index=False)

        # --- Planilha 5: Reboots ---
        reboots_df = store.query_df(f"""
            SELECT
                s.id,
                s.turno,
                UPPER(s.uf) AS uf,
                s.municipio,
                s.zona,
                s.secao,
                s.modelo_urna,
                s.reboots,
                s.hora_abertura,
                s.hora_encerramento,
                s.duracao_min,
                s.comparecimento,
                s.erros_log
            FROM secoes s
            WHERE {f['where']} AND s.reboots > 0
            ORDER BY s.reboots DESC, s.uf, s.municipio
        """, f["params"])

        if reboots_df.empty:
            reboots_df = _empty_df([
                "id", "turno", "uf", "municipio", "zona", "secao",
                "modelo_urna", "reboots", "hora_abertura",
                "hora_encerramento", "duracao_min", "comparecimento",
                "erros_log",
            ])
        reboots_df.to_excel(writer, sheet_name="Reboots", index=False)

    output.seek(0)
    return output


def generate_text_report(store, f) -> str:
    """Gera relatorio em texto formatado para download como .txt.

    Inclui:
        - Score de confianca e veredicto
        - KPIs resumidos
        - Categorias de risco
        - Top issues
        - Vulnerabilidades

    Returns:
        String formatada pronta para download.
    """
    lines = []
    sep = "=" * 72

    lines.append(sep)
    lines.append("RELATORIO DE AUDITORIA ELEITORAL - ELEICOES 2022")
    lines.append(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Turno: {f.get('turno', 'Ambos')}")
    lines.append(sep)
    lines.append("")

    # --- Score de confianca ---
    try:
        confidence = compute_confidence_score(store, f)
    except Exception:
        confidence = None

    if confidence and isinstance(confidence, dict) and "score" in confidence:
        score = confidence["score"]
        nivel = confidence.get("nivel", "INDETERMINADO")
        veredicto_votos = confidence.get("veredicto_votos", "")
        veredicto_auditoria = confidence.get("veredicto_auditoria", "")
        justificativa = confidence.get("justificativa", "")
        vulnerabilidades = confidence.get("vulnerabilidades", [])
        categorias = confidence.get("categorias", [])

        lines.append("1. SCORE DE CONFIANCA")
        lines.append("-" * 40)
        lines.append(f"   Score: {score:.1f}/100")
        lines.append(f"   Nivel: {nivel}")
        lines.append("")

        if veredicto_votos:
            lines.append("   Veredicto sobre os votos:")
            for line in textwrap.wrap(veredicto_votos, width=65):
                lines.append(f"   {line}")
            lines.append("")

        if veredicto_auditoria:
            lines.append("   Veredicto da auditoria:")
            for line in textwrap.wrap(veredicto_auditoria, width=65):
                lines.append(f"   {line}")
            lines.append("")

        if justificativa:
            lines.append("   Justificativa:")
            for line in textwrap.wrap(justificativa, width=65):
                lines.append(f"   {line}")
            lines.append("")

        # --- Categorias de risco ---
        if categorias:
            lines.append("")
            lines.append("2. CATEGORIAS DE RISCO")
            lines.append("-" * 40)
            for cat in categorias:
                nome = cat.get("nome", "?")
                cat_score = cat.get("score", 0)
                peso = cat.get("peso", 0)
                status = cat.get("status", "?")
                detalhe = cat.get("detalhe", "")
                lines.append(f"   [{status:>12s}] {nome}: {cat_score:.0f}/100 (peso {peso}%)")
                if detalhe:
                    for line in textwrap.wrap(detalhe, width=60):
                        lines.append(f"               {line}")
            lines.append("")

        # --- Vulnerabilidades ---
        if vulnerabilidades:
            lines.append("")
            lines.append("3. VULNERABILIDADES IDENTIFICADAS")
            lines.append("-" * 40)
            for i, vuln in enumerate(vulnerabilidades, 1):
                lines.append(f"   {i}. {vuln}")
            lines.append("")

    else:
        lines.append("Score de confianca nao disponivel.")
        lines.append("")

    # --- KPIs resumidos ---
    lines.append("")
    lines.append("4. KPIs RESUMIDOS")
    lines.append("-" * 40)

    resumo = store.query_df(f"""
        SELECT
            COUNT(*) AS total_secoes,
            COUNT(DISTINCT uf) AS ufs,
            COALESCE(SUM(eleitores_aptos), 0) AS eleitores,
            COALESCE(SUM(comparecimento), 0) AS comparecimento,
            COALESCE(SUM(reboots), 0) AS total_reboots,
            SUM(CASE WHEN reboots > 0 THEN 1 ELSE 0 END) AS secoes_com_reboot,
            SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) AS secoes_issues
        FROM secoes s WHERE {f['where']}
    """, f["params"])

    if not resumo.empty and int(resumo.iloc[0]["total_secoes"]) > 0:
        r = resumo.iloc[0]
        total_secoes = int(r["total_secoes"])
        total_comp = int(r["comparecimento"])
        total_reboots = int(r["total_reboots"])
        secoes_reboot = int(r["secoes_com_reboot"])
        secoes_issues = int(r["secoes_issues"])
        pct_comp = total_comp / int(r["eleitores"]) * 100 if int(r["eleitores"]) > 0 else 0

        lines.append(f"   Secoes analisadas:     {total_secoes:>12,}")
        lines.append(f"   UFs cobertas:          {int(r['ufs']):>12}")
        lines.append(f"   Eleitores aptos:       {int(r['eleitores']):>12,}")
        lines.append(f"   Comparecimento:        {total_comp:>12,} ({pct_comp:.1f}%)")
        lines.append(f"   Total de reboots:      {total_reboots:>12,}")
        lines.append(f"   Secoes com reboot:     {secoes_reboot:>12,} "
                     f"({secoes_reboot / total_secoes * 100:.1f}%)")
        lines.append(f"   Secoes com issues:     {secoes_issues:>12,} "
                     f"({secoes_issues / total_secoes * 100:.1f}%)")
    else:
        lines.append("   Sem dados para os filtros selecionados.")

    lines.append("")

    # --- Top issues ---
    lines.append("")
    lines.append("5. TOP ISSUES POR FREQUENCIA")
    lines.append("-" * 40)

    top_issues = store.query_df(f"""
        SELECT
            i.codigo,
            i.severidade,
            COUNT(*) AS total,
            COUNT(DISTINCT i.secao_id) AS secoes_afetadas
        FROM issues i
        JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
        GROUP BY i.codigo, i.severidade
        ORDER BY total DESC
        LIMIT 15
    """, f["issue_params"])

    if not top_issues.empty:
        lines.append(f"   {'Codigo':<8} {'Severidade':<12} {'Total':>8} {'Secoes':>8}  Descricao")
        lines.append(f"   {'------':<8} {'----------':<12} {'-----':>8} {'------':>8}  ---------")
        for _, row in top_issues.iterrows():
            codigo = row["codigo"]
            sev = row["severidade"]
            total = int(row["total"])
            secoes = int(row["secoes_afetadas"])
            desc = ISSUE_DESCRICOES.get(codigo, "")
            lines.append(f"   {codigo:<8} {sev:<12} {total:>8,} {secoes:>8,}  {desc}")
    else:
        lines.append("   Nenhuma issue encontrada.")

    lines.append("")

    # --- Integridade de hash ---
    lines.append("")
    lines.append("6. INTEGRIDADE DE HASH (SHA-512)")
    lines.append("-" * 40)

    sig = signature_integrity_summary(store, f)
    lines.append(f"   Total de secoes:       {sig['total_secoes']:>12,}")
    lines.append(f"   Hash OK:               {sig['secoes_hash_ok']:>12,}")
    lines.append(f"   Hash com falha:        {sig['secoes_hash_falha']:>12,}")
    lines.append(f"   % de secoes integras:  {sig['pct_ok']:>11.2f}%")

    lines.append("")
    lines.append(sep)
    lines.append("FIM DO RELATORIO")
    lines.append(sep)

    return "\n".join(lines)


# ============================================================
# 19. Score de Confiança
# ============================================================

def compute_confidence_score(store, f) -> dict:
    """Calcula score de confiança 0-100 com detalhamento por categoria.

    Usa Cramér's V para avaliação de Benford (robusto a amostras grandes).
    Quando turno=None, calcula média ponderada dos dados oficiais T1+T2
    para o veredicto_votos.

    Returns:
        dict com:
        - score: float (0-100)
        - nivel: str (ALTA/MODERADA/BAIXA)
        - categorias: list of dicts com {nome, score, peso, status, detalhe}
        - veredicto_votos: str
        - veredicto_auditoria: str
        - justificativa: str
        - vulnerabilidades: list of str
    """
    turno = f.get("turno")

    # Quando turno=None, usar média ponderada T1+T2 para referência oficial
    if turno is None:
        total_votos_validos = OFICIAL_1T["votos_validos"] + OFICIAL_2T["votos_validos"]
        OFICIAL = {
            "turno": None,
            "eleitores_aptos": OFICIAL_1T["eleitores_aptos"],
            "comparecimento": OFICIAL_1T["comparecimento"] + OFICIAL_2T["comparecimento"],
            "votos_validos": total_votos_validos,
            "lula_pct": (
                OFICIAL_1T["lula_pct"] * OFICIAL_1T["votos_validos"]
                + OFICIAL_2T["lula_pct"] * OFICIAL_2T["votos_validos"]
            ) / total_votos_validos,
            "bolso_pct": (
                OFICIAL_1T["bolso_pct"] * OFICIAL_1T["votos_validos"]
                + OFICIAL_2T["bolso_pct"] * OFICIAL_2T["votos_validos"]
            ) / total_votos_validos,
            "diferenca": OFICIAL_1T["diferenca"] + OFICIAL_2T["diferenca"],
        }
    elif turno == 2:
        OFICIAL = OFICIAL_2T
    else:
        OFICIAL = OFICIAL_1T

    categorias = []
    vulnerabilidades = []

    # ----------------------------------------------------------
    # Dados base
    # ----------------------------------------------------------
    summary = store.query_df(f"""
        SELECT
            COUNT(*) AS secoes,
            COUNT(DISTINCT uf) AS ufs,
            COALESCE(SUM(eleitores_aptos), 0) AS eleitores,
            COALESCE(SUM(comparecimento), 0) AS comparecimento,
            COALESCE(SUM(reboots), 0) AS total_reboots,
            SUM(CASE WHEN reboots > 0 THEN 1 ELSE 0 END) AS secoes_com_reboot,
            SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) AS secoes_issues
        FROM secoes s WHERE {f['where']}
    """, f["params"])

    if summary.empty or int(summary.iloc[0]["secoes"]) == 0:
        return {
            "score": 0.0,
            "nivel": "BAIXA",
            "categorias": [],
            "veredicto_votos": "Sem dados para análise.",
            "veredicto_auditoria": "Sem dados para análise.",
            "justificativa": "Nenhum dado disponível nos filtros selecionados.",
            "vulnerabilidades": [],
        }

    s = summary.iloc[0]
    total_secoes = int(s["secoes"])
    total_comp = int(s["comparecimento"])
    total_reboots = int(s["total_reboots"])
    secoes_com_reboot = int(s["secoes_com_reboot"])

    # Votos para Presidente
    votos_pres = store.query_df(f"""
        SELECT
            v.codigo_candidato AS candidato,
            SUM(v.quantidade) AS votos
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = 'Presidente'
            AND v.tipo_voto = 'nominal'
        GROUP BY v.codigo_candidato
        ORDER BY votos DESC
    """, f["params"])

    total_nominais = int(votos_pres["votos"].sum()) if not votos_pres.empty else 0

    pct_lula_am = 0.0
    pct_bolso_am = 0.0
    if not votos_pres.empty and total_nominais > 0:
        for _, row in votos_pres.iterrows():
            cand = int(row["candidato"])
            pct = int(row["votos"]) / total_nominais * 100
            if cand == 13:
                pct_lula_am = pct
            elif cand == 22:
                pct_bolso_am = pct

    # Issues por código
    issues_by_code = store.query_df(f"""
        SELECT i.codigo, COUNT(*) AS total
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
        GROUP BY i.codigo
    """, f["issue_params"])

    issue_counts = {}
    if not issues_by_code.empty:
        issue_counts = dict(zip(issues_by_code["codigo"], issues_by_code["total"]))

    # ----------------------------------------------------------
    # Categoria 1: Integridade de hash (C01)
    # ----------------------------------------------------------
    c01_count = int(issue_counts.get("C01", 0))
    if c01_count == 0:
        cat1_score = 100.0
        cat1_status = "OK"
        cat1_detalhe = "Nenhum hash SHA-512 inválido detectado."
    else:
        cat1_score = max(0.0, 100.0 - c01_count * 30.0)
        cat1_status = "CRITICO"
        cat1_detalhe = f"{c01_count} arquivo(s) com hash SHA-512 inválido."
        vulnerabilidades.append(f"Hash inválido em {c01_count} arquivo(s) (C01)")

    categorias.append({
        "nome": "Integridade hash (C01)",
        "score": round(cat1_score, 1),
        "peso": 25,
        "status": cat1_status,
        "detalhe": cat1_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 2: Overflow de votos (C05)
    # ----------------------------------------------------------
    c05_count = int(issue_counts.get("C05", 0))
    if c05_count == 0:
        cat2_score = 100.0
        cat2_status = "OK"
        cat2_detalhe = "Nenhuma seção com votos excedendo eleitores aptos."
    else:
        cat2_score = max(0.0, 100.0 - c05_count * 25.0)
        cat2_status = "CRITICO"
        cat2_detalhe = f"{c05_count} seção(ões) com votos > eleitores aptos."
        vulnerabilidades.append(f"Overflow de votos em {c05_count} seção(ões) (C05)")

    categorias.append({
        "nome": "Overflow votos (C05)",
        "score": round(cat2_score, 1),
        "peso": 20,
        "status": cat2_status,
        "detalhe": cat2_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 3: Conformidade Benford (Cramér's V)
    # ----------------------------------------------------------
    try:
        benford_digit_df = benford_first_digit(store, f)

        if benford_digit_df.empty:
            cat3_score = 50.0
            cat3_status = "INDETERMINADO"
            cat3_detalhe = "Dados insuficientes para teste de Benford."
        else:
            # Calcular Cramér's V por candidato
            cramers_values = []
            for candidato, grp in benford_digit_df.groupby("candidato"):
                full = pd.DataFrame({"digito": range(1, 10)})
                merged = full.merge(
                    grp[["digito", "contagem"]], on="digito", how="left"
                ).fillna(0)
                observed = merged["contagem"].values.astype(float)
                total = observed.sum()

                if total == 0:
                    continue

                expected = np.array(
                    [BENFORD_EXPECTED[d] * total for d in range(1, 10)]
                )
                mask = expected > 0
                if mask.sum() == 0:
                    continue

                chi2_stat, _ = stats.chisquare(observed[mask], f_exp=expected[mask])
                k = int(mask.sum())
                v = _cramers_v(float(chi2_stat), total, k)
                cramers_values.append(v)

            if not cramers_values:
                cat3_score = 50.0
                cat3_status = "INDETERMINADO"
                cat3_detalhe = "Dados insuficientes para calcular Cramér's V."
            else:
                # Usar o MAIOR Cramér's V (pior caso) para definir o score
                max_v = max(cramers_values)
                avg_v = sum(cramers_values) / len(cramers_values)

                cat3_score = _benford_cramers_v_score(max_v)

                if max_v < 0.05:
                    cat3_status = "OK"
                    cat3_detalhe = (
                        f"Distribuição conforme Benford para todos os candidatos "
                        f"(Cramér's V máximo: {max_v:.4f}, média: {avg_v:.4f}). "
                        f"Efeito desprezível."
                    )
                elif max_v < 0.10:
                    cat3_status = "OK"
                    cat3_detalhe = (
                        f"Distribuição aceitável segundo Benford "
                        f"(Cramér's V máximo: {max_v:.4f}, média: {avg_v:.4f}). "
                        f"Desvio sem relevância prática."
                    )
                elif max_v < 0.20:
                    cat3_status = "ATENCAO"
                    cat3_detalhe = (
                        f"Desvio moderado da Lei de Benford "
                        f"(Cramér's V máximo: {max_v:.4f}, média: {avg_v:.4f}). "
                        f"Pode refletir distribuição natural das seções."
                    )
                else:
                    cat3_status = "ALTO"
                    cat3_detalhe = (
                        f"Desvio relevante da Lei de Benford "
                        f"(Cramér's V máximo: {max_v:.4f}, média: {avg_v:.4f}). "
                        f"Investigação recomendada."
                    )
                    vulnerabilidades.append(
                        f"Cramér's V elevado na análise de Benford: {max_v:.4f}"
                    )

    except Exception:
        cat3_score = 50.0
        cat3_status = "ERRO"
        cat3_detalhe = "Erro ao calcular teste de Benford."

    categorias.append({
        "nome": "Conformidade Benford",
        "score": round(cat3_score, 1),
        "peso": 15,
        "status": cat3_status,
        "detalhe": cat3_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 4: Impacto de reboots
    # ----------------------------------------------------------
    pct_reboot = secoes_com_reboot / total_secoes * 100 if total_secoes > 0 else 0

    if pct_reboot == 0:
        cat4_score = 100.0
        cat4_status = "OK"
        cat4_detalhe = "Nenhum reboot durante a votação."
    elif pct_reboot < 10:
        cat4_score = 85.0
        cat4_status = "BAIXO"
        cat4_detalhe = f"Taxa de reboots aceitável: {pct_reboot:.1f}%."
    elif pct_reboot < 20:
        cat4_score = 60.0
        cat4_status = "MEDIO"
        cat4_detalhe = f"Taxa moderada de reboots: {pct_reboot:.1f}%."
        vulnerabilidades.append(f"Taxa moderada de reboots: {pct_reboot:.1f}%")
    else:
        cat4_score = 30.0
        cat4_status = "ALTO"
        cat4_detalhe = f"Taxa elevada de reboots: {pct_reboot:.1f}%."
        vulnerabilidades.append(f"Taxa elevada de reboots: {pct_reboot:.1f}%")

    # Verificar se reboots favorecem candidato
    try:
        reboot_corr = reboot_candidate_correlation(store, f)
        if reboot_corr.get("p_value") is not None and reboot_corr["p_value"] <= 0.05:
            cat4_score = max(cat4_score - 20, 0)
            cat4_detalhe += (
                f" Distribuição de votos difere significativamente em seções com reboot "
                f"(p={reboot_corr['p_value']})."
            )
    except Exception:
        pass

    categorias.append({
        "nome": "Impacto reboots",
        "score": round(cat4_score, 1),
        "peso": 15,
        "status": cat4_status,
        "detalhe": cat4_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 5: Consistência T1/T2
    # ----------------------------------------------------------
    try:
        attendance = cross_turno_attendance(store, f)
        if attendance.empty:
            cat5_score = 50.0
            cat5_status = "INDETERMINADO"
            cat5_detalhe = "Dados de ambos os turnos não disponíveis para comparação."
        else:
            variacao_media = attendance["variacao_pct"].abs().mean()
            if variacao_media < 5:
                cat5_score = 100.0
                cat5_status = "OK"
            elif variacao_media < 10:
                cat5_score = 75.0
                cat5_status = "ATENCAO"
            elif variacao_media < 20:
                cat5_score = 50.0
                cat5_status = "MEDIO"
            else:
                cat5_score = 25.0
                cat5_status = "ALTO"
                vulnerabilidades.append(
                    f"Variação média de comparecimento entre turnos: {variacao_media:.1f}%"
                )

            cat5_detalhe = (
                f"Variação média de comparecimento entre T1/T2: {variacao_media:.2f}%. "
                f"{len(attendance)} UFs pareadas."
            )
    except Exception:
        cat5_score = 50.0
        cat5_status = "ERRO"
        cat5_detalhe = "Erro ao comparar turnos."

    categorias.append({
        "nome": "Consistência T1/T2",
        "score": round(cat5_score, 1),
        "peso": 10,
        "status": cat5_status,
        "detalhe": cat5_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 6: Concentração de outliers
    # ----------------------------------------------------------
    try:
        outliers = outlier_summary(store, f)
        if outliers.empty:
            cat6_score = 100.0
            cat6_status = "OK"
            cat6_detalhe = "Nenhum outlier estatístico detectado (z > 3)."
        else:
            total_outliers = int(outliers["total"].sum())
            pct_outliers = total_outliers / total_secoes * 100 if total_secoes > 0 else 0
            if pct_outliers < 2:
                cat6_score = 95.0
                cat6_status = "OK"
            elif pct_outliers < 5:
                cat6_score = 75.0
                cat6_status = "ATENCAO"
            elif pct_outliers < 10:
                cat6_score = 50.0
                cat6_status = "MEDIO"
            else:
                cat6_score = 25.0
                cat6_status = "ALTO"
                vulnerabilidades.append(
                    f"Alta concentração de outliers: {pct_outliers:.1f}% das seções"
                )
            cat6_detalhe = (
                f"{total_outliers} outliers em {len(outliers)} UFs "
                f"({pct_outliers:.2f}% das seções)."
            )
    except Exception:
        cat6_score = 50.0
        cat6_status = "ERRO"
        cat6_detalhe = "Erro ao calcular outliers."

    categorias.append({
        "nome": "Concentração outliers",
        "score": round(cat6_score, 1),
        "peso": 5,
        "status": cat6_status,
        "detalhe": cat6_detalhe,
    })

    # ----------------------------------------------------------
    # Categoria 7: Segurança biometria
    # ----------------------------------------------------------
    try:
        zero_bio = zero_biometry_sections(store, f)
        n_zero_bio = len(zero_bio)
        pct_zero_bio = n_zero_bio / total_secoes * 100 if total_secoes > 0 else 0

        if pct_zero_bio == 0:
            cat7_score = 100.0
            cat7_status = "OK"
            cat7_detalhe = "Todas as seções com verificação biométrica."
        elif pct_zero_bio < 5:
            cat7_score = 80.0
            cat7_status = "ATENCAO"
            cat7_detalhe = f"{n_zero_bio} seções ({pct_zero_bio:.1f}%) sem biometria."
        elif pct_zero_bio < 15:
            cat7_score = 55.0
            cat7_status = "MEDIO"
            cat7_detalhe = f"{n_zero_bio} seções ({pct_zero_bio:.1f}%) sem biometria."
            vulnerabilidades.append(f"{pct_zero_bio:.1f}% das seções sem biometria")
        else:
            cat7_score = 30.0
            cat7_status = "ALTO"
            cat7_detalhe = f"{n_zero_bio} seções ({pct_zero_bio:.1f}%) sem biometria."
            vulnerabilidades.append(f"{pct_zero_bio:.1f}% das seções sem biometria")
    except Exception:
        cat7_score = 50.0
        cat7_status = "ERRO"
        cat7_detalhe = "Erro ao avaliar biometria."

    categorias.append({
        "nome": "Segurança biometria",
        "score": round(cat7_score, 1),
        "peso": 10,
        "status": cat7_status,
        "detalhe": cat7_detalhe,
    })

    # ----------------------------------------------------------
    # Score ponderado final
    # ----------------------------------------------------------
    total_peso = sum(c["peso"] for c in categorias)
    if total_peso > 0:
        score = sum(c["score"] * c["peso"] for c in categorias) / total_peso
    else:
        score = 0.0

    score = max(0.0, min(100.0, score))

    # Classificação
    if score >= 85:
        nivel = "ALTA"
    elif score >= 60:
        nivel = "MODERADA"
    else:
        nivel = "BAIXA"

    # ----------------------------------------------------------
    # Veredictos textuais
    # ----------------------------------------------------------
    desvio_lula = abs(pct_lula_am - OFICIAL["lula_pct"]) if total_nominais > 0 else 0
    desvio_bolso = abs(pct_bolso_am - OFICIAL["bolso_pct"]) if total_nominais > 0 else 0
    desvio_max = max(desvio_lula, desvio_bolso)

    # Veredicto sobre votos
    if total_nominais == 0:
        veredicto_votos = "Sem dados de votação para avaliar consistência."
    elif desvio_max < 1.0:
        veredicto_votos = (
            f"Resultados da amostra consistentes com o oficial TSE "
            f"(desvio máximo: {desvio_max:.2f} p.p.)."
        )
    elif desvio_max < 3.0:
        veredicto_votos = (
            f"Desvio moderado entre amostra e oficial "
            f"(Lula: {desvio_lula:.2f} p.p., Bolsonaro: {desvio_bolso:.2f} p.p.). "
            f"Pode refletir composição da amostra."
        )
    else:
        veredicto_votos = (
            f"Desvio significativo entre amostra e oficial "
            f"(Lula: {desvio_lula:.2f} p.p., Bolsonaro: {desvio_bolso:.2f} p.p.). "
            f"Verificar viés na amostragem."
        )

    # Nota sobre referência oficial quando turno=None
    if turno is None and total_nominais > 0:
        veredicto_votos += (
            " Referência oficial: média ponderada T1+T2 "
            f"(Lula: {OFICIAL['lula_pct']:.2f}%, Bolsonaro: {OFICIAL['bolso_pct']:.2f}%)."
        )

    # Veredicto de auditoria
    if score >= 85:
        veredicto_auditoria = (
            "A auditoria NÃO encontrou evidências de fraude ou manipulação "
            "que comprometam o resultado."
        )
    elif score >= 60:
        veredicto_auditoria = (
            "Foram encontradas inconsistências que merecem investigação, "
            "mas isoladamente não são suficientes para questionar o resultado."
        )
    else:
        veredicto_auditoria = (
            "Inconsistências significativas identificadas. "
            "Recomenda-se auditoria completa e independente."
        )

    # Justificativa
    n_criticos = sum(1 for c in categorias if c["status"] == "CRITICO")
    n_altos = sum(1 for c in categorias if c["status"] == "ALTO")
    n_ok = sum(1 for c in categorias if c["status"] == "OK")

    justificativa_parts = []
    justificativa_parts.append(
        f"Score baseado em {len(categorias)} categorias de análise "
        f"({n_ok} OK, {n_criticos} críticas, {n_altos} altas)."
    )
    justificativa_parts.append(
        f"Amostra de {total_secoes:,} seções "
        f"({total_comp:,} votos, {int(s['ufs'])} UFs)."
    )
    if total_nominais > 0:
        justificativa_parts.append(
            f"Desvio máximo vs resultado oficial: {desvio_max:.2f} p.p."
        )
    justificativa_parts.append(
        "Benford avaliado via Cramér's V (robusto a amostras grandes)."
    )

    justificativa = " ".join(justificativa_parts)

    return {
        "score": round(score, 1),
        "nivel": nivel,
        "categorias": categorias,
        "veredicto_votos": veredicto_votos,
        "veredicto_auditoria": veredicto_auditoria,
        "justificativa": justificativa,
        "vulnerabilidades": vulnerabilidades,
    }
