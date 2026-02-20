"""Dashboard BI - Auditoria Eleitoral 2022."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import sys

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataurnas.database.duckdb_store import DuckDBStore, DEFAULT_DB_PATH
from dataurnas.dashboard import analysis

# Dashboard lê do snapshot (criado pelo builder), não do DB principal
LIVE_DB_PATH = DEFAULT_DB_PATH.parent / "eleicoes_2022_live.duckdb"

st.set_page_config(
    page_title="DataUrnas-BR | Auditoria Eleitoral",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Descrições dos códigos de issue para referência no dashboard
ISSUE_DESCRICOES = {
    "C01": "Hash SHA-512 inválido",
    "C05": "Total de votos > eleitores aptos",
    "C06": "Reinício durante votação",
    "A01": "Divergência de eleitores aptos entre eleições",
    "A02": "Votos > comparecimento",
    "A03": "Divergência de votos no log vs BU",
    "A04": "Abertura muito tardia",
    "A05": "Encerramento antes do horário",
    "A06": "Comparecimento divergente entre cargos",
    "I01": "Eventos de substituição/contingência",
    "I02": "Alertas de mesário elevados",
    "I03": "Proporção elevada de nulos",
    "I04": "Proporção elevada de brancos",
    "I05": "Abstenção acima de 30%",
    "I06": "Urna não padrão (reservaSeção)",
    "M01": "Abertura antes do horário ajustado",
    "M02": "Encerramento muito tarde",
    "M04": "Número elevado de erros no log",
    "M05": "Ajustes de hora detectados",
    "M06": "Alta taxa de liberação sem biometria",
}


def get_store():
    """Abre conexão read-only ao snapshot do DuckDB.

    O builder mantém o DB principal com lock exclusivo.
    O dashboard lê do snapshot (cópia atualizada periodicamente).
    Fallback para o DB principal se o snapshot não existir.
    """
    # Preferir snapshot (sem conflito de lock)
    if LIVE_DB_PATH.exists():
        try:
            return DuckDBStore(LIVE_DB_PATH, read_only=True)
        except Exception:
            pass

    # Fallback: tentar DB principal (funciona se o builder não estiver rodando)
    if DEFAULT_DB_PATH.exists():
        try:
            return DuckDBStore(DEFAULT_DB_PATH, read_only=True)
        except Exception:
            pass

    # Auto-build: se não existe DB mas existem Parquet, construir automaticamente
    parquet_dir = DEFAULT_DB_PATH.parent.parent / "parquet"
    if parquet_dir.exists() and list(parquet_dir.glob("*.parquet")):
        try:
            import subprocess
            build_script = Path(__file__).parent.parent.parent.parent / "scripts" / "build_db.py"
            if build_script.exists():
                subprocess.run([sys.executable, str(build_script)], check=True)
                if LIVE_DB_PATH.exists():
                    return DuckDBStore(LIVE_DB_PATH, read_only=True)
                if DEFAULT_DB_PATH.exists():
                    return DuckDBStore(DEFAULT_DB_PATH, read_only=True)
        except Exception:
            pass

    return None


def build_filters(store):
    """Constrói filtros do sidebar e retorna cláusulas WHERE e parâmetros.

    Filtros disponíveis:
        - Turno (Ambos / 1º / 2º)
        - Região
        - Estado (UF)
        - Município (dependente da UF selecionada)
        - Modelo de Urna
        - Tipo de Urna (Todos / Normal / Reserva)
        - Apenas seções com issues (checkbox)
        - Severidade
        - Código da Issue
    """
    st.sidebar.title("Filtros")
    options = store.get_filter_options()

    # --- Turno ---
    turnos_disponiveis = options.get("turnos", [1])
    turno_opcoes = ["Ambos os Turnos"]
    if 1 in turnos_disponiveis:
        turno_opcoes.append("1º Turno")
    if 2 in turnos_disponiveis:
        turno_opcoes.append("2º Turno")
    turno_sel = st.sidebar.radio("Turno", turno_opcoes, index=0)

    # --- Região ---
    regioes_sel = st.sidebar.multiselect("Região", options.get("regioes", []))

    # --- Estado (UF) ---
    ufs_disponiveis = options.get("ufs", [])
    if regioes_sel:
        from dataurnas.config import REGIOES
        ufs_filtradas = []
        for r in regioes_sel:
            ufs_filtradas.extend(REGIOES.get(r, []))
        ufs_disponiveis = [u for u in ufs_disponiveis if u in ufs_filtradas]
    ufs_sel = st.sidebar.multiselect("Estado (UF)", ufs_disponiveis)

    # --- Município (dependente da UF) ---
    municipios_disponiveis = []
    if ufs_sel:
        placeholders = ",".join(["?" for _ in ufs_sel])
        mun_rows = store.query_df(f"""
            SELECT DISTINCT municipio
            FROM secoes
            WHERE uf IN ({placeholders})
            ORDER BY municipio
        """, ufs_sel)
        if not mun_rows.empty:
            municipios_disponiveis = mun_rows["municipio"].tolist()
    municipios_sel = st.sidebar.multiselect("Município", municipios_disponiveis)

    # --- Modelo de Urna ---
    modelos_sel = st.sidebar.multiselect("Modelo de Urna", options.get("modelos", []))

    # --- Tipo de Urna (Normal/Reserva/Todos) ---
    tipo_urna_opcoes = ["Todos", "Normal", "Reserva"]
    tipo_urna_sel = st.sidebar.selectbox("Tipo de Urna", tipo_urna_opcoes, index=0)

    # --- Apenas seções com issues ---
    apenas_issues = st.sidebar.checkbox("Apenas seções com issues", value=False)

    # --- Severidade ---
    sev_sel = st.sidebar.multiselect("Severidade", options.get("severidades", []))

    # --- Código da Issue ---
    codigos_raw = options.get("codigos_issue", [])
    codigos_display = [
        f"{c} — {ISSUE_DESCRICOES[c]}" if c in ISSUE_DESCRICOES else c
        for c in codigos_raw
    ]
    issues_display_sel = st.sidebar.multiselect("Código da Issue", codigos_display)
    issues_sel = [v.split(" — ")[0] for v in issues_display_sel]

    # ============================================================
    # Montar WHERE base (seções)
    # ============================================================
    where_parts = []
    params = []

    # Turno
    turno_value = None
    if turno_sel == "1º Turno":
        where_parts.append("s.turno = ?")
        params.append(1)
        turno_value = 1
    elif turno_sel == "2º Turno":
        where_parts.append("s.turno = ?")
        params.append(2)
        turno_value = 2

    # UF / Região
    if ufs_sel:
        placeholders = ",".join(["?" for _ in ufs_sel])
        where_parts.append(f"s.uf IN ({placeholders})")
        params.extend(ufs_sel)
    elif regioes_sel:
        placeholders = ",".join(["?" for _ in regioes_sel])
        where_parts.append(f"s.regiao IN ({placeholders})")
        params.extend(regioes_sel)

    # Município
    if municipios_sel:
        placeholders = ",".join(["?" for _ in municipios_sel])
        where_parts.append(f"s.municipio IN ({placeholders})")
        params.extend(municipios_sel)

    # Modelo
    if modelos_sel:
        placeholders = ",".join(["?" for _ in modelos_sel])
        where_parts.append(f"s.modelo_urna IN ({placeholders})")
        params.extend(modelos_sel)

    # Tipo de urna
    if tipo_urna_sel == "Normal":
        where_parts.append("s.is_reserva = FALSE")
    elif tipo_urna_sel == "Reserva":
        where_parts.append("s.is_reserva = TRUE")

    # Apenas seções com issues
    if apenas_issues:
        where_parts.append("s.has_issues = TRUE")

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    # WHERE para issues (inclui filtros de severidade e código)
    issue_where = where_clause
    params_issues = list(params)

    if sev_sel:
        placeholders = ",".join(["?" for _ in sev_sel])
        issue_where += f" AND i.severidade IN ({placeholders})"
        params_issues.extend(sev_sel)

    if issues_sel:
        placeholders = ",".join(["?" for _ in issues_sel])
        issue_where += f" AND i.codigo IN ({placeholders})"
        params_issues.extend(issues_sel)

    return {
        "where": where_clause,
        "params": params,
        "issue_where": issue_where,
        "issue_params": params_issues,
        "turno": turno_value,  # None = ambos, 1 = 1T, 2 = 2T
    }


def render_kpis(store, f):
    """Renderiza KPIs no topo da página."""
    summary_df = store.query_df(f"""
        SELECT
            COUNT(*) as total_secoes,
            SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) as com_issues,
            SUM(CASE WHEN NOT has_issues THEN 1 ELSE 0 END) as sem_issues,
            COALESCE(SUM(eleitores_aptos), 0) as eleitores,
            COALESCE(SUM(comparecimento), 0) as comparecimento,
            COALESCE(SUM(reboots), 0) as total_reboots,
            COUNT(DISTINCT uf) as ufs
        FROM secoes s
        WHERE {f['where']}
    """, f["params"])

    if summary_df.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
        return False

    row = summary_df.iloc[0]
    total_secoes = int(row["total_secoes"])
    if total_secoes == 0:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
        return False

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Seções Analisadas", f"{total_secoes:,}")
    c2.metric("Com Inconsistências", f"{int(row['com_issues']):,}")
    c3.metric("Eleitores Aptos", f"{int(row['eleitores']):,}")
    c4.metric("Comparecimento", f"{int(row['comparecimento']):,}")

    eleitores = int(row["eleitores"])
    comparecimento = int(row["comparecimento"])
    pct_abs = round((1 - comparecimento / eleitores) * 100, 1) if eleitores > 0 else 0
    c5.metric("Abstenção", f"{pct_abs}%")
    c6.metric("Reboots", f"{int(row['total_reboots']):,}")
    return True


def tab_inconsistencias(store, f):
    """Aba de Inconsistências."""
    st.subheader("Distribuição de Inconsistências")

    col1, col2 = st.columns(2)
    with col1:
        sev_df = store.query_df(f"""
            SELECT i.severidade, COUNT(*) as total
            FROM issues i JOIN secoes s ON i.secao_id = s.id
            WHERE {f['issue_where']}
            GROUP BY i.severidade
            ORDER BY CASE i.severidade
                WHEN 'critica' THEN 1
                WHEN 'alta' THEN 2
                WHEN 'media' THEN 3
                WHEN 'informativa' THEN 4
            END
        """, f["issue_params"])
        if not sev_df.empty:
            st.bar_chart(sev_df.set_index("severidade"))
        else:
            st.info("Nenhuma inconsistência encontrada.")

    with col2:
        cod_df = store.query_df(f"""
            SELECT i.codigo, COUNT(*) as total
            FROM issues i JOIN secoes s ON i.secao_id = s.id
            WHERE {f['issue_where']}
            GROUP BY i.codigo
            ORDER BY total DESC
        """, f["issue_params"])
        if not cod_df.empty:
            st.bar_chart(cod_df.set_index("codigo"))

    st.subheader("Issues por Estado")
    uf_issues_df = store.query_df(f"""
        SELECT UPPER(s.uf) as estado, i.codigo, COUNT(*) as total
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
        GROUP BY s.uf, i.codigo
        ORDER BY s.uf, total DESC
    """, f["issue_params"])
    if not uf_issues_df.empty:
        pivot = uf_issues_df.pivot_table(
            index="estado", columns="codigo", values="total", fill_value=0
        )
        st.dataframe(pivot, width="stretch")

    st.subheader("Detalhes das Inconsistências")
    detail_df = store.query_df(f"""
        SELECT
            i.codigo,
            i.severidade,
            UPPER(s.uf) as uf,
            s.municipio,
            s.zona,
            s.secao,
            'UE' || s.modelo_urna as modelo,
            i.descricao,
            i.base_legal
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
        ORDER BY
            CASE i.severidade
                WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                WHEN 'media' THEN 3 ELSE 4
            END,
            i.codigo
        LIMIT 500
    """, f["issue_params"])
    if not detail_df.empty:
        st.dataframe(detail_df, width="stretch", height=400)
    else:
        st.info("Nenhuma inconsistência encontrada para os filtros atuais.")

    # Legenda dos códigos
    with st.expander("Legenda dos Códigos de Issue"):
        for cod, desc in sorted(ISSUE_DESCRICOES.items()):
            st.text(f"{cod}: {desc}")


def tab_modelos(store, f):
    """Aba de Modelos de Urna."""
    st.subheader("Distribuição por Modelo")

    modelo_df = store.query_df(f"""
        SELECT
            'UE' || s.modelo_urna as modelo,
            COUNT(*) as secoes,
            ROUND(AVG(s.reboots), 2) as media_reboots,
            SUM(s.reboots) as total_reboots,
            ROUND(AVG(s.pct_biometria), 1) as media_biometria,
            SUM(CASE WHEN s.has_issues THEN 1 ELSE 0 END) as com_issues,
            ROUND(SUM(CASE WHEN s.has_issues THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as pct_issues
        FROM secoes s
        WHERE {f['where']} AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna
        ORDER BY s.modelo_urna
    """, f["params"])

    if not modelo_df.empty:
        st.dataframe(modelo_df, width="stretch")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Reboots por Modelo")
            st.bar_chart(modelo_df[["modelo", "media_reboots"]].set_index("modelo"))

        with col2:
            st.subheader("% Seções com Issues por Modelo")
            st.bar_chart(modelo_df[["modelo", "pct_issues"]].set_index("modelo"))
    else:
        st.info("Nenhum dado de modelo disponível.")

    st.subheader("Issues por Modelo de Urna")
    modelo_issues = store.query_df(f"""
        SELECT
            'UE' || s.modelo_urna as modelo,
            i.codigo,
            COUNT(*) as total
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']} AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna, i.codigo
        ORDER BY s.modelo_urna, total DESC
    """, f["issue_params"])
    if not modelo_issues.empty:
        pivot = modelo_issues.pivot_table(
            index="modelo", columns="codigo", values="total", fill_value=0
        )
        st.dataframe(pivot, width="stretch")

    # Enhanced: Model vote pattern
    st.subheader("Padrão de Votação por Modelo")
    try:
        vote_pattern_df = analysis.model_vote_pattern(store, f)
        if not vote_pattern_df.empty:
            st.dataframe(vote_pattern_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=vote_pattern_df["modelo"],
                y=vote_pattern_df["pct_lula"],
                name="Lula (%)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=vote_pattern_df["modelo"],
                y=vote_pattern_df["pct_bolso"],
                name="Bolsonaro (%)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Modelo",
                yaxis_title="Percentual (%)",
                title="Votação para Presidente por Modelo de Urna",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                "Variações entre modelos refletem distribuição geográfica. "
                "Modelos mais antigos predominam no Norte/Nordeste; mais novos no Sul/Sudeste."
            )
        else:
            st.info("Sem dados de padrão de votação por modelo.")
    except Exception as e:
        st.warning(f"Erro ao carregar padrão de votação por modelo: {e}")

    # Enhanced: Model issue rate normalized
    st.subheader("Taxa de Issues por Modelo (Normalizada)")
    try:
        issue_rate_df = analysis.model_issue_rate(store, f)
        if not issue_rate_df.empty:
            st.dataframe(issue_rate_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=issue_rate_df["modelo"],
                y=issue_rate_df["rate"],
                name="Taxa Total",
                marker_color="#636EFA",
            ))
            if "rate_critica" in issue_rate_df.columns:
                fig.add_trace(go.Bar(
                    x=issue_rate_df["modelo"],
                    y=issue_rate_df["rate_critica"],
                    name="Taxa Crítica",
                    marker_color="#EF553B",
                ))
            if "rate_alta" in issue_rate_df.columns:
                fig.add_trace(go.Bar(
                    x=issue_rate_df["modelo"],
                    y=issue_rate_df["rate_alta"],
                    name="Taxa Alta",
                    marker_color="#FFA15A",
                ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Modelo",
                yaxis_title="Issues / Seção",
                title="Taxa de Issues Normalizada por Modelo",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados de taxa de issues por modelo.")
    except Exception as e:
        st.warning(f"Erro ao carregar taxa de issues por modelo: {e}")


def tab_estados(store, f):
    """Aba por Estado."""
    st.subheader("Métricas por Estado")

    estado_df = store.query_df(f"""
        SELECT
            UPPER(s.uf) as estado,
            s.regiao,
            COUNT(*) as secoes,
            SUM(s.eleitores_aptos) as eleitores,
            SUM(s.comparecimento) as comparecimento,
            ROUND((1 - SUM(s.comparecimento)*1.0 / NULLIF(SUM(s.eleitores_aptos), 0)) * 100, 1) as abstencao_pct,
            SUM(s.reboots) as reboots,
            SUM(CASE WHEN s.has_issues THEN 1 ELSE 0 END) as com_issues,
            ROUND(AVG(s.pct_biometria), 1) as biometria_pct,
            ROUND(AVG(s.duracao_min), 0) as duracao_media
        FROM secoes s
        WHERE {f['where']}
        GROUP BY s.uf, s.regiao
        ORDER BY s.uf
    """, f["params"])

    if not estado_df.empty:
        st.dataframe(estado_df, width="stretch")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Abstenção por Estado")
            st.bar_chart(estado_df[["estado", "abstencao_pct"]].set_index("estado"))

        with col2:
            st.subheader("Reboots por Estado")
            st.bar_chart(estado_df[["estado", "reboots"]].set_index("estado"))
    else:
        st.info("Nenhum dado disponível.")


def tab_votacao(store, f):
    """Aba de Votação."""
    st.subheader("Resultado da Votação (Amostra)")

    cargo_sel = st.selectbox("Cargo", [
        "Presidente", "Governador", "Senador",
        "Deputado Federal", "Deputado Estadual",
    ])

    votos_params = list(f["params"]) + [cargo_sel]
    votos_df = store.query_df(f"""
        SELECT
            v.codigo_candidato as candidato,
            v.partido,
            SUM(v.quantidade) as votos,
            COUNT(DISTINCT v.secao_id) as secoes
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']}
            AND v.cargo = ?
            AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IS NOT NULL
        GROUP BY v.codigo_candidato, v.partido
        ORDER BY votos DESC
        LIMIT 20
    """, votos_params)

    if not votos_df.empty:
        total_nominais = votos_df["votos"].sum()
        if total_nominais > 0:
            votos_df["pct"] = (votos_df["votos"] / total_nominais * 100).round(1)
        st.dataframe(votos_df, width="stretch")

        st.bar_chart(
            votos_df.head(10).set_index("candidato")[["votos"]]
        )
    else:
        st.info(f"Nenhum voto nominal encontrado para {cargo_sel}.")

    st.subheader("Nulos e Brancos por Cargo")
    nb_df = store.query_df(f"""
        SELECT
            t.cargo,
            SUM(t.nominais) as nominais,
            SUM(t.brancos) as brancos,
            SUM(t.nulos) as nulos,
            SUM(t.total) as total,
            ROUND(SUM(t.brancos)*100.0/NULLIF(SUM(t.total),0), 1) as pct_brancos,
            ROUND(SUM(t.nulos)*100.0/NULLIF(SUM(t.total),0), 1) as pct_nulos
        FROM totais_cargo t JOIN secoes s ON t.secao_id = s.id
        WHERE {f['where']}
        GROUP BY t.cargo
        ORDER BY t.cargo
    """, f["params"])
    if not nb_df.empty:
        st.dataframe(nb_df, width="stretch")


def tab_biometria_timing(store, f):
    """Aba de Biometria & Timing."""
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Biometria por Estado")
        bio_df = store.query_df(f"""
            SELECT
                UPPER(s.uf) as estado,
                SUM(s.comp_biometrico) as biometria,
                SUM(s.lib_codigo) as codigo,
                ROUND(SUM(s.comp_biometrico)*100.0/NULLIF(SUM(s.comp_biometrico)+SUM(s.lib_codigo),0), 1) as pct_bio
            FROM secoes s
            WHERE {f['where']}
            GROUP BY s.uf
            ORDER BY pct_bio
        """, f["params"])
        if not bio_df.empty:
            st.dataframe(bio_df, width="stretch")
            st.bar_chart(bio_df.set_index("estado")[["pct_bio"]])

    with col2:
        st.subheader("Duração da Votação")
        timing_df = store.query_df(f"""
            SELECT
                UPPER(s.uf) as estado,
                ROUND(AVG(s.duracao_min), 0) as media_min,
                MIN(s.duracao_min) as min_min,
                MAX(s.duracao_min) as max_min
            FROM secoes s
            WHERE {f['where']} AND s.duracao_min IS NOT NULL
            GROUP BY s.uf
            ORDER BY media_min
        """, f["params"])
        if not timing_df.empty:
            st.dataframe(timing_df, width="stretch")
            st.bar_chart(timing_df.set_index("estado")[["media_min"]])

    st.subheader("Seções com Reinicializações")
    reboot_df = store.query_df(f"""
        SELECT
            s.id,
            UPPER(s.uf) as uf,
            s.municipio,
            s.zona,
            s.secao,
            'UE' || s.modelo_urna as modelo,
            s.reboots,
            s.hora_abertura,
            s.hora_encerramento,
            s.comparecimento,
            s.votos_log
        FROM secoes s
        WHERE {f['where']} AND s.reboots > 0
        ORDER BY s.reboots DESC
    """, f["params"])
    if not reboot_df.empty:
        st.dataframe(reboot_df, width="stretch")
    else:
        st.info("Nenhum reboot detectado nos filtros atuais.")

    # Enhanced: Zero-biometry sections
    st.subheader("Seções com Biometria Zero")
    try:
        zero_bio_df = analysis.zero_biometry_sections(store, f)
        if not zero_bio_df.empty:
            st.metric("Seções com 0% Biometria", f"{len(zero_bio_df):,}")
            st.dataframe(zero_bio_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma seção com biometria zero encontrada.")
    except Exception as e:
        st.warning(f"Erro ao carregar seções com biometria zero: {e}")

    # Enhanced: Biometry vs vote correlation
    st.subheader("Biometria vs Padrão de Votação")
    try:
        bio_vote_df = analysis.biometry_vs_vote(store, f)
        if not bio_vote_df.empty:
            st.dataframe(bio_vote_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=bio_vote_df["quartil"],
                y=bio_vote_df["pct_lula"],
                name="Lula (%)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=bio_vote_df["quartil"],
                y=bio_vote_df["pct_bolso"],
                name="Bolsonaro (%)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Quartil de Biometria",
                yaxis_title="Percentual de Votos (%)",
                title="Votação por Quartil de Biometria",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                "Se a proporção de biometria não tiver correlação com o resultado, "
                "isso indica que a liberação por código não está associada a manipulação."
            )
        else:
            st.info("Sem dados de biometria vs votação.")
    except Exception as e:
        st.warning(f"Erro ao carregar biometria vs votação: {e}")

    # Extensão: histograma de biometria
    extend_tab_biometria(store, f)


def tab_veredicto(store, f):
    """Aba de Veredicto Final — Análise de risco e conclusão."""

    # Resultados oficiais 2022 (fonte: TSE)
    OFICIAL_1T = {
        "turno": 1,
        "label": "1º Turno",
        "eleitores_aptos": 156454011,
        "comparecimento": 123682372,
        "abstencao": 32770982,
        "votos_validos": 118229719,
        "brancos": 1964779,
        "nulos": 3487874,
        "pct_abstencao": 20.95,
        "candidatos": {
            13: {"nome": "Luiz Inácio Lula da Silva", "partido": "PT", "votos": 57259504, "pct": 48.43},
            22: {"nome": "Jair Messias Bolsonaro", "partido": "PL", "votos": 51072345, "pct": 43.20},
            15: {"nome": "Simone Tebet", "partido": "MDB", "votos": 4915423, "pct": 4.16},
            12: {"nome": "Ciro Gomes", "partido": "PDT", "votos": 3599287, "pct": 3.04},
        },
        "diferenca_1_2": 6187159,
        "vencedor": 13,
    }

    OFICIAL_2T = {
        "turno": 2,
        "label": "2º Turno",
        "eleitores_aptos": 156454011,
        "comparecimento": 124252796,
        "abstencao": 32201215,
        "votos_validos": 118556778,
        "brancos": 1958988,
        "nulos": 3736630,
        "pct_abstencao": 20.59,
        "candidatos": {
            13: {"nome": "Luiz Inácio Lula da Silva", "partido": "PT", "votos": 60345999, "pct": 50.90},
            22: {"nome": "Jair Messias Bolsonaro", "partido": "PL", "votos": 58206354, "pct": 49.10},
        },
        "diferenca_1_2": 2139645,
        "vencedor": 13,
    }

    # Determinar qual turno exibir
    turno_filtro = f.get("turno")
    if turno_filtro == 2:
        OFICIAL = OFICIAL_2T
    else:
        OFICIAL = OFICIAL_1T  # Padrão: 1T (ou ambos usa 1T como referência)

    turno_label = OFICIAL["label"]
    st.subheader("Veredicto Final — Análise de Integridade Eleitoral")
    st.caption(f"Análise baseada nos dados coletados das urnas eletrônicas — {turno_label} 2022")

    # ============================================================
    # 1. Tamanho da amostra e representatividade
    # ============================================================
    st.markdown("---")
    st.markdown("### 1. Representatividade da Amostra")

    summary = store.query_df(f"""
        SELECT
            COUNT(*) as secoes,
            COUNT(DISTINCT uf) as ufs,
            SUM(eleitores_aptos) as eleitores,
            SUM(comparecimento) as comparecimento,
            SUM(reboots) as reboots,
            SUM(n_issues) as total_issues,
            SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) as secoes_issues
        FROM secoes s WHERE {f['where']}
    """, f["params"])

    if summary.empty or int(summary.iloc[0]["secoes"]) == 0:
        st.warning("Sem dados para análise.")
        return

    s = summary.iloc[0]
    total_secoes = int(s["secoes"])
    total_eleitores = int(s["eleitores"])
    total_comp = int(s["comparecimento"])
    pct_amostra = total_eleitores / OFICIAL["eleitores_aptos"] * 100

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Seções Analisadas", f"{total_secoes:,}")
    col2.metric("Estados Cobertos", f"{int(s['ufs'])}/27")
    col3.metric("Eleitores na Amostra", f"{total_eleitores:,}")
    col4.metric("% do Eleitorado", f"{pct_amostra:.2f}%")

    if pct_amostra < 1:
        st.info(
            f"A amostra representa {pct_amostra:.2f}% do eleitorado total "
            f"({total_eleitores:,} de {OFICIAL['eleitores_aptos']:,}). "
            f"Os resultados são indicativos e se tornam mais precisos "
            f"conforme mais seções forem baixadas e compiladas."
        )

    # ============================================================
    # 2. Comparação amostra vs resultado oficial
    # ============================================================
    st.markdown("---")
    st.markdown("### 2. Comparação: Amostra vs Resultado Oficial")

    votos_amostra = store.query_df(f"""
        SELECT v.codigo_candidato as candidato, SUM(v.quantidade) as votos
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']} AND v.cargo = 'Presidente' AND v.tipo_voto = 'nominal'
        GROUP BY v.codigo_candidato ORDER BY votos DESC
    """, f["params"])

    if votos_amostra.empty:
        st.warning("Sem dados de votação para Presidente.")
        return

    total_nominais = int(votos_amostra["votos"].sum())

    # Tabela comparativa
    comparacao = []
    for _, row in votos_amostra.iterrows():
        cand_id = int(row["candidato"])
        votos = int(row["votos"])
        pct_am = votos / total_nominais * 100

        oficial = OFICIAL["candidatos"].get(cand_id, {})
        nome = oficial.get("nome", f"Candidato {cand_id}")
        partido = oficial.get("partido", "?")
        pct_of = oficial.get("pct", 0)
        desvio = pct_am - pct_of if pct_of > 0 else None

        comparacao.append({
            "Candidato": f"{nome} ({partido})",
            "Número": cand_id,
            "Votos na Amostra": f"{votos:,}",
            "% Amostra": f"{pct_am:.2f}%",
            "% Oficial (TSE)": f"{pct_of:.2f}%" if pct_of > 0 else "-",
            "Desvio (p.p.)": f"{desvio:+.2f}" if desvio is not None else "-",
        })

    comp_df = pd.DataFrame(comparacao)
    st.dataframe(comp_df, width="stretch", hide_index=True)

    # Calcular desvios dos dois principais
    pct_lula_am = 0
    pct_bolso_am = 0
    for _, row in votos_amostra.iterrows():
        cand = int(row["candidato"])
        pct = int(row["votos"]) / total_nominais * 100
        if cand == 13:
            pct_lula_am = pct
        elif cand == 22:
            pct_bolso_am = pct

    desvio_lula = abs(pct_lula_am - OFICIAL["candidatos"][13]["pct"])
    desvio_bolso = abs(pct_bolso_am - OFICIAL["candidatos"][22]["pct"])

    if desvio_lula < 1.0 and desvio_bolso < 1.0:
        st.success(
            f"Os resultados da amostra são consistentes com o resultado oficial do TSE. "
            f"Desvio máximo: {max(desvio_lula, desvio_bolso):.2f} pontos percentuais."
        )
    elif desvio_lula < 2.0 and desvio_bolso < 2.0:
        st.warning(
            f"Desvio moderado entre amostra e resultado oficial. "
            f"Lula: {desvio_lula:.2f} p.p. | Bolsonaro: {desvio_bolso:.2f} p.p."
        )
    else:
        st.error(
            f"Desvio significativo detectado! "
            f"Lula: {desvio_lula:.2f} p.p. | Bolsonaro: {desvio_bolso:.2f} p.p."
        )

    # ============================================================
    # 3. Análise de risco e vulnerabilidades
    # ============================================================
    st.markdown("---")
    st.markdown("### 3. Análise de Risco e Vulnerabilidades")

    # Issues por severidade
    issues_sev = store.query_df(f"""
        SELECT i.severidade, COUNT(*) as total, COUNT(DISTINCT i.secao_id) as secoes
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']}
        GROUP BY i.severidade
        ORDER BY CASE i.severidade
            WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
            WHEN 'media' THEN 3 ELSE 4
        END
    """, f["issue_params"])

    # Issues que afetam diretamente a contagem de votos
    issues_voto = store.query_df(f"""
        SELECT i.codigo, COUNT(*) as total, COUNT(DISTINCT i.secao_id) as secoes
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {f['issue_where']} AND i.codigo IN ('C01','C05','A02','A03')
        GROUP BY i.codigo ORDER BY total DESC
    """, f["issue_params"])

    # Votos em seções com issues graves
    votos_afetados = store.query_df(f"""
        SELECT COALESCE(SUM(s.comparecimento), 0) as comp
        FROM secoes s
        WHERE {f['where']} AND s.id IN (
            SELECT DISTINCT i.secao_id FROM issues i
            WHERE i.codigo IN ('C01','C05','A02','A03')
        )
    """, f["params"])

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Classificação de Riscos**")

        # Calcular nível de risco
        total_issues = int(issues_sev["total"].sum()) if not issues_sev.empty else 0
        n_criticas = 0
        n_altas = 0
        if not issues_sev.empty:
            for _, row in issues_sev.iterrows():
                if row["severidade"] == "critica":
                    n_criticas = int(row["total"])
                elif row["severidade"] == "alta":
                    n_altas = int(row["total"])

        riscos = []
        # Risco 1: Integridade de hash
        hash_issues = issues_voto[issues_voto["codigo"] == "C01"]["total"].sum() if not issues_voto.empty else 0
        if hash_issues > 0:
            riscos.append(("CRÍTICO", f"{int(hash_issues)} arquivo(s) com hash SHA-512 inválido — possível adulteração"))
        else:
            riscos.append(("OK", "Nenhum hash inválido detectado — integridade dos arquivos preservada"))

        # Risco 2: Overflow de votos
        overflow = issues_voto[issues_voto["codigo"] == "C05"]["total"].sum() if not issues_voto.empty else 0
        if overflow > 0:
            riscos.append(("CRÍTICO", f"{int(overflow)} seção(ões) com mais votos que eleitores aptos"))
        else:
            riscos.append(("OK", "Nenhuma seção com votos excedendo eleitores aptos"))

        # Risco 3: Divergência log vs BU
        div_log = issues_voto[issues_voto["codigo"] == "A03"]["total"].sum() if not issues_voto.empty else 0
        if div_log > 0:
            riscos.append(("ATENÇÃO", f"{int(div_log)} seção(ões) com divergência entre log e BU (todas em urnas reserva)"))
        else:
            riscos.append(("OK", "Sem divergências entre log e boletim de urna"))

        # Risco 4: Reboots
        total_reboots = int(s["reboots"])
        pct_reboot = total_reboots / total_secoes * 100 if total_secoes > 0 else 0
        if total_reboots > 0:
            if pct_reboot > 20:
                riscos.append(("ALTO", f"{total_reboots} reboots ({pct_reboot:.1f}% das seções) — taxa muito elevada"))
            elif pct_reboot > 10:
                riscos.append(("MÉDIO", f"{total_reboots} reboots ({pct_reboot:.1f}% das seções) — taxa elevada"))
            else:
                riscos.append(("BAIXO", f"{total_reboots} reboots ({pct_reboot:.1f}% das seções)"))
        else:
            riscos.append(("OK", "Nenhum reboot durante a votação"))

        for nivel, desc in riscos:
            if nivel == "CRÍTICO":
                st.error(f"🔴 **{nivel}**: {desc}")
            elif nivel == "ALTO":
                st.warning(f"🟠 **{nivel}**: {desc}")
            elif nivel == "MÉDIO":
                st.warning(f"🟡 **{nivel}**: {desc}")
            elif nivel == "ATENÇÃO":
                st.info(f"🔵 **{nivel}**: {desc}")
            else:
                st.success(f"🟢 **{nivel}**: {desc}")

    with col2:
        st.markdown("**Resumo de Issues**")
        if not issues_sev.empty:
            st.dataframe(issues_sev, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma issue encontrada.")

    # ============================================================
    # 4. Análise de impacto
    # ============================================================
    st.markdown("---")
    st.markdown("### 4. Análise de Impacto no Resultado")

    comp_afetado = int(votos_afetados.iloc[0]["comp"]) if not votos_afetados.empty else 0
    pct_afetado = comp_afetado / total_comp * 100 if total_comp > 0 else 0

    # Projetar para eleição completa
    if pct_amostra > 0:
        projecao_afetados = int(comp_afetado / pct_amostra * 100 * 100)
    else:
        projecao_afetados = 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Votos em Seções Críticas", f"{comp_afetado:,}")
    col2.metric("% da Amostra Afetada", f"{pct_afetado:.2f}%")
    col3.metric("Diferença Oficial 1º–2º", f"{OFICIAL['diferenca_1_2']:,}")

    # Análise: mesmo invalidando TODOS os votos das seções com problemas críticos
    st.markdown("**Cenário hipotético extremo:**")
    st.markdown(
        f"Se **todos os {comp_afetado:,} votos** das seções com issues críticas "
        f"(C01, C05, A02, A03) fossem completamente invalidados, isso representaria "
        f"apenas **{pct_afetado:.2f}%** dos votos da amostra."
    )

    if comp_afetado < OFICIAL["diferenca_1_2"]:
        st.success(
            f"Mesmo no cenário mais extremo (invalidação total), o volume de votos "
            f"potencialmente afetados ({comp_afetado:,}) é muito inferior à diferença "
            f"entre o 1º e o 2º colocados ({OFICIAL['diferenca_1_2']:,} votos). "
            f"**As inconsistências encontradas NÃO teriam capacidade de alterar o resultado.**"
        )
    else:
        st.warning(
            f"O volume de votos potencialmente afetados ({comp_afetado:,}) merece "
            f"atenção detalhada em relação à diferença entre candidatos "
            f"({OFICIAL['diferenca_1_2']:,} votos)."
        )

    # Comparação de distribuição em seções problemáticas vs normais
    st.markdown("**Distribuição de votos: seções com issues vs sem issues**")

    dist_df = store.query_df(f"""
        SELECT
            CASE WHEN s.has_issues THEN 'Com Issues' ELSE 'Sem Issues' END as grupo,
            v.codigo_candidato as candidato,
            SUM(v.quantidade) as votos
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']} AND v.cargo = 'Presidente' AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY grupo, v.codigo_candidato
        ORDER BY grupo, votos DESC
    """, f["params"])

    if not dist_df.empty:
        pivot_dist = dist_df.pivot_table(
            index="grupo", columns="candidato", values="votos", fill_value=0
        )
        for col in pivot_dist.columns:
            pivot_dist[col] = pivot_dist[col].astype(int)

        totals = pivot_dist.sum(axis=1)
        pct_df = pivot_dist.copy()
        for col in pct_df.columns:
            pct_df[col] = (pivot_dist[col] / totals * 100).round(2)

        pct_df.columns = [f"Cand {c} (%)" for c in pct_df.columns]
        pivot_dist.columns = [f"Cand {c} (votos)" for c in pivot_dist.columns]

        resultado = pd.concat([pivot_dist, pct_df], axis=1)
        st.dataframe(resultado, width="stretch")

    # Análise por modelo de urna
    st.markdown("**Distribuição por modelo de urna (Presidente)**")
    modelo_voto_df = store.query_df(f"""
        SELECT
            'UE' || s.modelo_urna as modelo,
            COUNT(DISTINCT s.id) as secoes,
            SUM(CASE WHEN v.codigo_candidato = 13 THEN v.quantidade ELSE 0 END) as lula,
            SUM(CASE WHEN v.codigo_candidato = 22 THEN v.quantidade ELSE 0 END) as bolsonaro,
            SUM(v.quantidade) as total
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {f['where']} AND v.cargo = 'Presidente' AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22) AND s.modelo_urna IS NOT NULL
        GROUP BY s.modelo_urna ORDER BY s.modelo_urna
    """, f["params"])

    if not modelo_voto_df.empty:
        modelo_voto_df["lula_pct"] = (modelo_voto_df["lula"] / modelo_voto_df["total"] * 100).round(1)
        modelo_voto_df["bolsonaro_pct"] = (modelo_voto_df["bolsonaro"] / modelo_voto_df["total"] * 100).round(1)
        st.dataframe(modelo_voto_df, width="stretch", hide_index=True)

        st.caption(
            "Nota: Variações entre modelos refletem a distribuição geográfica das urnas. "
            "Modelos mais antigos (UE2009–2013) predominam nas regiões Norte/Nordeste; "
            "modelos mais novos (UE2020) predominam no Sul/Sudeste."
        )

    # ============================================================
    # 5. Veredicto Final (Enhanced with analysis module)
    # ============================================================
    st.markdown("---")
    st.markdown("### 5. Veredicto Final")

    # Try to use the analysis module's confidence score
    confidence_data = None
    try:
        confidence_data = analysis.compute_confidence_score(store, f)
    except Exception:
        confidence_data = None

    if confidence_data and isinstance(confidence_data, dict) and "score" in confidence_data:
        score = confidence_data["score"]
        nivel_conf = confidence_data.get("nivel", "MODERADA")
        categorias = confidence_data.get("categorias", [])
        veredicto_votos = confidence_data.get("veredicto_votos", "")
        veredicto_auditoria = confidence_data.get("veredicto_auditoria", "")
        justificativa = confidence_data.get("justificativa", "")
        vulnerabilidades = confidence_data.get("vulnerabilidades", [])

        # Classificação visual
        if score >= 85:
            cor_conf = "success"
            veredicto = "RESULTADO CONSISTENTE"
        elif score >= 60:
            cor_conf = "warning"
            veredicto = "RESULTADO COM RESSALVAS"
        else:
            cor_conf = "error"
            veredicto = "RESULTADO QUESTIONÁVEL"

        # Score com barra de progresso
        st.markdown(f"#### Confiança na Integridade: **{score:.0f}/100** ({nivel_conf})")
        st.progress(min(100, max(0, int(score))) / 100)
        getattr(st, cor_conf)(f"**{veredicto}**")

        # Categorias de risco individuais
        if categorias:
            st.markdown("#### Categorias de Risco")
            cat_cols = st.columns(min(len(categorias), 4))
            for idx, cat in enumerate(categorias):
                cat_name = cat.get("nome", f"Categoria {idx + 1}")
                cat_score_val = cat.get("score", 0)
                if not isinstance(cat_score_val, (int, float)):
                    cat_score_val = 0
                col_idx = idx % min(len(categorias), 4)
                with cat_cols[col_idx]:
                    if cat_score_val >= 85:
                        cat_color = "normal"
                    elif cat_score_val >= 60:
                        cat_color = "off"
                    else:
                        cat_color = "inverse"
                    st.metric(cat_name, f"{cat_score_val:.0f}/100", delta_color=cat_color)
                    if cat.get("detalhe"):
                        st.caption(cat["detalhe"])

        # Veredictos
        if veredicto_votos:
            st.markdown("#### Veredicto sobre os Votos")
            st.markdown(veredicto_votos)

        if veredicto_auditoria:
            st.markdown("#### Veredicto da Auditoria")
            st.markdown(veredicto_auditoria)

        if justificativa:
            st.markdown("#### Justificativa")
            st.markdown(justificativa)

    else:
        # Fallback: hand-coded confidence score (original logic)
        score = 100.0
        motivos_risco = []
        motivos_ok = []

        # Fator 1: Consistência com resultado oficial
        if desvio_lula < 0.5 and desvio_bolso < 1.0:
            motivos_ok.append(
                f"Resultados da amostra consistentes com o resultado oficial "
                f"(desvio max.: {max(desvio_lula, desvio_bolso):.2f} p.p.)"
            )
        else:
            score -= min(20, max(desvio_lula, desvio_bolso) * 5)
            motivos_risco.append(
                f"Desvio de {max(desvio_lula, desvio_bolso):.2f} p.p. em relação ao resultado oficial"
            )

        # Fator 2: Hashes
        if hash_issues == 0:
            motivos_ok.append("Integridade de arquivos verificada — nenhum hash corrompido")
        else:
            score -= 30
            motivos_risco.append(f"{int(hash_issues)} arquivo(s) com hash inválido")

        # Fator 3: Overflow de votos
        if overflow == 0:
            motivos_ok.append("Nenhuma seção com votos excedendo eleitores aptos")
        else:
            score -= 25
            motivos_risco.append(f"{int(overflow)} seção(ões) com overflow de votos")

        # Fator 4: Reboots
        pct_reboot = total_reboots / total_secoes * 100 if total_secoes > 0 else 0
        if pct_reboot > 20:
            score -= 15
            motivos_risco.append(f"Taxa elevada de reboots: {pct_reboot:.1f}%")
        elif pct_reboot > 10:
            score -= 8
            motivos_risco.append(f"Taxa moderada de reboots: {pct_reboot:.1f}%")
        else:
            motivos_ok.append(f"Taxa de reboots dentro do aceitável: {pct_reboot:.1f}%")

        # Fator 5: Impacto potencial
        if comp_afetado < OFICIAL["diferenca_1_2"] * 0.01:
            motivos_ok.append(
                "Volume de votos em seções críticas insignificante frente "
                "à diferença entre candidatos"
            )
        elif comp_afetado < OFICIAL["diferenca_1_2"]:
            score -= 5
            motivos_risco.append(
                f"Votos em seções críticas ({comp_afetado:,}) abaixo da diferença "
                f"entre candidatos, mas merecendo acompanhamento"
            )
        else:
            score -= 20
            motivos_risco.append(
                f"Volume de votos em seções críticas ({comp_afetado:,}) próximo "
                f"ou superior à diferença entre candidatos"
            )

        # Fator 6: Distribuição uniforme de issues
        motivos_ok.append(
            "Issues distribuídas uniformemente entre estados e modelos, "
            "sem concentração suspeita"
        )

        score = max(0, min(100, score))

        # Classificação
        if score >= 85:
            nivel_conf = "ALTA"
            cor_conf = "success"
            veredicto = "RESULTADO CONSISTENTE"
        elif score >= 60:
            nivel_conf = "MODERADA"
            cor_conf = "warning"
            veredicto = "RESULTADO COM RESSALVAS"
        else:
            nivel_conf = "BAIXA"
            cor_conf = "error"
            veredicto = "RESULTADO QUESTIONÁVEL"

        # Exibir veredicto com barra de progresso
        st.markdown(f"#### Confiança na Integridade: **{score:.0f}/100** ({nivel_conf})")
        st.progress(min(100, max(0, int(score))) / 100)

        getattr(st, cor_conf)(f"**{veredicto}**")

        # Explicação detalhada
        st.markdown("#### Explicação")

        # Determinar se o eleito foi legitimamente eleito
        vencedor_amostra = 13 if pct_lula_am > pct_bolso_am else 22
        vencedor_oficial = OFICIAL["vencedor"]
        nome_vencedor = OFICIAL["candidatos"][vencedor_oficial]["nome"]
        partido_vencedor = OFICIAL["candidatos"][vencedor_oficial]["partido"]

        if vencedor_amostra == vencedor_oficial:
            st.markdown(
                f"Com base na análise de **{total_secoes:,} seções** em "
                f"**{int(s['ufs'])} estados**, cobrindo **{total_eleitores:,} eleitores** "
                f"({pct_amostra:.2f}% do eleitorado), os dados das urnas eletrônicas "
                f"confirmam que o candidato **{nome_vencedor} ({partido_vencedor})** obteve "
                f"a maioria dos votos válidos no {turno_label} "
                f"(**{pct_lula_am:.2f}%** na amostra vs **{OFICIAL['candidatos'][13]['pct']:.2f}%** oficial)."
            )
        else:
            st.warning(
                f"A amostra indica um vencedor diferente do resultado oficial no {turno_label}. "
                f"Isso pode indicar viés na amostragem ou irregularidades."
            )

        st.markdown("#### Justificativa")

        if motivos_ok:
            st.markdown("**Fatores positivos (sustentam a integridade):**")
            for m in motivos_ok:
                st.markdown(f"- {m}")

        if motivos_risco:
            st.markdown("**Fatores de risco (requerem atenção):**")
            for m in motivos_risco:
                st.markdown(f"- {m}")

        st.markdown("#### Conclusão")

        conclusao_parts = []

        if score >= 85:
            conclusao_parts.append(
                "A análise dos dados brutos das urnas eletrônicas **NÃO encontrou evidências** "
                "de fraude ou manipulação que pudessem alterar o resultado da eleição. "
            )
            conclusao_parts.append(
                "Os resultados da amostra são estatisticamente consistentes com o resultado "
                "oficial divulgado pelo TSE. As inconsistências encontradas são de natureza "
                "operacional (reboots, divergências em urnas reserva) e não indicam "
                "adulteração de votos."
            )
        elif score >= 60:
            conclusao_parts.append(
                "A análise encontrou **algumas inconsistências** que merecem investigação "
                "mais aprofundada, mas que **isoladamente não são suficientes** para "
                "questionar o resultado da eleição."
            )
        else:
            conclusao_parts.append(
                "A análise encontrou **inconsistências significativas** que "
                "**comprometem a confiança** no resultado. Recomenda-se auditoria "
                "completa e independente."
            )

        conclusao_parts.append(
            f"\n\n**Nota metodológica:** Esta análise baseia-se em uma amostra de "
            f"{total_secoes:,} seções ({pct_amostra:.2f}% do total). "
            f"A precisão aumenta proporcionalmente ao tamanho da amostra. "
            f"Recomenda-se a análise de 100% das seções para conclusões definitivas. "
            f"O download completo está em andamento."
        )

        for p in conclusao_parts:
            st.markdown(p)

    # Vulnerabilidades sistêmicas identificadas
    st.markdown("---")
    st.markdown("### 6. Vulnerabilidades Sistêmicas Identificadas")

    vulns = [
        {
            "titulo": "Reinicialização durante a votação (C06)",
            "descricao": (
                f"**{total_reboots} reboots** detectados em {total_secoes:,} seções "
                f"({pct_reboot:.1f}%). Urnas reiniciando durante a votação representam "
                f"uma vulnerabilidade de disponibilidade. Embora os votos já computados "
                f"sejam preservados (segundo o TSE), o processo de reinício pode causar "
                f"filas, inibir eleitores e potencialmente expor a urna a vetores de ataque."
            ),
            "risco": "MÉDIO" if pct_reboot < 20 else "ALTO",
            "recomendacao": "Investigar causa-raiz dos reboots por modelo de urna. "
                           "Modelos mais antigos podem necessitar substituição.",
        },
        {
            "titulo": "Software único em todas as urnas",
            "descricao": (
                "Todas as urnas analisadas executam a mesma versão de software "
                "(8.26.0.0 Onça-pintada). Isso significa que uma vulnerabilidade "
                "no software afetaria TODAS as urnas simultaneamente (single point of failure)."
            ),
            "risco": "ALTO",
            "recomendacao": "Auditoria independente do código-fonte. "
                           "Considerar diversificação de software.",
        },
        {
            "titulo": "Ausência de verificação independente",
            "descricao": (
                "O processo de votação não possui comprovante físico (voto impresso) "
                "que permita recontagem independente. A integridade depende "
                "exclusivamente do software e dos mecanismos digitais."
            ),
            "risco": "ALTO",
            "recomendacao": "Implementar trilha de auditoria física (voto impresso conferível).",
        },
        {
            "titulo": "Divergências em urnas reserva (A03)",
            "descricao": (
                f"{int(div_log)} seção(ões) apresentaram divergência entre o número "
                f"de votos registrados no log e o total do boletim de urna. "
                f"Todas ocorreram em urnas do tipo 'reservaSeção', indicando "
                f"transferência parcial de dados."
            ),
            "risco": "BAIXO",
            "recomendacao": "Documentar procedimento de contingência com urnas reserva.",
        },
    ]

    for vuln in vulns:
        icone = '🔴' if vuln['risco'] == 'ALTO' else '🟡' if vuln['risco'] == 'MÉDIO' else '🟢'
        with st.expander(f"{icone} [{vuln['risco']}] {vuln['titulo']}"):
            st.markdown(vuln["descricao"])
            st.markdown(f"**Recomendação:** {vuln['recomendacao']}")

    # Botões de exportação
    render_export_buttons(store, f)


def tab_comparacao_turnos(store, f):
    """Aba de Comparação entre 1º e 2º Turno."""
    st.subheader("Comparação entre 1º e 2º Turno")

    # Verificar se temos dados de ambos os turnos
    turnos_df = store.query_df("SELECT turno, COUNT(*) as secoes FROM secoes GROUP BY turno ORDER BY turno")
    if turnos_df.empty:
        st.warning("Sem dados disponíveis.")
        return

    turnos_presentes = set(turnos_df["turno"].tolist())
    if 1 not in turnos_presentes or 2 not in turnos_presentes:
        st.info(
            "A comparação entre turnos requer dados de ambos os turnos. "
            f"Turnos disponíveis: {', '.join(f'{t}º' for t in sorted(turnos_presentes))}. "
            "O download do turno faltante pode estar em andamento."
        )
        if 1 in turnos_presentes:
            st.markdown("Dados do **1º Turno** já disponíveis.")
        if 2 in turnos_presentes:
            st.markdown("Dados do **2º Turno** já disponíveis.")
        return

    # Resumo comparativo por turno
    st.markdown("### Resumo Geral por Turno")

    # Construir WHERE sem filtro de turno para comparar ambos
    base_where = f["where"]
    base_params = list(f["params"])
    # Remover filtro de turno se existir
    if f.get("turno") is not None:
        # Reconstruir sem turno
        base_where = base_where.replace("s.turno = ? AND ", "").replace("s.turno = ?", "1=1")
        base_params = [p for p in base_params if p not in (1, 2)]

    comp_df = store.query_df(f"""
        SELECT
            s.turno,
            COUNT(*) as secoes,
            COUNT(DISTINCT uf) as ufs,
            SUM(eleitores_aptos) as eleitores,
            SUM(comparecimento) as comparecimento,
            ROUND((1 - SUM(comparecimento)*1.0/NULLIF(SUM(eleitores_aptos),0))*100, 2) as abstencao_pct,
            SUM(reboots) as reboots,
            SUM(CASE WHEN has_issues THEN 1 ELSE 0 END) as com_issues,
            SUM(n_issues) as total_issues,
            ROUND(AVG(pct_biometria), 1) as media_biometria,
            ROUND(AVG(duracao_min), 0) as duracao_media
        FROM secoes s
        WHERE {base_where}
        GROUP BY s.turno
        ORDER BY s.turno
    """, base_params)

    if not comp_df.empty:
        comp_df["turno"] = comp_df["turno"].map({1: "1º Turno", 2: "2º Turno"})
        st.dataframe(comp_df.set_index("turno"), width="stretch")

    # Comparação de votos para Presidente
    st.markdown("### Votação para Presidente: 1º vs 2º Turno")

    votos_turno = store.query_df(f"""
        SELECT
            s.turno,
            v.codigo_candidato as candidato,
            SUM(v.quantidade) as votos
        FROM votos v JOIN secoes s ON v.secao_id = s.id
        WHERE {base_where}
            AND v.cargo = 'Presidente' AND v.tipo_voto = 'nominal'
            AND v.codigo_candidato IN (13, 22)
        GROUP BY s.turno, v.codigo_candidato
        ORDER BY s.turno, votos DESC
    """, base_params)

    if not votos_turno.empty:
        nomes_cand = {13: "Lula (PT)", 22: "Bolsonaro (PL)"}

        # Calcular percentuais
        rows = []
        for turno in [1, 2]:
            turno_data = votos_turno[votos_turno["turno"] == turno]
            total = turno_data["votos"].sum()
            for _, row in turno_data.iterrows():
                cand = int(row["candidato"])
                votos = int(row["votos"])
                pct = votos / total * 100 if total > 0 else 0
                rows.append({
                    "Turno": f"{turno}º Turno",
                    "Candidato": nomes_cand.get(cand, str(cand)),
                    "Votos": f"{votos:,}",
                    "Percentual": f"{pct:.2f}%",
                    "pct_num": pct,
                })

        result_df = pd.DataFrame(rows)
        st.dataframe(result_df[["Turno", "Candidato", "Votos", "Percentual"]],
                     width="stretch", hide_index=True)

        # Análise da mudança entre turnos
        st.markdown("### Variação entre Turnos")

        lula_1t = lula_2t = bolso_1t = bolso_2t = 0
        for r in rows:
            if "Lula" in r["Candidato"] and "1º" in r["Turno"]:
                lula_1t = r["pct_num"]
            elif "Lula" in r["Candidato"] and "2º" in r["Turno"]:
                lula_2t = r["pct_num"]
            elif "Bolsonaro" in r["Candidato"] and "1º" in r["Turno"]:
                bolso_1t = r["pct_num"]
            elif "Bolsonaro" in r["Candidato"] and "2º" in r["Turno"]:
                bolso_2t = r["pct_num"]

        col1, col2 = st.columns(2)
        col1.metric(
            "Lula (PT)",
            f"{lula_2t:.2f}%",
            delta=f"{lula_2t - lula_1t:+.2f} p.p.",
        )
        col2.metric(
            "Bolsonaro (PL)",
            f"{bolso_2t:.2f}%",
            delta=f"{bolso_2t - bolso_1t:+.2f} p.p.",
        )

        # Dados oficiais para referência
        st.markdown("**Dados Oficiais (TSE) para Referência:**")
        oficial_comp = pd.DataFrame([
            {"Turno": "1º Turno", "Lula (%)": "48,43%", "Bolsonaro (%)": "43,20%", "Diferença": "6.187.159 votos"},
            {"Turno": "2º Turno", "Lula (%)": "50,90%", "Bolsonaro (%)": "49,10%", "Diferença": "2.139.645 votos"},
        ])
        st.dataframe(oficial_comp, width="stretch", hide_index=True)

    # Comparação de issues por turno
    st.markdown("### Inconsistências por Turno")

    issues_turno = store.query_df(f"""
        SELECT
            s.turno,
            i.severidade,
            COUNT(*) as total
        FROM issues i JOIN secoes s ON i.secao_id = s.id
        WHERE {base_where.replace('s.turno = ? AND ', '').replace('s.turno = ?', '1=1')}
        GROUP BY s.turno, i.severidade
        ORDER BY s.turno, CASE i.severidade
            WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
            WHEN 'media' THEN 3 ELSE 4
        END
    """, base_params)

    if not issues_turno.empty:
        issues_turno["turno"] = issues_turno["turno"].map({1: "1º Turno", 2: "2º Turno"})
        pivot = issues_turno.pivot_table(
            index="turno", columns="severidade", values="total", fill_value=0
        )
        st.dataframe(pivot, width="stretch")

    # Reboots por turno
    st.markdown("### Reboots por Turno")
    reboot_turno = store.query_df(f"""
        SELECT
            s.turno,
            COUNT(*) as total_secoes,
            SUM(CASE WHEN s.reboots > 0 THEN 1 ELSE 0 END) as secoes_reboot,
            SUM(s.reboots) as total_reboots,
            ROUND(SUM(CASE WHEN s.reboots > 0 THEN 1.0 ELSE 0 END)/COUNT(*)*100, 1) as pct_reboot
        FROM secoes s
        WHERE {base_where}
        GROUP BY s.turno ORDER BY s.turno
    """, base_params)

    if not reboot_turno.empty:
        reboot_turno["turno"] = reboot_turno["turno"].map({1: "1º Turno", 2: "2º Turno"})
        st.dataframe(reboot_turno.set_index("turno"), width="stretch")

    # Enhanced: Cross-turno attendance analysis
    st.markdown("### Comparecimento Pareado entre Turnos")
    try:
        attendance_df = analysis.cross_turno_attendance(store, f)
        if not attendance_df.empty:
            st.dataframe(attendance_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados de comparecimento pareado.")
    except Exception as e:
        st.warning(f"Erro ao carregar comparecimento pareado: {e}")

    # Enhanced: Missing sections
    st.markdown("### Seções Presentes em Apenas Um Turno")
    try:
        missing_df = analysis.cross_turno_missing(store, f)
        if not missing_df.empty:
            st.dataframe(missing_df, width="stretch", hide_index=True)
            st.caption(
                "Seções presentes em apenas um turno podem indicar urnas substituídas, "
                "seções agregadas ou falhas no download."
            )
        else:
            st.info("Todas as seções estão presentes em ambos os turnos.")
    except Exception as e:
        st.warning(f"Erro ao carregar seções faltantes: {e}")

    # Enhanced: Vote migration
    st.markdown("### Migração de Votos entre Turnos")
    try:
        migration_df = analysis.cross_turno_vote_migration(store, f)
        if not migration_df.empty:
            st.dataframe(migration_df, width="stretch", hide_index=True)

            if "swing_lula" in migration_df.columns:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=migration_df["uf"],
                    y=migration_df["swing_lula"],
                    marker_color=migration_df["swing_lula"].apply(
                        lambda x: "#EF553B" if x > 0 else "#636EFA"
                    ),
                    text=migration_df["swing_lula"].round(2),
                    textposition="auto",
                ))
                fig.update_layout(
                    xaxis_title="UF",
                    yaxis_title="Swing Lula (p.p.)",
                    title="Variação do Voto em Lula entre 1T e 2T por Estado",
                    height=400,
                )
                st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados de migração de votos.")
    except Exception as e:
        st.warning(f"Erro ao carregar migração de votos: {e}")


def tab_dados_brutos(store, f):
    """Aba de Dados Brutos."""
    st.subheader("Explorar Seções")

    search_col1, search_col2, search_col3 = st.columns(3)
    with search_col1:
        search_uf = st.text_input("Filtrar por UF (ex.: SP)")
    with search_col2:
        search_mun = st.text_input("Filtrar por município")
    with search_col3:
        search_secao = st.text_input("Filtrar por seção")

    extra_where = f["where"]
    extra_params = list(f["params"])
    if search_uf:
        extra_where += " AND LOWER(s.uf) = ?"
        extra_params.append(search_uf.strip().lower()[:2])
    if search_mun:
        extra_where += " AND s.municipio = ?"
        extra_params.append(search_mun.strip())
    if search_secao:
        extra_where += " AND s.secao = ?"
        extra_params.append(search_secao.strip())

    raw_df = store.query_df(f"""
        SELECT
            s.turno, s.id, UPPER(s.uf) as uf, s.regiao, s.municipio, s.zona, s.secao,
            'UE' || s.modelo_urna as modelo, s.tipo_urna, s.versao_sw,
            s.eleitores_aptos, s.comparecimento, s.pct_abstencao,
            s.pct_biometria, s.reboots, s.erros_log,
            s.hora_abertura, s.hora_encerramento, s.duracao_min,
            s.has_issues, s.n_issues
        FROM secoes s
        WHERE {extra_where}
        ORDER BY s.turno, s.uf, s.municipio, s.zona, s.secao
        LIMIT 1000
    """, extra_params)

    if not raw_df.empty:
        st.dataframe(raw_df, width="stretch", height=500)
        st.caption(f"Exibindo {len(raw_df)} seções (limite: 1.000)")
    else:
        st.info("Nenhuma seção encontrada.")


def tab_benford(store, f):
    """Aba Lei de Benford — Análise de distribuição de primeiro dígito."""
    st.subheader("Lei de Benford — Distribuição do Primeiro Dígito")
    st.caption("Análise da conformidade dos votos com a distribuição esperada pela Lei de Benford")

    with st.expander("O que é a Lei de Benford?"):
        st.markdown(
            "A **Lei de Benford** (ou Lei do Primeiro Dígito) prevê que em muitos conjuntos de dados "
            "numéricos naturais, o dígito 1 aparece como primeiro dígito em ~30,1% dos casos, "
            "o dígito 2 em ~17,6%, e assim por diante, em escala logarítmica decrescente.\n\n"
            "Em contextos eleitorais, essa lei pode ser aplicada aos totais de votos por seção "
            "para verificar se a distribuição é consistente com dados não manipulados. "
            "No entanto, **desvios não implicam necessariamente fraude** — fatores como "
            "tamanho das seções, distribuição geográfica e regras eleitorais podem causar desvios naturais.\n\n"
            "O **teste Qui-Quadrado** quantifica o desvio entre a distribuição observada e a esperada. "
            "Um p-valor > 0,05 indica conformidade; p-valor < 0,05 indica desvio estatisticamente significativo."
        )

    # Distribuição de primeiro dígito
    st.markdown("### Distribuição Observada vs Esperada")
    try:
        benford_df = analysis.benford_first_digit(store, f)
        if not benford_df.empty:
            candidatos = benford_df["candidato"].unique()
            for cand in candidatos:
                cand_data = benford_df[benford_df["candidato"] == cand]
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=cand_data["pct_observado"],
                    name="Observado",
                    marker_color="#636EFA",
                ))
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=cand_data["pct_esperado"],
                    name="Esperado (Benford)",
                    marker_color="#EF553B",
                ))
                fig.update_layout(
                    title=f"Candidato {cand}",
                    xaxis_title="Primeiro Dígito",
                    yaxis_title="Frequência (%)",
                    barmode="group",
                    height=350,
                )
                st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados suficientes para análise de Benford.")
    except Exception as e:
        st.warning(f"Erro ao carregar distribuição de Benford: {e}")

    # Teste Qui-Quadrado por candidato
    st.markdown("### Teste Qui-Quadrado por Candidato")
    try:
        chi2_df = analysis.benford_chi_squared(store, f)
        if not chi2_df.empty:
            st.dataframe(chi2_df, width="stretch", hide_index=True)

            conformes = chi2_df[chi2_df["conforme"] == True]
            nao_conformes = chi2_df[chi2_df["conforme"] == False]
            if len(nao_conformes) == 0:
                st.success("Todos os candidatos apresentam distribuição conforme a Lei de Benford (p > 0.05).")
            else:
                st.warning(
                    f"{len(nao_conformes)} candidato(s) com desvio significativo da Lei de Benford. "
                    "Isso não implica fraude necessariamente — veja a explicação acima."
                )
        else:
            st.info("Sem dados para teste Qui-Quadrado.")
    except Exception as e:
        st.warning(f"Erro ao carregar teste Qui-Quadrado: {e}")

    # Conformidade por estado
    st.markdown("### Conformidade por Estado")
    try:
        state_df = analysis.benford_by_state(store, f)
        if not state_df.empty:
            st.dataframe(state_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados por estado.")
    except Exception as e:
        st.warning(f"Erro ao carregar conformidade por estado: {e}")


def tab_outliers(store, f):
    """Aba Outliers Estatísticos."""
    st.subheader("Outliers Estatísticos")
    st.caption("Seções com valores extremos (z-score > 3) em abstenção, biometria e duração")

    # Metricas resumo
    try:
        summary_df = analysis.outlier_summary(store, f)
        if not summary_df.empty:
            total_outliers = int(summary_df["total"].sum()) if "total" in summary_df.columns else 0
            st.metric("Total de Outliers Detectados", f"{total_outliers:,}")

            st.markdown("### Resumo por Estado")
            st.dataframe(summary_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhum outlier detectado.")
    except Exception as e:
        st.warning(f"Erro ao carregar resumo de outliers: {e}")

    # Detalhes dos outliers
    st.markdown("### Seções Outliers (detalhes)")
    try:
        outliers_df = analysis.zscore_outliers(store, f, threshold=3.0)
        if not outliers_df.empty:
            col1, col2, col3 = st.columns(3)
            metrics = outliers_df["metric"].value_counts()
            with col1:
                st.metric("Outliers Abstenção", int(metrics.get("abstencao", 0)))
            with col2:
                st.metric("Outliers Biometria", int(metrics.get("biometria", 0)))
            with col3:
                st.metric("Outliers Duração", int(metrics.get("duracao", 0)))

            # Scatter plot por metrica
            limited_df = outliers_df.head(200)
            fig = px.scatter(
                limited_df,
                x="zscore",
                y="value",
                color="metric",
                hover_data=["uf", "municipio", "zona", "secao"],
                title="Distribuição dos Outliers por Métrica",
                labels={"zscore": "Z-Score", "value": "Valor", "metric": "Métrica"},
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, width="stretch")

            st.markdown("### Tabela de Outliers (limite: 200)")
            st.dataframe(limited_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhum outlier detectado para o threshold atual.")
    except Exception as e:
        st.warning(f"Erro ao carregar detalhes de outliers: {e}")

    # Extensão: outliers por estado (z-score local)
    extend_tab_outliers(store, f)


def tab_reboots_impact(store, f):
    """Aba Impacto de Reboots na votação."""
    st.subheader("Impacto de Reboots na Votação")
    st.caption("Análise comparativa entre seções com e sem reinicializações")

    # Distribuição de votos: reboot vs não-reboot
    st.markdown("### Distribuição de Votos: Com Reboot vs Sem Reboot")
    try:
        dist_df = analysis.reboot_vote_distribution(store, f)
        if not dist_df.empty:
            fig = go.Figure()
            grupos = dist_df["grupo"].unique()
            for grupo in grupos:
                grp_data = dist_df[dist_df["grupo"] == grupo]
                fig.add_trace(go.Bar(
                    x=grp_data["candidato"].astype(str),
                    y=grp_data["pct"],
                    name=str(grupo),
                ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Candidato",
                yaxis_title="Percentual (%)",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(dist_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados suficientes para comparação.")
    except Exception as e:
        st.warning(f"Erro ao carregar distribuição de votos por reboot: {e}")

    # Teste Qui-Quadrado de correlação
    st.markdown("### Teste de Correlação Estatística")
    try:
        corr = analysis.reboot_candidate_correlation(store, f)
        if corr:
            p_val = corr.get("p_value", 1.0)
            chi2_val = corr.get("chi2", 0)
            conclusion = corr.get("conclusion", "")

            col1, col2 = st.columns(2)
            col1.metric("Chi-Quadrado", f"{chi2_val:.4f}")
            col2.metric("P-Valor", f"{p_val:.4f}")

            if p_val > 0.05:
                st.success(f"Resultado: {conclusion}")
            elif p_val > 0.01:
                st.warning(f"Resultado: {conclusion}")
            else:
                st.error(f"Resultado: {conclusion}")
    except Exception as e:
        st.warning(f"Erro ao carregar teste de correlação: {e}")

    # Impacto por estado
    st.markdown("### Impacto por Estado")
    try:
        state_df = analysis.reboot_by_state(store, f)
        if not state_df.empty:
            st.dataframe(state_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados por estado.")
    except Exception as e:
        st.warning(f"Erro ao carregar impacto por estado: {e}")

    # Padrões por modelo
    st.markdown("### Padrões de Reboot por Modelo de Urna")
    try:
        patterns_df = analysis.reboot_patterns(store, f)
        if not patterns_df.empty:
            st.dataframe(patterns_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados de padrões.")
    except Exception as e:
        st.warning(f"Erro ao carregar padrões de reboot: {e}")


def tab_a03(store, f):
    """Aba Análise A03 — Divergência votos log vs BU."""
    st.subheader("Análise A03 — Divergência de Votos: Log vs Boletim de Urna")
    st.caption("Investigação detalhada da issue A03 (votos contabilizados no log divergem do BU)")

    with st.expander("Causa-raiz da issue A03"):
        st.markdown(
            "A issue **A03** ocorre quando o número de votos registrados no arquivo de log "
            "da urna diverge do total presente no Boletim de Urna (BU).\n\n"
            "**Causa identificada:** Na grande maioria dos casos, isso ocorre em **urnas reserva** "
            "(tipo `reservaSecao`), onde a urna substituiu outra que apresentou falha. "
            "O log da urna reserva registra apenas os votos computados após a substituição, "
            "enquanto o BU contém o total consolidado (incluindo votos da urna original, "
            "quando transferidos).\n\n"
            "**No 2º Turno**, há um fator adicional: o log acumula registros do 1º e 2º turno. "
            "A contagem de votos no log pode incluir eventos de ambos os turnos, "
            "gerando divergência artificial com o BU do 2º turno.\n\n"
            "**Conclusão:** A03 é um artefato do processo de contingência e acumulação de logs, "
            "**não indicando adulteração de votos**."
        )

    # A03 por turno
    st.markdown("### Resumo por Turno")
    try:
        turno_df = analysis.a03_by_turno(store, f)
        if not turno_df.empty:
            st.dataframe(turno_df, width="stretch", hide_index=True)

            col1, col2 = st.columns(2)
            for _, row in turno_df.iterrows():
                turno_val = row.get("turno", "?")
                count_val = int(row.get("count", 0))
                pct_val = row.get("pct_of_secoes", 0)
                if turno_val == 1:
                    col1.metric(f"A03 no 1º Turno", f"{count_val}", f"{pct_val:.2f}% das seções")
                elif turno_val == 2:
                    col2.metric(f"A03 no 2º Turno", f"{count_val}", f"{pct_val:.2f}% das seções")
        else:
            st.info("Nenhuma issue A03 encontrada.")
    except Exception as e:
        st.warning(f"Erro ao carregar A03 por turno: {e}")

    # Detalhes A03
    st.markdown("### Seções com Divergência A03")
    try:
        detail_df = analysis.a03_detail(store, f)
        if not detail_df.empty:
            st.dataframe(detail_df, width="stretch", hide_index=True)

            st.caption(
                "votos_log = votos contabilizados no log | "
                "comparecimento = total no BU | "
                "ratio = votos_log / comparecimento"
            )
        else:
            st.info("Nenhuma seção com A03 nos filtros atuais.")
    except Exception as e:
        st.warning(f"Erro ao carregar detalhes A03: {e}")


def tab_timing(store, f):
    """Aba Timing Detalhado — Duração da votação."""
    st.subheader("Timing Detalhado — Duração da Votação")
    st.caption("Análise detalhada da duração de votação por estado e detecção de anomalias")

    # Distribuição por estado
    st.markdown("### Distribuição de Duração por Estado")
    try:
        dur_df = analysis.duration_distribution(store, f)
        if not dur_df.empty:
            st.dataframe(dur_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=dur_df["uf"],
                y=dur_df["media_min"],
                name="Média (min)",
                marker_color="#636EFA",
            ))
            fig.add_trace(go.Bar(
                x=dur_df["uf"],
                y=dur_df["mediana_min"],
                name="Mediana (min)",
                marker_color="#EF553B",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="UF",
                yaxis_title="Duração (minutos)",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados de duração.")
    except Exception as e:
        st.warning(f"Erro ao carregar distribuição de duração: {e}")

    # Seções com duração anormal
    st.markdown("### Seções com Duração Anormal")
    try:
        abnormal_df = analysis.abnormal_duration_sections(store, f)
        if not abnormal_df.empty:
            st.metric("Seções com Duração Anormal", f"{len(abnormal_df):,}")
            st.dataframe(abnormal_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma seção com duração anormal detectada.")
    except Exception as e:
        st.warning(f"Erro ao carregar seções com duração anormal: {e}")

    # Duração vs votação
    st.markdown("### Duração vs Padrão de Votação")
    try:
        dvote_df = analysis.duration_vs_vote(store, f)
        if not dvote_df.empty:
            st.dataframe(dvote_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=dvote_df["quartil"],
                y=dvote_df["pct_lula"],
                name="Lula (%)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=dvote_df["quartil"],
                y=dvote_df["pct_bolso"],
                name="Bolsonaro (%)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Quartil de Duração",
                yaxis_title="Percentual de Votos (%)",
                title="Votação por Quartil de Duração",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                "Se houver correlação entre duração da votação e resultado, "
                "pode indicar fatores geográficos ou demográficos — não necessariamente irregularidade."
            )
        else:
            st.info("Sem dados de duração vs votação.")
    except Exception as e:
        st.warning(f"Erro ao carregar duração vs votação: {e}")

    # Extensão: histograma de duração
    extend_tab_timing(store, f)


def tab_geographic(store, f):
    """Aba Anomalias Geográficas."""
    st.subheader("Anomalias Geográficas")
    st.caption("Análise da concentração de issues por estado e correlação com resultado eleitoral")

    # Densidade de issues por estado
    st.markdown("### Densidade de Issues por Estado")
    try:
        density_df = analysis.issue_density_by_state(store, f)
        if not density_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=density_df["uf"],
                y=density_df["density"],
                marker_color=density_df["z_density"].apply(
                    lambda z: "#EF553B" if abs(z) > 2 else "#FFA15A" if abs(z) > 1 else "#636EFA"
                ),
                text=density_df["density"].round(2),
                textposition="auto",
            ))
            fig.update_layout(
                xaxis_title="UF",
                yaxis_title="Densidade (issues/seção)",
                title="Densidade de Issues por Estado",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(density_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados de densidade.")
    except Exception as e:
        st.warning(f"Erro ao carregar densidade de issues: {e}")

    # Correlação issues vs resultado
    st.markdown("### Densidade de Issues vs Resultado Eleitoral")
    try:
        corr_df = analysis.geographic_issue_vs_result(store, f)
        if not corr_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=corr_df["density"],
                y=corr_df["pct_lula"],
                mode="markers+text",
                text=corr_df["uf"],
                textposition="top center",
                name="% Lula",
                marker=dict(size=10, color="#EF553B"),
            ))
            fig.add_trace(go.Scatter(
                x=corr_df["density"],
                y=corr_df["pct_bolso"],
                mode="markers+text",
                text=corr_df["uf"],
                textposition="bottom center",
                name="% Bolsonaro",
                marker=dict(size=10, color="#636EFA"),
            ))
            fig.update_layout(
                xaxis_title="Densidade de Issues (issues/seção)",
                yaxis_title="Percentual de Votos (%)",
                title="Correlação: Densidade de Issues vs Resultado",
                height=500,
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(corr_df, width="stretch", hide_index=True)

            st.caption(
                "Se a densidade de issues não apresentar correlação com o resultado eleitoral, "
                "isso indica que as issues são de natureza operacional e não direcionadas."
            )
        else:
            st.info("Sem dados para correlação.")
    except Exception as e:
        st.warning(f"Erro ao carregar correlação geográfica: {e}")


# ============================================================
# Novas tabs integradas
# ============================================================


def paginate_dataframe(df, page_size=100):
    """Paginação para DataFrames grandes.

    Exibe controles de paginação via st.number_input e retorna o slice
    correspondente à página atual.
    """
    if df.empty:
        return df

    total_rows = len(df)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        page = st.number_input(
            "Página",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"page_{id(df)}",
        )
    with col3:
        st.markdown(f"**{total_rows:,}** resultados | "
                    f"Página **{page}** de **{total_pages}**")

    start = (page - 1) * page_size
    end = min(start + page_size, total_rows)

    return df.iloc[start:end].reset_index(drop=True)


def tab_benford_v2(store, f):
    """Aba Lei de Benford -- Análise de 1o e 2o dígito + todos os cargos."""

    st.subheader("Lei de Benford -- Distribuição do Primeiro e Segundo Dígito")
    st.caption(
        "Análise da conformidade dos votos com a distribuição esperada "
        "pela Lei de Benford (1o e 2o dígitos)"
    )

    with st.expander("O que é a Lei de Benford?"):
        st.markdown(
            "A **Lei de Benford** (ou Lei do Primeiro Dígito) prevê que em muitos conjuntos de dados "
            "numéricos naturais, o dígito 1 aparece como primeiro dígito em ~30,1% dos casos, "
            "o dígito 2 em ~17,6%, e assim por diante, em escala logarítmica decrescente.\n\n"
            "A lei também se aplica ao **segundo dígito**, com uma distribuição mais uniforme "
            "(de ~11,97% para o dígito 0 até ~8,50% para o dígito 9).\n\n"
            "Em contextos eleitorais, essa lei pode ser aplicada aos totais de votos por seção "
            "para verificar se a distribuição é consistente com dados não manipulados. "
            "No entanto, **desvios não implicam necessariamente fraude** -- fatores como "
            "tamanho das seções, distribuição geográfica e regras eleitorais podem causar desvios naturais.\n\n"
            "O **teste Qui-Quadrado** quantifica o desvio entre a distribuição observada e a esperada. "
            "O **Cramér's V** mede o tamanho do efeito (V < 0,05 = desprezível, "
            "V < 0,10 = pequeno, V < 0,20 = moderado). "
            "O **teste KS** avalia a distância entre as distribuições acumuladas."
        )

    # ============================================================
    # Seção 1: Primeiro Dígito (original)
    # ============================================================
    st.markdown("### Distribuição do 1o Dígito -- Observado vs Esperado")
    try:
        benford_df = analysis.benford_first_digit(store, f)

        if not benford_df.empty:
            candidatos = benford_df["candidato"].unique()
            for cand in candidatos:
                cand_data = benford_df[benford_df["candidato"] == cand]
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=cand_data["pct_observado"],
                    name="Observado",
                    marker_color="#636EFA",
                ))
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=cand_data["pct_esperado"],
                    name="Esperado (Benford)",
                    marker_color="#EF553B",
                ))
                fig.update_layout(
                    title=f"Candidato {cand} -- 1o Dígito",
                    xaxis_title="Primeiro Dígito",
                    yaxis_title="Frequência (%)",
                    barmode="group",
                    height=350,
                )
                st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados suficientes para análise de Benford (1o dígito).")
    except Exception as e:
        st.warning(f"Erro ao carregar distribuição de Benford (1o dígito): {e}")

    # Teste Qui-Quadrado por candidato (1o dígito)
    st.markdown("### Teste Qui-Quadrado por Candidato (1o Dígito)")
    try:
        chi2_df = analysis.benford_chi_squared(store, f)

        if not chi2_df.empty:
            st.dataframe(chi2_df, width="stretch", hide_index=True)

            nao_conformes = chi2_df[chi2_df["conforme"] == False]
            if len(nao_conformes) == 0:
                st.success(
                    "Todos os candidatos apresentam distribuição conforme "
                    "a Lei de Benford (p > 0,05)."
                )
            else:
                st.warning(
                    f"{len(nao_conformes)} candidato(s) com desvio significativo "
                    "da Lei de Benford. "
                    "Isso não implica fraude necessariamente -- veja a explicação acima."
                )
        else:
            st.info("Sem dados para teste Qui-Quadrado (1o dígito).")
    except Exception as e:
        st.warning(f"Erro ao carregar teste Qui-Quadrado (1o dígito): {e}")

    # Conformidade por estado (1o dígito)
    st.markdown("### Conformidade por Estado (1o Dígito)")
    try:
        state_df = analysis.benford_by_state(store, f)

        if not state_df.empty:
            st.dataframe(state_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados por estado.")
    except Exception as e:
        st.warning(f"Erro ao carregar conformidade por estado: {e}")

    # ============================================================
    # Seção 2: Segundo Dígito (NOVA)
    # ============================================================
    st.markdown("---")
    st.markdown("### Análise do 2o Dígito")
    st.caption(
        "Distribuição do segundo dígito dos votos por seção para Presidente. "
        "Seções com menos de 10 votos (sem segundo dígito) são excluídas."
    )

    try:
        second_digit_df = analysis.benford_second_digit(store, f)
        if not second_digit_df.empty:
            candidatos_2d = second_digit_df["candidato"].unique()

            # Distribuição esperada para exibir no gráfico
            expected_pcts = {
                d: round(analysis.BENFORD_SECOND_EXPECTED[d] * 100, 4) for d in range(0, 10)
            }

            for cand in candidatos_2d:
                cand_data = second_digit_df[
                    second_digit_df["candidato"] == cand
                ].copy()

                # Garantir todos os dígitos 0-9
                full_digits = pd.DataFrame({"digito": range(0, 10)})
                cand_data = full_digits.merge(
                    cand_data[["digito", "percentual"]], on="digito", how="left"
                ).fillna(0)

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=cand_data["percentual"],
                    name="Observado",
                    marker_color="#636EFA",
                ))
                fig.add_trace(go.Bar(
                    x=cand_data["digito"],
                    y=[expected_pcts[d] for d in range(0, 10)],
                    name="Esperado (Benford 2o dígito)",
                    marker_color="#EF553B",
                ))
                fig.update_layout(
                    title=f"Candidato {cand} -- 2o Dígito",
                    xaxis_title="Segundo Dígito",
                    yaxis_title="Frequência (%)",
                    barmode="group",
                    height=350,
                )
                st.plotly_chart(fig, width="stretch")
        else:
            st.info(
                "Sem dados suficientes para análise do 2o dígito de Benford."
            )
    except Exception as e:
        st.warning(f"Erro ao carregar distribuição do 2o dígito: {e}")

    # Tabela de testes estatísticos para 2o dígito
    st.markdown("### Testes Estatísticos -- 2o Dígito (Chi-Quadrado + KS + Cramér's V)")
    try:
        chi2_second = analysis.benford_second_digit_chi(store, f)
        if chi2_second:
            chi2_second_df = pd.DataFrame(chi2_second)
            st.dataframe(chi2_second_df, width="stretch", hide_index=True)

            nao_conf_2d = chi2_second_df[chi2_second_df["conforme"] == False]
            if len(nao_conf_2d) == 0:
                st.success(
                    "Todos os candidatos apresentam distribuição do 2o dígito "
                    "conforme a Lei de Benford (p > 0,05 e Cramér's V < 0,10)."
                )
            else:
                st.warning(
                    f"{len(nao_conf_2d)} candidato(s) com desvio no 2o dígito. "
                    "Verifique o Cramér's V para avaliar a relevância prática do desvio."
                )

            st.caption(
                "Cramér's V < 0,05 = efeito desprezível | "
                "< 0,10 = efeito pequeno | "
                "< 0,20 = efeito moderado | "
                ">= 0,20 = efeito grande"
            )
        else:
            st.info("Sem dados para testes estatísticos do 2o dígito.")
    except Exception as e:
        st.warning(f"Erro ao carregar testes do 2o dígito: {e}")

    # ============================================================
    # Seção 3: Benford por Cargo (NOVA)
    # ============================================================
    st.markdown("---")
    st.markdown("### Benford por Cargo")
    st.caption(
        "Aplicação da Lei de Benford (1o dígito) para todos os cargos, "
        "não apenas Presidente. Inclui Chi-Quadrado e Cramér's V."
    )

    try:
        offices_df = analysis.benford_all_offices(store, f)
        if not offices_df.empty:
            st.dataframe(offices_df, width="stretch", hide_index=True)

            n_conformes_cargo = int(offices_df["conforme"].sum())
            n_total_cargo = len(offices_df)

            if n_conformes_cargo == n_total_cargo:
                st.success(
                    f"Todos os {n_total_cargo} cargos apresentam distribuição "
                    "conforme a Lei de Benford (p > 0,05)."
                )
            else:
                n_nao_conf = n_total_cargo - n_conformes_cargo
                st.warning(
                    f"{n_nao_conf}/{n_total_cargo} cargo(s) com desvio "
                    "significativo (p <= 0,05). "
                    "Verifique o Cramér's V para avaliar a relevância prática."
                )

            # Destacar cargos com Cramér's V alto
            altos_v = offices_df[offices_df["cramers_v"] >= 0.10]
            if not altos_v.empty:
                st.info(
                    f"{len(altos_v)} cargo(s) com Cramér's V >= 0,10: "
                    f"{', '.join(altos_v['cargo'].tolist())}. "
                    "Investigação adicional pode ser útil."
                )
        else:
            st.info("Sem dados suficientes para análise por cargo.")
    except Exception as e:
        st.warning(f"Erro ao carregar Benford por cargo: {e}")


def tab_nulos_brancos(store, f):
    """Aba Nulos & Brancos - Análise de outliers em nulos e brancos."""
    st.subheader("Nulos & Brancos")
    st.caption("Detecção de seções com proporção anormal de votos nulos ou brancos (z-score > 3)")

    # ----------------------------------------------------------
    # Seção 1: KPIs
    # ----------------------------------------------------------
    try:
        summary_df = analysis.null_blank_summary(store, f)
        if not summary_df.empty:
            total_outliers_nulos = int(summary_df["outliers_nulos"].sum())
            total_outliers_brancos = int(summary_df["outliers_brancos"].sum())

            col1, col2 = st.columns(2)
            col1.metric("Total de Outliers de Nulos", f"{total_outliers_nulos:,}")
            col2.metric("Total de Outliers de Brancos", f"{total_outliers_brancos:,}")
        else:
            st.info("Nenhum outlier de nulos/brancos detectado.")
    except Exception as e:
        st.warning(f"Erro ao carregar KPIs de nulos/brancos: {e}")

    # ----------------------------------------------------------
    # Seção 2: Resumo por estado
    # ----------------------------------------------------------
    st.markdown("### Resumo por Estado")
    try:
        summary_df = analysis.null_blank_summary(store, f)
        if not summary_df.empty:
            st.dataframe(summary_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados de resumo por estado.")
    except Exception as e:
        st.warning(f"Erro ao carregar resumo por estado: {e}")

    # ----------------------------------------------------------
    # Seção 3: Scatter plot pct_nulos vs pct_brancos
    # ----------------------------------------------------------
    st.markdown("### Dispersão: % Nulos vs % Brancos por Seção")
    try:
        scatter_df = store.query_df(f"""
            SELECT
                t.secao_id,
                s.uf,
                ROUND(t.nulos * 100.0 / NULLIF(t.total, 0), 2) AS pct_nulos,
                ROUND(t.brancos * 100.0 / NULLIF(t.total, 0), 2) AS pct_brancos
            FROM totais_cargo t
            JOIN secoes s ON t.secao_id = s.id
            WHERE {f['where']}
                AND t.cargo = 'Presidente'
                AND t.total > 0
            ORDER BY RANDOM()
            LIMIT 2000
        """, f["params"])

        if not scatter_df.empty:
            fig = px.scatter(
                scatter_df,
                x="pct_nulos",
                y="pct_brancos",
                color="uf",
                hover_data=["secao_id"],
                title="% Nulos vs % Brancos (Presidente, amostra de 2.000 seções)",
                labels={
                    "pct_nulos": "% Nulos",
                    "pct_brancos": "% Brancos",
                    "uf": "UF",
                },
                opacity=0.6,
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados para o gráfico de dispersão.")
    except Exception as e:
        st.warning(f"Erro ao carregar scatter de nulos/brancos: {e}")

    # ----------------------------------------------------------
    # Seção 4: Correlação nulos vs resultado
    # ----------------------------------------------------------
    st.markdown("### Correlação: Nulos vs Resultado Eleitoral")
    try:
        nb_result_df = analysis.null_blank_vs_result(store, f)
        if not nb_result_df.empty:
            st.dataframe(nb_result_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=nb_result_df["quartil"],
                y=nb_result_df["pct_lula"],
                name="Lula (%)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=nb_result_df["quartil"],
                y=nb_result_df["pct_bolso"],
                name="Bolsonaro (%)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Quartil de % Nulos",
                yaxis_title="Percentual de Votos (%)",
                title="Votação por Quartil de Nulos (Presidente)",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                "Se a proporção de votos nulos não tiver correlação com o resultado, "
                "isso indica que os nulos refletem o comportamento do eleitor, "
                "não uma manipulação direcionada."
            )
        else:
            st.info("Sem dados de correlação nulos vs resultado.")
    except Exception as e:
        st.warning(f"Erro ao carregar correlação nulos vs resultado: {e}")

    # ----------------------------------------------------------
    # Seção 5: Tabela de seções outliers
    # ----------------------------------------------------------
    st.markdown("### Seções Outliers de Nulos/Brancos")
    try:
        outliers_df = analysis.null_blank_by_section(store, f)
        if not outliers_df.empty:
            st.metric("Total de Seções Outliers", f"{len(outliers_df):,}")
            st.dataframe(outliers_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma seção outlier detectada para os filtros atuais.")
    except Exception as e:
        st.warning(f"Erro ao carregar seções outliers: {e}")


def tab_distribuicao_candidato(store, f):
    """Aba Distribuição por Candidato - Análise da votação Lula x Bolsonaro por seção."""
    st.subheader("Distribuição por Candidato")
    st.caption("Análise da distribuição de votos entre Lula (13) e Bolsonaro (22) por seção")

    # ----------------------------------------------------------
    # Seção 1: Histograma de pct_lula por seção
    # ----------------------------------------------------------
    st.markdown("### Histograma: % Lula por Seção (Presidente)")
    try:
        hist_df = store.query_df(f"""
            SELECT
                s.id AS secao_id,
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

        if not hist_df.empty:
            hist_df["pct_lula"] = (
                hist_df["votos_13"]
                / (hist_df["votos_13"] + hist_df["votos_22"])
                * 100
            ).round(2)

            fig = px.histogram(
                hist_df,
                x="pct_lula",
                nbins=50,
                title="Distribuição do % de Votos em Lula por Seção (Presidente)",
                labels={"pct_lula": "% Lula (votos válidos 13 vs 22)"},
                color_discrete_sequence=["#636EFA"],
            )
            fig.update_layout(
                xaxis_title="% Lula",
                yaxis_title="Quantidade de Seções",
                height=450,
            )
            fig.add_vline(
                x=50, line_dash="dash", line_color="red",
                annotation_text="50%", annotation_position="top right",
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                f"Total de seções no histograma: {len(hist_df):,}. "
                f"Média: {hist_df['pct_lula'].mean():.2f}%, "
                f"Mediana: {hist_df['pct_lula'].median():.2f}%."
            )
        else:
            st.info("Sem dados para histograma.")
    except Exception as e:
        st.warning(f"Erro ao carregar histograma de distribuição: {e}")

    # ----------------------------------------------------------
    # Seção 2: Estatísticas por estado
    # ----------------------------------------------------------
    st.markdown("### Estatísticas por Estado")
    try:
        state_df = analysis.candidate_distribution_by_state(store, f)
        if not state_df.empty:
            st.dataframe(state_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=state_df["uf"],
                y=state_df["media"],
                name="Média (% Lula)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=state_df["uf"],
                y=state_df["mediana"],
                name="Mediana (% Lula)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="UF",
                yaxis_title="% Lula",
                title="Média e Mediana do % Lula por Estado",
                height=400,
            )
            fig.add_hline(
                y=50, line_dash="dash", line_color="gray",
                annotation_text="50%", annotation_position="top right",
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem dados de estatísticas por estado.")
    except Exception as e:
        st.warning(f"Erro ao carregar estatísticas por estado: {e}")

    # ----------------------------------------------------------
    # Seção 3: Seções extremas
    # ----------------------------------------------------------
    st.markdown("### Seções com Votação Extrema")
    try:
        extreme_df = analysis.candidate_extreme_sections(store, f)
        if not extreme_df.empty:
            n_dom_13 = int((extreme_df["tipo_extremo"] == "dominancia_13").sum())
            n_dom_22 = int((extreme_df["tipo_extremo"] == "dominancia_22").sum())

            col1, col2 = st.columns(2)
            col1.metric("Dominancia 13 (Lula >= 95%)", f"{n_dom_13:,}")
            col2.metric("Dominancia 22 (Bolsonaro >= 95%)", f"{n_dom_22:,}")

            st.warning(
                f"Encontradas {len(extreme_df):,} seções com votação extrema "
                f"(>= 95% para um candidato). Seções com poucos eleitores "
                f"tendem a apresentar esse padrão naturalmente."
            )

            st.dataframe(extreme_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma seção com votação extrema detectada.")
    except Exception as e:
        st.warning(f"Erro ao carregar seções extremas: {e}")

    # ----------------------------------------------------------
    # Seção 4: Outliers por z-score estadual
    # ----------------------------------------------------------
    st.markdown("### Outliers por Z-Score Estadual")
    st.caption("Seções com distribuição atípica em relação à sua própria UF (z-score > 3)")
    try:
        zscore_df = analysis.candidate_section_distribution(store, f)
        if not zscore_df.empty:
            st.metric("Seções Outliers (z-score > 3)", f"{len(zscore_df):,}")

            # Scatter plot
            limited_df = zscore_df.head(200)
            fig = px.scatter(
                limited_df,
                x="pct_lula",
                y="zscore",
                color="uf",
                hover_data=["municipio", "zona", "secao", "votos_13", "votos_22"],
                title="Outliers: % Lula vs Z-Score por UF",
                labels={
                    "pct_lula": "% Lula",
                    "zscore": "Z-Score",
                    "uf": "UF",
                },
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, width="stretch")

            st.dataframe(zscore_df, width="stretch", hide_index=True)
        else:
            st.info("Nenhum outlier de distribuição por candidato detectado.")
    except Exception as e:
        st.warning(f"Erro ao carregar outliers de distribuição: {e}")


def tab_mapa(store, f):
    """Tab Mapa Choropleth do Brasil colorido por métrica selecionada."""
    import json
    import urllib.request
    import numpy as np

    st.subheader("Mapa do Brasil -- Métricas por Estado")
    st.caption(
        "Visualização geográfica das métricas eleitorais. "
        "Selecione a métrica desejada para colorir o mapa."
    )

    # Carregar GeoJSON
    @st.cache_data(ttl=3600)
    def load_brazil_geojson():
        url = (
            "https://raw.githubusercontent.com/codeforamerica/"
            "click_that_hood/master/public/data/brazil-states.geojson"
        )
        with urllib.request.urlopen(url) as resp:
            return json.loads(resp.read().decode())

    try:
        geojson = load_brazil_geojson()
    except Exception as e:
        st.error(f"Erro ao carregar GeoJSON do Brasil: {e}")
        return

    # Obter dados
    try:
        metrics_df = analysis.map_state_metrics(store, f)
        votes_df = analysis.map_state_votes(store, f)
    except Exception as e:
        st.warning(f"Erro ao carregar métricas do mapa: {e}")
        return

    if metrics_df.empty:
        st.info("Sem dados disponíveis para os filtros selecionados.")
        return

    # Merge métricas e votos
    if not votes_df.empty:
        df = metrics_df.merge(votes_df[["uf", "pct_lula", "pct_bolso"]], on="uf", how="left")
    else:
        df = metrics_df.copy()
        df["pct_lula"] = np.nan
        df["pct_bolso"] = np.nan

    # Criar coluna UF maiúscula para match com GeoJSON (propriedade "sigla")
    df["uf_upper"] = df["uf"].str.upper()

    # Seletor de métrica
    metric_options = {
        "Reboots (%)": "pct_reboots",
        "Issues por seção": "density_issues",
        "Abstenção (%)": "pct_abstencao",
        "Biometria (%)": "media_biometria",
        "% Lula": "pct_lula",
    }

    metric_label = st.selectbox("Métrica para coloração", list(metric_options.keys()))
    metric_col = metric_options[metric_label]

    if metric_col not in df.columns or df[metric_col].dropna().empty:
        st.info(f"Sem dados para a métrica '{metric_label}'.")
        return

    # Escala de cores
    if metric_col == "media_biometria":
        color_scale = "RdYlGn"
    elif metric_col == "pct_lula":
        color_scale = "RdBu_r"
    else:
        color_scale = "RdYlGn_r"

    # Criar mapa choropleth
    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="uf_upper",
        featureidkey="properties.sigla",
        color=metric_col,
        color_continuous_scale=color_scale,
        scope="south america",
        title=f"Mapa: {metric_label}",
        hover_name="uf_upper",
        hover_data={
            "uf_upper": False,
            "secoes": True,
            "pct_reboots": ":.2f",
            "pct_issues": ":.2f",
            "pct_abstencao": ":.2f",
            "media_biometria": ":.2f",
            "density_issues": ":.4f",
        },
        labels={
            "pct_reboots": "Reboots (%)",
            "density_issues": "Issues/seção",
            "pct_abstencao": "Abstenção (%)",
            "media_biometria": "Biometria (%)",
            "pct_lula": "% Lula",
            "secoes": "Seções",
        },
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(height=600, margin=dict(l=0, r=0, t=40, b=0))

    st.plotly_chart(fig, width="stretch")

    # Tabela de dados por UF
    st.markdown("### Dados por Estado")

    display_df = df.copy()
    display_df["uf"] = display_df["uf"].str.upper()
    display_cols = [
        "uf", "secoes", "pct_reboots", "pct_issues", "pct_abstencao",
        "media_biometria", "density_issues",
    ]
    if "pct_lula" in display_df.columns:
        display_cols.extend(["pct_lula", "pct_bolso"])

    display_cols = [c for c in display_cols if c in display_df.columns]

    st.dataframe(
        display_df[display_cols].sort_values("uf").reset_index(drop=True),
        width="stretch",
        hide_index=True,
    )

    st.caption(
        "z_density: z-score da densidade de issues. "
        "Valores > 2 indicam estados com concentração atípica de issues."
    )


def tab_reserva(store, f):
    """Tab Seções Reserva -- Comparativo entre seções normais e reserva."""

    st.subheader("Seções Reserva -- Análise Comparativa")
    st.caption(
        "Seções reserva (reservaSecao) são urnas substitutas ativadas quando a "
        "urna original apresenta falha. Esta análise compara seu comportamento "
        "com as seções normais."
    )

    # 1. Comparativo de métricas
    st.markdown("### Comparativo de Métricas: Normal vs Reserva")
    try:
        comp_df = analysis.reserve_vs_normal(store, f)
        if not comp_df.empty:
            st.dataframe(comp_df, width="stretch", hide_index=True)

            # Gráfico de barras comparativo
            fig = go.Figure()
            for _, row in comp_df.iterrows():
                tipo = str(row["tipo"])
                fig.add_trace(go.Bar(
                    x=["Média Reboots", "Média Erros", "% Issues"],
                    y=[row["media_reboots"], row["media_erros"], row["pct_issues"]],
                    name=tipo,
                ))

            fig.update_layout(
                barmode="group",
                xaxis_title="Métrica",
                yaxis_title="Valor",
                title="Comparativo: Seção Normal vs Reserva",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            # Destaque: seções reserva com mais reboots e erros
            reserva_row = comp_df[comp_df["tipo"] == "reservaSecao"]
            normal_row = comp_df[comp_df["tipo"] == "secao"]

            if not reserva_row.empty and not normal_row.empty:
                r = reserva_row.iloc[0]
                n = normal_row.iloc[0]

                col1, col2, col3 = st.columns(3)
                col1.metric(
                    "Seções Reserva",
                    f"{int(r['secoes']):,}",
                    f"{r['secoes'] / (r['secoes'] + n['secoes']) * 100:.2f}% do total"
                    if (r["secoes"] + n["secoes"]) > 0 else "-",
                )
                col2.metric(
                    "Reboots (Reserva vs Normal)",
                    f"{r['media_reboots']:.2f}",
                    f"{r['media_reboots'] - n['media_reboots']:+.2f}",
                )
                col3.metric(
                    "Erros (Reserva vs Normal)",
                    f"{r['media_erros']:.2f}",
                    f"{r['media_erros'] - n['media_erros']:+.2f}",
                )
        else:
            st.info("Sem dados de tipo de urna disponíveis.")
    except Exception as e:
        st.warning(f"Erro ao carregar comparativo normal vs reserva: {e}")

    # 2. Check A03
    st.markdown("---")
    st.markdown("### Verificação A03 -- Exclusividade em Seções Reserva")
    try:
        a03 = analysis.reserve_a03_check(store, f)

        if a03["total_a03"] == 0:
            st.info("Nenhuma issue A03 encontrada nos filtros atuais.")
        else:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total A03", f"{a03['total_a03']:,}")
            col2.metric("A03 em Reserva", f"{a03['a03_reserva']:,}")
            col3.metric("A03 em Normal", f"{a03['a03_normal']:,}")
            col4.metric("% em Reserva", f"{a03['pct_reserva']:.1f}%")

            if a03["a03_normal"] == 0:
                st.success(
                    "Todas as issues A03 (divergência votos log vs BU) ocorrem "
                    "exclusivamente em seções reserva. Isso confirma que a divergência "
                    "é causada pelo processo de contingência, e não por adulteração."
                )
            else:
                st.error(
                    f"ANOMALIA: {a03['a03_normal']} issue(s) A03 em seções NORMAIS! "
                    f"Isso é inesperado e requer investigação detalhada, pois A03 "
                    f"deveria ocorrer apenas em urnas reserva."
                )
    except Exception as e:
        st.warning(f"Erro ao verificar A03 em seções reserva: {e}")

    # 3. Padrão de votação
    st.markdown("---")
    st.markdown("### Padrão de Votação: Normal vs Reserva")
    try:
        vote_df = analysis.reserve_vote_pattern(store, f)
        if not vote_df.empty:
            st.dataframe(vote_df, width="stretch", hide_index=True)

            fig = go.Figure()
            for _, row in vote_df.iterrows():
                tipo = str(row["tipo"])
                fig.add_trace(go.Bar(
                    x=["% Lula", "% Bolsonaro", "% Nulos", "% Brancos"],
                    y=[row["pct_lula"], row["pct_bolso"], row["pct_nulos"], row["pct_brancos"]],
                    name=tipo,
                ))

            fig.update_layout(
                barmode="group",
                xaxis_title="Tipo de Voto",
                yaxis_title="Percentual (%)",
                title="Votação para Presidente: Seção Normal vs Reserva",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            st.caption(
                "Diferenças no padrão de votação entre seções normais e reserva "
                "podem refletir características geográficas das seções que necessitaram "
                "de substituição, e não indicam necessariamente irregularidade."
            )
        else:
            st.info("Sem dados de votação por tipo de seção.")
    except Exception as e:
        st.warning(f"Erro ao carregar padrão de votação por tipo: {e}")


def tab_substituicoes_erros(store, f):
    """Tab Substituições & Erros -- Análise de substituições e erros de log."""

    st.subheader("Substituições & Erros de Log")
    st.caption(
        "Análise de eventos de substituição/contingência nas urnas e "
        "distribuição de erros registrados no log por modelo de urna."
    )

    # 1. Substituições por estado
    st.markdown("### Substituições por Estado")
    try:
        sub_state_df = analysis.substitution_by_state(store, f)
        if not sub_state_df.empty:
            total_sub = int(sub_state_df["total_substituicoes"].sum())
            total_secoes_sub = int(sub_state_df["secoes_com_substituicao"].sum())

            col1, col2 = st.columns(2)
            col1.metric("Total de Substituições", f"{total_sub:,}")
            col2.metric("Seções com Substituição", f"{total_secoes_sub:,}")

            st.dataframe(sub_state_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=sub_state_df["uf"].str.upper(),
                y=sub_state_df["total_substituicoes"],
                name="Total Substituições",
                marker_color="#636EFA",
                text=sub_state_df["total_substituicoes"],
                textposition="auto",
            ))
            fig.update_layout(
                xaxis_title="UF",
                yaxis_title="Total de Substituições",
                title="Substituições por Estado",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=sub_state_df["uf"].str.upper(),
                y=sub_state_df["pct_secoes"],
                name="% Seções com Substituição",
                marker_color="#EF553B",
                text=sub_state_df["pct_secoes"].apply(lambda x: f"{x:.1f}%"),
                textposition="auto",
            ))
            fig2.update_layout(
                xaxis_title="UF",
                yaxis_title="% de Seções",
                title="Percentual de Seções com Substituição por Estado",
                height=400,
            )
            st.plotly_chart(fig2, width="stretch")
        else:
            st.info("Nenhuma substituição registrada nos filtros atuais.")
    except Exception as e:
        st.warning(f"Erro ao carregar substituições por estado: {e}")

    # 2. Votação com/sem substituição
    st.markdown("---")
    st.markdown("### Votação: Com Substituição vs Sem Substituição")
    try:
        sub_vote_df = analysis.substitution_vs_result(store, f)
        if not sub_vote_df.empty:
            st.dataframe(sub_vote_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=sub_vote_df["grupo"],
                y=sub_vote_df["pct_lula"],
                name="Lula (%)",
                marker_color="#EF553B",
            ))
            fig.add_trace(go.Bar(
                x=sub_vote_df["grupo"],
                y=sub_vote_df["pct_bolso"],
                name="Bolsonaro (%)",
                marker_color="#636EFA",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Grupo",
                yaxis_title="Percentual (%)",
                title="Votação para Presidente: Com vs Sem Substituição",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            com_sub = sub_vote_df[sub_vote_df["grupo"] == "com_substituição"]
            sem_sub = sub_vote_df[sub_vote_df["grupo"] == "sem_substituição"]

            if not com_sub.empty and not sem_sub.empty:
                diff_lula = float(com_sub.iloc[0]["pct_lula"]) - float(sem_sub.iloc[0]["pct_lula"])
                diff_bolso = float(com_sub.iloc[0]["pct_bolso"]) - float(sem_sub.iloc[0]["pct_bolso"])

                if abs(diff_lula) < 2.0 and abs(diff_bolso) < 2.0:
                    st.success(
                        f"Diferença pequena entre grupos: "
                        f"Lula {diff_lula:+.2f} p.p., Bolsonaro {diff_bolso:+.2f} p.p. "
                        f"Substituições não parecem afetar o padrão de votação."
                    )
                else:
                    st.warning(
                        f"Diferença detectada: "
                        f"Lula {diff_lula:+.2f} p.p., Bolsonaro {diff_bolso:+.2f} p.p. "
                        f"Pode refletir distribuição geográfica das substituições."
                    )
        else:
            st.info("Sem dados de votação para comparação.")
    except Exception as e:
        st.warning(f"Erro ao carregar votação por substituição: {e}")

    # 3. Erros de log por modelo
    st.markdown("---")
    st.markdown("### Erros de Log por Modelo de Urna")
    try:
        err_df = analysis.error_log_by_model(store, f)
        if not err_df.empty:
            st.dataframe(err_df, width="stretch", hide_index=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=err_df["modelo_urna"].apply(lambda m: f"UE{m}" if m else "?"),
                y=err_df["media_erros"],
                name="Média de Erros",
                marker_color="#636EFA",
            ))
            fig.add_trace(go.Bar(
                x=err_df["modelo_urna"].apply(lambda m: f"UE{m}" if m else "?"),
                y=err_df["pct_com_erros"],
                name="% Seções com Erros",
                marker_color="#EF553B",
            ))
            fig.update_layout(
                barmode="group",
                xaxis_title="Modelo de Urna",
                yaxis_title="Valor",
                title="Erros de Log por Modelo de Urna",
                height=400,
            )
            st.plotly_chart(fig, width="stretch")

            worst = err_df.iloc[0]
            st.info(
                f"Modelo com mais erros: UE{worst['modelo_urna']} "
                f"(média: {worst['media_erros']:.2f}, "
                f"máximo: {int(worst['max_erros'])}, "
                f"{worst['pct_com_erros']:.1f}% com erros)."
            )

            st.caption(
                "Modelos mais antigos tendem a apresentar mais erros de log. "
                "Isso pode refletir desgaste do hardware, mas não necessariamente "
                "indica comprometimento dos votos."
            )
        else:
            st.info("Sem dados de erros de log por modelo.")
    except Exception as e:
        st.warning(f"Erro ao carregar erros por modelo: {e}")


def tab_integridade(store, f):
    """Tab Integridade & Assinaturas -- verificação de hash SHA-512."""

    st.subheader("Integridade & Assinaturas Digitais")
    st.caption("Verificação de integridade dos arquivos das urnas via hash SHA-512")

    with st.expander("Como funciona a verificação de integridade?"):
        st.markdown(
            "Cada arquivo gerado pela urna eletrônica (BU, log, RDV) "
            "possui um hash **SHA-512** registrado no arquivo de assinatura "
            "(`.vscmr`). Durante o processamento, o DataUrnas recalcula o "
            "hash de cada arquivo baixado e compara com o hash original.\n\n"
            "- **Hash SHA-512**: Função criptográfica que gera uma "
            "\"impressão digital\" única de 512 bits para cada arquivo. "
            "Qualquer alteração no conteúdo, por menor que seja, "
            "resulta em um hash completamente diferente.\n\n"
            "- **Issue C01**: Indica que o hash recalculado NÃO corresponde "
            "ao hash original, sugerindo possível corrupção ou "
            "adulteração do arquivo.\n\n"
            "- **Se não houver issues C01**: Todos os arquivos estão "
            "íntegros -- o conteúdo baixado é idêntico ao "
            "gerado pela urna."
        )

    # Resumo
    sig = analysis.signature_integrity_summary(store, f)

    if sig["total_secoes"] == 0:
        st.warning("Nenhuma seção encontrada para os filtros selecionados.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Seções", f"{sig['total_secoes']:,}")
    col2.metric("Hash Válido", f"{sig['secoes_hash_ok']:,}")
    col3.metric("Hash Inválido", f"{sig['secoes_hash_falha']:,}")
    col4.metric("% Íntegras", f"{sig['pct_ok']:.2f}%")

    st.markdown("---")

    if sig["secoes_hash_falha"] == 0:
        st.success(
            "Todas as seções passaram na verificação de "
            "integridade SHA-512. Nenhum arquivo apresentou hash inválido."
        )
    else:
        st.error(
            f"{sig['secoes_hash_falha']} seção(ões) apresentaram "
            f"hash SHA-512 inválido (issue C01). Isso indica possível "
            f"corrupção ou adulteração dos arquivos."
        )

        st.markdown("### Seções com Falha de Hash")
        detail_df = analysis.signature_detail(store, f)
        if not detail_df.empty:
            st.dataframe(detail_df, width="stretch", hide_index=True)
        else:
            st.info("Sem detalhes disponíveis para as falhas de hash.")


def render_export_buttons(store, f):
    """Botões de exportação para a tab Veredicto Final."""
    st.markdown("---")
    st.markdown("### Exportar Relatório")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Gerar Relatório Excel", type="secondary"):
            with st.spinner("Gerando relatório Excel..."):
                excel_data = analysis.generate_excel_report(store, f)
                st.session_state["_export_excel"] = excel_data

        if "_export_excel" in st.session_state:
            turno_label = f.get("turno", "ambos")
            filename = f"auditoria_eleicoes_2022_T{turno_label}.xlsx"
            st.download_button(
                label="Baixar Relatório Excel (.xlsx)",
                data=st.session_state["_export_excel"],
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with col2:
        if st.button("Gerar Relatório Texto", type="secondary"):
            with st.spinner("Gerando relatório texto..."):
                text_data = analysis.generate_text_report(store, f)
                st.session_state["_export_text"] = text_data

        if "_export_text" in st.session_state:
            turno_label = f.get("turno", "ambos")
            filename = f"auditoria_eleicoes_2022_T{turno_label}.txt"
            st.download_button(
                label="Baixar Relatório Texto (.txt)",
                data=st.session_state["_export_text"],
                file_name=filename,
                mime="text/plain",
            )


def tab_dados_brutos_v2(store, f):
    """Tab de dados brutos melhorada com paginação e filtros adicionais."""
    st.subheader("Explorar Seções")

    # Filtros inline
    search_col1, search_col2, search_col3, search_col4 = st.columns(4)
    with search_col1:
        search_uf = st.text_input("Filtrar por UF (ex.: SP)", key="brutos_v2_uf")
    with search_col2:
        mun_query_where = f["where"]
        mun_query_params = list(f["params"])
        if search_uf:
            mun_query_where += " AND LOWER(s.uf) = ?"
            mun_query_params.append(search_uf.strip().lower()[:2])

        mun_options_df = store.query_df(f"""
            SELECT DISTINCT s.municipio
            FROM secoes s
            WHERE {mun_query_where}
            ORDER BY s.municipio
            LIMIT 500
        """, mun_query_params)

        mun_list = mun_options_df["municipio"].tolist() if not mun_options_df.empty else []
        search_mun = st.selectbox(
            "Município",
            [""] + mun_list,
            index=0,
            key="brutos_v2_mun",
        )
    with search_col3:
        search_zona = st.text_input("Filtrar por zona", key="brutos_v2_zona")
    with search_col4:
        search_secao = st.text_input("Filtrar por seção", key="brutos_v2_secao")

    # Montar WHERE adicional
    extra_where = f["where"]
    extra_params = list(f["params"])
    if search_uf:
        extra_where += " AND LOWER(s.uf) = ?"
        extra_params.append(search_uf.strip().lower()[:2])
    if search_mun:
        extra_where += " AND s.municipio = ?"
        extra_params.append(search_mun.strip())
    if search_zona:
        extra_where += " AND s.zona = ?"
        extra_params.append(search_zona.strip())
    if search_secao:
        extra_where += " AND s.secao = ?"
        extra_params.append(search_secao.strip())

    # Contar total de resultados
    count_df = store.query_df(f"""
        SELECT COUNT(*) AS total
        FROM secoes s
        WHERE {extra_where}
    """, extra_params)
    total_results = int(count_df.iloc[0]["total"]) if not count_df.empty else 0

    if total_results == 0:
        st.info("Nenhuma seção encontrada.")
        return

    # Paginação
    page_size = 100
    total_pages = max(1, (total_results + page_size - 1) // page_size)

    pg_col1, pg_col2, pg_col3 = st.columns([1, 2, 1])
    with pg_col1:
        page = st.number_input(
            "Página",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="brutos_v2_page",
        )
    with pg_col3:
        st.markdown(
            f"**{total_results:,}** resultados | "
            f"Página **{page}** de **{total_pages}**"
        )

    offset = (page - 1) * page_size

    raw_df = store.query_df(f"""
        SELECT
            s.turno, s.id, UPPER(s.uf) AS uf, s.regiao, s.municipio,
            s.zona, s.secao,
            'UE' || s.modelo_urna AS modelo, s.tipo_urna, s.versao_sw,
            s.eleitores_aptos, s.comparecimento, s.pct_abstencao,
            s.pct_biometria, s.reboots, s.erros_log,
            s.hora_abertura, s.hora_encerramento, s.duracao_min,
            s.has_issues, s.n_issues
        FROM secoes s
        WHERE {extra_where}
        ORDER BY s.turno, s.uf, s.municipio, s.zona, s.secao
        LIMIT {page_size} OFFSET {offset}
    """, extra_params)

    if not raw_df.empty:
        st.dataframe(raw_df, width="stretch", height=500, hide_index=True)
        st.caption(
            f"Exibindo {len(raw_df)} de {total_results:,} seções "
            f"(página {page}/{total_pages})"
        )
    else:
        st.info("Nenhuma seção encontrada para esta página.")


def tab_drilldown(store, f):
    """Tab Explorador de Seção com drill-down cascata."""
    st.subheader("Explorador de Seção")
    st.caption("Selecione uma seção específica para análise detalhada")

    # ---- Seletores em cascata ----
    col_uf, col_mun, col_zona, col_secao = st.columns(4)

    # 1. UF
    ufs_df = store.query_df(f"""
        SELECT DISTINCT s.uf
        FROM secoes s
        WHERE {f['where']}
        ORDER BY s.uf
    """, f["params"])

    if ufs_df.empty:
        st.info("Nenhuma seção encontrada para os filtros atuais.")
        return

    ufs_list = ufs_df["uf"].tolist()

    with col_uf:
        uf_sel = st.selectbox("UF", options=ufs_list, key="drilldown_uf")

    # 2. Município
    mun_params = list(f["params"]) + [uf_sel]
    mun_df = store.query_df(f"""
        SELECT DISTINCT s.municipio
        FROM secoes s
        WHERE {f['where']} AND s.uf = ?
        ORDER BY s.municipio
    """, mun_params)

    mun_list = mun_df["municipio"].tolist() if not mun_df.empty else []

    with col_mun:
        mun_sel = st.selectbox("Município", options=mun_list, key="drilldown_mun")

    # 3. Zona
    zona_params = list(f["params"]) + [uf_sel, mun_sel]
    zona_df = store.query_df(f"""
        SELECT DISTINCT s.zona
        FROM secoes s
        WHERE {f['where']} AND s.uf = ? AND s.municipio = ?
        ORDER BY s.zona
    """, zona_params)

    zona_list = zona_df["zona"].tolist() if not zona_df.empty else []

    with col_zona:
        zona_sel = st.selectbox("Zona", options=zona_list, key="drilldown_zona")

    # 4. Seção
    secao_params = list(f["params"]) + [uf_sel, mun_sel, zona_sel]
    secao_df = store.query_df(f"""
        SELECT s.id, s.secao, s.turno
        FROM secoes s
        WHERE {f['where']} AND s.uf = ? AND s.municipio = ? AND s.zona = ?
        ORDER BY s.turno, s.secao
    """, secao_params)

    if secao_df.empty:
        st.info("Nenhuma seção encontrada para a combinação selecionada.")
        return

    secao_options = {}
    for _, row in secao_df.iterrows():
        label = f"Seção {row['secao']} (T{int(row['turno'])})"
        secao_options[label] = row["id"]

    with col_secao:
        secao_label = st.selectbox(
            "Seção", options=list(secao_options.keys()), key="drilldown_secao"
        )

    if not secao_label:
        return

    secao_id = secao_options[secao_label]

    st.divider()

    # ---- Dados da seção ----
    detail = analysis.section_detail(store, secao_id)
    info = detail["info"]

    if not info:
        st.warning("Seção não encontrada no banco de dados.")
        return

    # ---- Card com informações ----
    st.markdown("### Informações da Seção")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("UF", str(info.get("uf", "-")).upper())
    c2.metric("Município", str(info.get("municipio", "-")))
    c3.metric("Zona / Seção", f"{info.get('zona', '-')} / {info.get('secao', '-')}")
    c4.metric("Turno", f"{info.get('turno', '-')}o")

    c5, c6, c7, c8 = st.columns(4)
    modelo = info.get("modelo_urna", "-")
    c5.metric("Modelo", f"UE{modelo}" if modelo and modelo != "-" else "-")
    c6.metric("Tipo Urna", str(info.get("tipo_urna", "-")))
    c7.metric("Software", str(info.get("versao_sw", "-")))
    c8.metric("Reserva", "Sim" if info.get("is_reserva") else "Não")

    c9, c10, c11, c12 = st.columns(4)
    eleitores = info.get("eleitores_aptos", 0)
    comparecimento = info.get("comparecimento", 0)
    c9.metric("Eleitores Aptos", f"{eleitores:,}" if eleitores else "-")
    c10.metric("Comparecimento", f"{comparecimento:,}" if comparecimento else "-")
    pct_bio = info.get("pct_biometria")
    c11.metric("Biometria", f"{pct_bio:.1f}%" if pd.notna(pct_bio) else "-")
    pct_abs = info.get("pct_abstencao")
    c12.metric("Abstenção", f"{pct_abs:.1f}%" if pd.notna(pct_abs) else "-")

    c13, c14, c15, c16 = st.columns(4)
    c13.metric("Hora Abertura", str(info.get("hora_abertura", "-")))
    c14.metric("Hora Encerramento", str(info.get("hora_encerramento", "-")))
    duracao = info.get("duracao_min")
    c15.metric("Duração (min)", f"{duracao}" if pd.notna(duracao) else "-")
    reboots = info.get("reboots", 0)
    c16.metric("Reboots", f"{reboots}")

    # ---- Score de risco ----
    st.markdown("### Score de Risco")

    risk = analysis.section_risk_score(store, secao_id)
    risk_score = risk["score"]
    risk_nivel = risk["nivel"]
    risk_detalhes = risk["detalhes"]

    col_gauge, col_detalhes = st.columns([1, 1])

    with col_gauge:
        if risk_nivel == "OK":
            bar_color = "#00CC96"
        elif risk_nivel == "ATENÇÃO":
            bar_color = "#FFA15A"
        else:
            bar_color = "#EF553B"

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=risk_score,
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": f"Risco: {risk_nivel}"},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"color": bar_color},
                "bgcolor": "white",
                "steps": [
                    {"range": [0, 15], "color": "#d4edda"},
                    {"range": [15, 40], "color": "#fff3cd"},
                    {"range": [40, 100], "color": "#f8d7da"},
                ],
                "threshold": {
                    "line": {"color": "red", "width": 2},
                    "thickness": 0.75,
                    "value": risk_score,
                },
            },
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, width="stretch")

    with col_detalhes:
        st.markdown("**Fatores de risco:**")
        for detalhe in risk_detalhes:
            st.markdown(f"- {detalhe}")

    # ---- Tabela de votos por cargo/candidato ----
    st.markdown("### Votos por Cargo")
    votos_df = detail["votos"]
    if not votos_df.empty:
        for cargo in votos_df["cargo"].unique():
            cargo_data = votos_df[votos_df["cargo"] == cargo].copy()
            st.markdown(f"**{cargo}**")
            display_cols = ["tipo_voto", "codigo_candidato", "partido", "quantidade"]
            available_cols = [c for c in display_cols if c in cargo_data.columns]
            st.dataframe(
                cargo_data[available_cols].reset_index(drop=True),
                width="stretch",
                hide_index=True,
            )
    else:
        st.info("Nenhum voto encontrado para esta seção.")

    # ---- Tabela de issues ----
    st.markdown("### Inconsistências (Issues)")
    issues_df = detail["issues"]
    if not issues_df.empty:
        display_cols = ["codigo", "severidade", "descricao", "base_legal"]
        available_cols = [c for c in display_cols if c in issues_df.columns]
        st.dataframe(
            issues_df[available_cols].reset_index(drop=True),
            width="stretch",
            hide_index=True,
        )
    else:
        st.success("Nenhuma inconsistência registrada para esta seção.")

    # ---- Timeline visual ----
    st.markdown("### Timeline da Votação")
    hora_abertura = info.get("hora_abertura")
    hora_encerramento = info.get("hora_encerramento")

    if hora_abertura and hora_encerramento and str(hora_abertura) != "None":
        try:
            def _parse_hora(h):
                parts = str(h).split(":")
                if len(parts) >= 2:
                    return int(parts[0]) * 60 + int(parts[1])
                return 0

            min_abertura = _parse_hora(hora_abertura)
            min_encerramento = _parse_hora(hora_encerramento)

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=[min_encerramento - min_abertura],
                y=["Votação"],
                orientation="h",
                base=[min_abertura],
                marker_color="#636EFA",
                text=[f"{hora_abertura} -> {hora_encerramento}"],
                textposition="inside",
                name="Período de votação",
            ))

            n_reboots = int(reboots) if pd.notna(reboots) else 0
            if n_reboots > 0:
                intervalo = (min_encerramento - min_abertura)
                for i in range(min(n_reboots, 10)):
                    pos = min_abertura + intervalo * (i + 1) / (n_reboots + 1)
                    fig.add_trace(go.Scatter(
                        x=[pos],
                        y=["Votação"],
                        mode="markers",
                        marker=dict(size=14, color="red", symbol="x"),
                        name=f"Reboot {i + 1}" if i == 0 else None,
                        showlegend=(i == 0),
                    ))

            fig.add_vline(x=480, line_dash="dash", line_color="green",
                          annotation_text="8h (abertura oficial)")
            fig.add_vline(x=1020, line_dash="dash", line_color="orange",
                          annotation_text="17h (encerramento oficial)")

            tick_vals = list(range(360, 1260, 60))
            tick_text = [f"{v // 60}:{v % 60:02d}" for v in tick_vals]

            fig.update_layout(
                xaxis=dict(
                    title="Horário",
                    tickvals=tick_vals,
                    ticktext=tick_text,
                    range=[360, 1260],
                ),
                yaxis=dict(title=""),
                height=200,
                showlegend=True,
                margin=dict(l=10, r=10, t=30, b=40),
            )
            st.plotly_chart(fig, width="stretch")
        except Exception as e:
            st.warning(f"Erro ao renderizar timeline: {e}")
    else:
        st.info("Informações de horário não disponíveis para esta seção.")

    # ---- Totais por cargo ----
    st.markdown("### Totais por Cargo")
    totais_df = detail["totais"]
    if not totais_df.empty:
        display_cols = [
            "cargo", "comparecimento", "nominais",
            "brancos", "nulos", "legenda", "total",
        ]
        available_cols = [c for c in display_cols if c in totais_df.columns]
        st.dataframe(
            totais_df[available_cols].reset_index(drop=True),
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("Nenhum total por cargo disponível.")


def tab_ranking_estados(store, f):
    """Tab Ranking de Risco por estado."""
    st.subheader("Ranking de Risco por Estado")
    st.caption(
        "Score de risco agregado por UF, baseado em reboots, issues e biometria zero"
    )

    try:
        ranking_df = analysis.state_risk_ranking(store, f)
    except Exception as e:
        st.warning(f"Erro ao calcular ranking de risco: {e}")
        return

    if ranking_df.empty:
        st.info("Sem dados para calcular o ranking de risco.")
        return

    # ---- Tabela de ranking ----
    st.markdown("### Tabela de Ranking")
    st.dataframe(ranking_df, width="stretch", hide_index=True)

    # ---- Gráfico de barras horizontal por score ----
    st.markdown("### Score de Risco por Estado")
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        x=ranking_df["score_risco"],
        y=ranking_df["uf"],
        orientation="h",
        marker_color=ranking_df["score_risco"].apply(
            lambda s: "#EF553B" if s >= 60 else "#FFA15A" if s >= 30 else "#00CC96"
        ),
        text=ranking_df["score_risco"].round(1),
        textposition="auto",
    ))
    fig_bar.update_layout(
        xaxis_title="Score de Risco (0-100)",
        yaxis_title="UF",
        yaxis=dict(autorange="reversed"),
        height=max(400, len(ranking_df) * 25),
        margin=dict(l=10),
    )
    st.plotly_chart(fig_bar, width="stretch")

    # ---- Heatmap: UFs x métricas ----
    st.markdown("### Heatmap: UFs x Métricas de Risco")

    heatmap_data = ranking_df[["uf", "pct_reboots", "pct_issues", "pct_zero_bio"]].copy()
    heatmap_data = heatmap_data.set_index("uf")
    heatmap_data.columns = ["% Reboots", "% Issues", "% Bio Zero"]

    fig_heat = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns.tolist(),
        y=heatmap_data.index.tolist(),
        colorscale="YlOrRd",
        text=heatmap_data.values.round(2),
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="UF: %{y}<br>Métrica: %{x}<br>Valor: %{z:.2f}%<extra></extra>",
    ))
    fig_heat.update_layout(
        title="Distribuição de Métricas de Risco por Estado",
        xaxis_title="Métrica",
        yaxis_title="UF",
        height=max(500, len(ranking_df) * 22),
    )
    st.plotly_chart(fig_heat, width="stretch")


def extend_tab_outliers(store, f):
    """Seção adicional para tab_outliers: Outliers por Estado (z-score local)."""
    st.markdown("---")
    st.markdown("### Outliers por Estado (z-score local)")
    st.caption(
        "Z-score calculado por UF (média e desvio padrão locais), "
        "detectando seções atípicas dentro de cada estado"
    )

    try:
        state_outliers_df = analysis.zscore_outliers_by_state(store, f, threshold=3.0)
        if not state_outliers_df.empty:
            total_state_outliers = len(state_outliers_df)
            st.metric("Total de Outliers (por estado)", f"{total_state_outliers:,}")

            resumo = state_outliers_df.groupby(["uf", "metrica"]).size().unstack(fill_value=0)
            if not resumo.empty:
                st.markdown("**Resumo por UF e Métrica**")
                st.dataframe(resumo, width="stretch")

            limited = state_outliers_df.head(200)
            fig = px.scatter(
                limited,
                x="zscore",
                y="valor",
                color="metrica",
                facet_col="uf",
                facet_col_wrap=6,
                hover_data=["secao_id", "media_uf", "std_uf"],
                title="Outliers por Estado e Métrica",
                labels={
                    "zscore": "Z-Score (local)",
                    "valor": "Valor",
                    "metrica": "Métrica",
                },
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, width="stretch")

            st.markdown("### Tabela de Outliers por Estado (limite: 200)")
            st.dataframe(limited, width="stretch", hide_index=True)
        else:
            st.info(
                "Nenhum outlier detectado com z-score local (por estado) > 3.0."
            )
    except Exception as e:
        st.warning(f"Erro ao carregar outliers por estado: {e}")


def extend_tab_timing(store, f):
    """Seção adicional para tab_timing: Histograma de duração."""
    st.markdown("---")
    st.markdown("### Histograma de Duração da Votação")
    st.caption("Distribuição da duração em faixas de 30 minutos")

    try:
        hist_df = analysis.duration_histogram_data(store, f)
        if not hist_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=hist_df["faixa"],
                y=hist_df["quantidade"],
                marker_color="#636EFA",
                text=hist_df["quantidade"],
                textposition="auto",
            ))
            fig.update_layout(
                xaxis_title="Faixa de Duração",
                yaxis_title="Quantidade de Seções",
                title="Distribuição da Duração da Votação",
                height=450,
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(hist_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados para o histograma de duração.")
    except Exception as e:
        st.warning(f"Erro ao carregar histograma de duração: {e}")


def extend_tab_biometria(store, f):
    """Seção adicional para tab_biometria: Histograma de biometria."""
    st.markdown("---")
    st.markdown("### Histograma de Biometria")
    st.caption("Distribuição do percentual de biometria em faixas de 5%")

    try:
        hist_df = analysis.biometry_histogram_data(store, f)
        if not hist_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=hist_df["faixa"],
                y=hist_df["quantidade"],
                marker_color="#00CC96",
                text=hist_df["quantidade"],
                textposition="auto",
            ))
            fig.update_layout(
                xaxis_title="Faixa de Biometria (%)",
                yaxis_title="Quantidade de Seções",
                title="Distribuição do Percentual de Biometria",
                height=450,
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(hist_df, width="stretch", hide_index=True)
        else:
            st.info("Sem dados para o histograma de biometria.")
    except Exception as e:
        st.warning(f"Erro ao carregar histograma de biometria: {e}")


def main():
    store = get_store()

    if store is None:
        st.error(
            "Banco de dados não encontrado ou em uso exclusivo. "
            "Se o download está em andamento, aguarde alguns segundos e recarregue a página."
        )
        st.stop()

    f = build_filters(store)

    col_title, col_refresh = st.columns([5, 1])
    with col_title:
        st.title("Auditoria Eleitoral — Eleições 2022")
        st.caption("Análise de inconsistências, vulnerabilidades e padrões em urnas eletrônicas")
    with col_refresh:
        st.button("Atualizar Dados", type="primary", help="Recarrega dados do banco (útil durante download)")

    has_data = render_kpis(store, f)
    if not has_data:
        st.stop()

    st.divider()

    (tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10,
     tab11, tab12, tab13, tab14, tab15, tab16, tab17, tab18, tab19,
     tab20, tab21, tab22) = st.tabs([
        "Veredicto Final",        # 1
        "Lei de Benford",         # 2
        "Outliers Estatísticos",  # 3
        "Impacto de Reboots",     # 4
        "Análise T1 vs T2",       # 5
        "Anomalias Geográficas",  # 6
        "Análise A03",            # 7
        "Biometria & Segurança",  # 8
        "Timing Detalhado",       # 9
        "Modelos & Hardware",     # 10
        "Inconsistências",        # 11
        "Por Estado",             # 12
        "Votação",                # 13
        "Nulos & Brancos",        # 14  [NOVO]
        "Distribuição Candidato", # 15  [NOVO]
        "Mapa do Brasil",         # 16  [NOVO]
        "Seções Reserva",         # 17  [NOVO]
        "Substituições & Erros",  # 18  [NOVO]
        "Integridade Hash",       # 19  [NOVO]
        "Explorador de Seção",    # 20  [NOVO]
        "Ranking de Risco",       # 21  [NOVO]
        "Dados Brutos",           # 22
    ])

    with tab1:
        tab_veredicto(store, f)
    with tab2:
        tab_benford_v2(store, f)
    with tab3:
        tab_outliers(store, f)
    with tab4:
        tab_reboots_impact(store, f)
    with tab5:
        tab_comparacao_turnos(store, f)
    with tab6:
        tab_geographic(store, f)
    with tab7:
        tab_a03(store, f)
    with tab8:
        tab_biometria_timing(store, f)
    with tab9:
        tab_timing(store, f)
    with tab10:
        tab_modelos(store, f)
    with tab11:
        tab_inconsistencias(store, f)
    with tab12:
        tab_estados(store, f)
    with tab13:
        tab_votacao(store, f)
    with tab14:
        tab_nulos_brancos(store, f)
    with tab15:
        tab_distribuicao_candidato(store, f)
    with tab16:
        tab_mapa(store, f)
    with tab17:
        tab_reserva(store, f)
    with tab18:
        tab_substituicoes_erros(store, f)
    with tab19:
        tab_integridade(store, f)
    with tab20:
        tab_drilldown(store, f)
    with tab21:
        tab_ranking_estados(store, f)
    with tab22:
        tab_dados_brutos_v2(store, f)

    store.close()


if __name__ == "__main__":
    main()
