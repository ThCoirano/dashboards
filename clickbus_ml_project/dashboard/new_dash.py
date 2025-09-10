import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px


# ==============================
# Função utilitária
# ==============================
def carregar_csv(caminho: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(caminho, delimiter=';', encoding='utf-8-sig')
        return df
    except Exception as e:
        st.error(f"Erro ao carregar CSV: {e}")
        return pd.DataFrame()
    

# ==============================
# Resumo Geral
# ==============================
def mostrar_resumo_geral(df: pd.DataFrame):
    st.subheader("📌 Resumo Geral")

    total_clientes = df["qtd_clientes"].sum() if "qtd_clientes" in df else 0
    gmv_medio_geral = df["gmv_medio_cliente"].mean() if "gmv_medio_cliente" in df else 0
    passagens_media = df["passagens_media"].mean() if "passagens_media" in df else 0
    pct_retorno_medio = df["pct_retorno"].mean() if "pct_retorno" in df else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Clientes", f"{int(total_clientes):,}".replace(",", "."))
    col2.metric("GMV Médio Geral", f"R${gmv_medio_geral:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    col3.metric("Passagens Média Geral", f"{passagens_media:.2f}")
    col4.metric("% Retorno Médio", f"{pct_retorno_medio*100:.1f}%")


# ==============================
# Legenda dos Clusters
# ==============================
def mostrar_legenda_clusters():
    st.subheader("📖 Legenda dos Clusters")

    clusters = {
        0: "Compras intermediárias, frequência moderada, só ida",
        1: "Compras intermediárias, alta frequência, só ida",
        2: "Compras altas, muitas passagens, só ida",
        3: "Compras baixas, frequência moderada, só ida",
        4: "Compras altas, muitas passagens, só ida",
        5: "Compras altas, muitas passagens, só ida",
        6: "Compras intermediárias, alta frequência, ida e volta",
        7: "Compras intermediárias, frequência moderada, só ida"
    }

    for i in range(0, len(clusters), 4):
        cols = st.columns(4)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < len(clusters):
                col.markdown(
                    f"""
                    <div style="
                        background-color:#1E1E1E;
                        padding:10px;
                        border-radius:8px;
                        margin-bottom:10px;
                        box-shadow:0 2px 4px rgba(0,0,0,0.15);
                        min-height:80px;
                    ">
                        <div style="color:#4CAF50; font-weight:bold; margin-bottom:4px;">
                            Cluster {idx}
                        </div>
                        <div style="color:white; font-size:13px; line-height:1.3;">
                            {clusters[idx]}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


# ==============================
# Tabela Perfil Cluster
# ==============================
def mostrar_tabela_base_perfil_cluster(caminho_csv: str):
    st.header("📊 Tabela Base Perfil Cluster")
    df = carregar_csv(caminho_csv)
    df = df.reset_index(drop=True)

    if df.empty:
        st.warning("Nenhum dado encontrado no CSV.")
        return
    
    if "perfil_textual" in df.columns:
        df = df.drop(columns=["perfil_textual"])
    
    if "gmv_medio_cliente" in df.columns:
        df["gmv_medio_cliente"] = df["gmv_medio_cliente"].apply(
            lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )
    if "qtd_clientes" in df.columns:
        df["qtd_clientes"] = df["qtd_clientes"].apply(lambda x: f"{int(x):,}".replace(",", "."))
    if "pct_retorno" in df.columns:
        df["pct_retorno"] = df["pct_retorno"].apply(lambda x: f"{x*100:.2f}%")
    if "percentual_retorno" in df.columns:
        df["percentual_retorno"] = df["percentual_retorno"].apply(lambda x: f"{x:.2f}%")

    st.dataframe(df, use_container_width=True)


# ==============================
# Gráfico de Picos
# ==============================
def mostrar_picos_horarios(caminho_csv: str):
    st.subheader("⏰ Análise de Picos de Horas por Cluster")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de picos de horários.")
        return
    
    df = df.reset_index(drop=True)

    fig1 = px.bar(
        df,
        x="cluster",
        y="compras",
        color="hora",
        barmode="group",
        title="Compras por Hora e Cluster",
        labels={"compras": "Nº Compras", "cluster": "Cluster", "hora": "Hora do Dia"}
    )
    st.plotly_chart(fig1, use_container_width=True)


# ==============================
# Ranking de Horários
# ==============================
def mostrar_rank_horarios(caminho_csv: str, top_n: int = 5):
    st.subheader("🏆 Ranking de Horários por Cluster")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de picos de horários.")
        return

    df = df[df["rank_hora"].between(1, top_n)].copy()
    df["hora_fmt"] = df["hora"].apply(lambda x: f"{int(x):02d}h00")

    matriz = (
        df.pivot_table(index="cluster", columns="rank_hora", values="hora_fmt", aggfunc="first")
        .reindex(columns=range(1, top_n + 1))
        .rename(columns=lambda c: f"Top {c}")
        .sort_index()
    )
    matriz = matriz.dropna(how="all").fillna("—")

    st.table(matriz)


# ==============================
# KPIs de Picos
# ==============================
def mostrar_kpis_picos(caminho_csv: str):
    st.subheader("📊 KPIs de Picos de Horários")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de picos de horários.")
        return

    df = df.reset_index(drop=True)
    df["hora_fmt"] = df["hora"].apply(lambda x: f"{int(x):02d}h00")

    df_pico = df.loc[df.groupby("cluster")["compras"].idxmax()].copy()

    cluster_heavy = df_pico.loc[df_pico["heavy_ratio"].idxmax()]
    cluster_gmv = df_pico.loc[df_pico["gmv"].idxmax()]
    cluster_compras = df_pico.loc[df_pico["compras"].idxmax()]
    cluster_score = df.groupby("cluster")["score"].mean().idxmax()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Cluster mais dependente de Heavy Users",
        f"Cluster {cluster_heavy['cluster']} ({cluster_heavy['heavy_ratio']*100:.1f}%)")
    col2.metric("Maior GMV em Pico",
        f"R$ {cluster_gmv['gmv']:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."),
        f"Cluster {cluster_gmv['cluster']} às {cluster_gmv['hora_fmt']}")
    col3.metric("Maior nº Compras em Pico",
        f"{cluster_compras['compras']:,}".replace(",", "."),
        f"Cluster {cluster_compras['cluster']} às {cluster_compras['hora_fmt']}")
    col4.metric("Maior Score Médio", f"Cluster {cluster_score}")

    resumo = df_pico[["cluster", "hora_fmt", "compras", "gmv", "heavy_ratio"]].copy()
    resumo = resumo.rename(columns={
        "hora_fmt": "Pico de Compras",
        "compras": "Compras no Pico",
        "gmv": "GMV no Pico",
        "heavy_ratio": "% Heavy Users no Pico"
    })
    resumo["Compras no Pico"] = resumo["Compras no Pico"].apply(lambda x: f"{int(x):,}".replace(",", "."))
    resumo["GMV no Pico"] = resumo["GMV no Pico"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
    resumo["% Heavy Users no Pico"] = resumo["% Heavy Users no Pico"].apply(lambda x: f"{x*100:.1f}%")
    resumo = resumo.reset_index(drop=True)

    st.markdown("### 📌 Resumo por Cluster")
    st.table(resumo)


# ==============================
# Análise de Picos (com abas)
# ==============================
def mostrar_analise_picos(caminho_csv: str):
    st.header("📊 Análise de Picos de Horários")

    tab_resumo, tab_detalhe = st.tabs(["📌 Resumo", "🔎 Detalhe"])

    with tab_resumo:
        mostrar_kpis_picos(caminho_csv)

    with tab_detalhe:
        mostrar_picos_horarios(caminho_csv)
        mostrar_rank_horarios(caminho_csv)

def grafico_qtd_recomendacoes(df: pd.DataFrame):
    st.subheader("📊 Recomendações de Tickets Por Cluster")

    # 1) Agrupar e ordenar
    resumo = (
        df.groupby("cluster", as_index=False)["potencial_bruto_tickets"]
          .sum()
          .sort_values("potencial_bruto_tickets", ascending=False)
    )

    # 2) Converter para rótulo categórico
    resumo["cluster_lab"] = resumo["cluster"].apply(lambda x: f"Cluster {int(x)}")

    # 3) Plot + ordem categórica fixa (do maior para o menor)
    fig = px.bar(
        resumo,
        x="cluster_lab",
        y="potencial_bruto_tickets",
        text="potencial_bruto_tickets",
        title="Quantidade de Recomendações de Tickets por Cluster",
        labels={"cluster_lab": "Cluster", "potencial_bruto_tickets": "Qtd de Recomendações"}
    )
    fig.update_traces(textposition="outside")
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=resumo["cluster_lab"].tolist()  # mantém a ordem do DataFrame (já decrescente)
    )

    st.plotly_chart(fig, use_container_width=True)

def mostrar_recomendacoes(caminho_csv: str):
    st.header("🎯 Recomendações - Produto Bruto")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de recomendações.")
        return
    
    df = df.reset_index(drop=True)

    # ==============================
    # KPIs executivos
    # ==============================
    total_recos = df["qtd_recomendacoes"].sum()
    total_tickets = df["potencial_ganho_tickets"].sum()
    total_ganho = df["potencial_ganho_reais_60"].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Recomendações", f"{int(total_recos):,}".replace(",", "."))
    col2.metric("Tickets Potenciais (60%)", f"{int(total_tickets):,}".replace(",", "."))
    col3.metric("Receita Potencial (60%)", 
        f"R$ {total_ganho:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

    st.markdown("---")

    # ==============================
    # Gráfico - Recomendações por cluster
    # ==============================
    grafico_qtd_recomendacoes(df)

    # ==============================
    # Gráfico - Top 10 clusters/horas por receita potencial
    # ==============================
    # top10 = df.sort_values("potencial_ganho_reais_60", ascending=False).head(10)

    # fig = px.bar(
    #     top10,
    #     x="potencial_ganho_reais_60",
    #     y=top10["cluster"].astype(str) + " - " + top10["hora"].astype(str) + "h",
    #     orientation="h",
    #     title="💰 Top 10 Clusters/Horas - Receita Potencial (60%)",
    #     labels={
    #         "potencial_ganho_reais_60": "Receita Potencial (R$)",
    #         "y": "Cluster - Hora"
    #     },
    #     text_auto=".2s"
    # )
    # st.plotly_chart(fig, use_container_width=True)
    
def mostrar_detalhe_recomendacoes(caminho_csv: str):
    st.subheader("🔎 Detalhe das Recomendações")

    # Lê o CSV completo (mantém todas as colunas)
    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de detalhes de recomendações.")
        return

    df = df.reset_index(drop=True)

    # ------------------------------
    # Filtro de cluster
    # ------------------------------
    if "cluster" not in df.columns:
        st.error("A coluna 'cluster' não existe no CSV de detalhe.")
        return

    clusters = sorted(df["cluster"].unique())
    cluster_sel = st.selectbox("Selecione o Cluster", clusters)

    df_filtrado = df[df["cluster"] == cluster_sel].copy()
    if df_filtrado.empty:
        st.info("Não há linhas para o cluster selecionado.")
        return

    # ------------------------------
    # Mostra tabela completa
    # ------------------------------
    st.dataframe(df_filtrado, use_container_width=True)

    st.markdown("---")

    # ------------------------------
    # Gráfico de análise das viações recomendadas
    # ------------------------------
    if "viacao_recomendada" in df_filtrado.columns and "oportunidade" in df_filtrado.columns:
        df_filtrado["oportunidade"] = pd.to_numeric(df_filtrado["oportunidade"], errors="coerce")
        df_grafico = df_filtrado.groupby("viacao_recomendada", as_index=False)["oportunidade"].sum()

        fig = px.bar(
            df_grafico,
            x="viacao_recomendada",
            y="oportunidade",
            text="oportunidade",
            title=f"🚍 Oportunidade por Viação Recomendada - Cluster {cluster_sel}",
            labels={"viacao_recomendada": "Viação", "oportunidade": "Oportunidade (R$)"}
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)


# ==============================
# Main
# ==============================
def main():
    st.set_page_config(page_title="Dashboard ClickBus", layout="wide")
    st.title("Painel Estratégico - ClickBus")

    # ==============================
    # Caminhos dos CSVs
    # ==============================
    base_csv = Path("./data/base_perfil_cluster.csv")
    picos_csv = Path("./data/picos_horarios.csv")
    recos_csv = Path("./data/top_vendas_por_hora_conversao_60.csv")
    detalhe_recos_csv = Path("./data/detalhe_recomendacao.csv")

    # ==============================
    # Perfil de Cluster
    # ==============================
    if base_csv.exists():
        df_base = carregar_csv(base_csv)
        if not df_base.empty:
            mostrar_resumo_geral(df_base)
            mostrar_legenda_clusters()
            mostrar_tabela_base_perfil_cluster(base_csv)
    else:
        st.warning("CSV base_perfil_cluster.csv não encontrado.")

    # ==============================
    # Análise de Picos de Horários
    # ==============================
    if picos_csv.exists():
        mostrar_analise_picos(picos_csv)
    else:
        st.warning("CSV picos_horarios.csv não encontrado.")

    # ==============================
    # Recomendações (Resumo + Detalhe)
    # ==============================
    if recos_csv.exists():
        st.header("🎯 Recomendações - Produto Bruto")
        mostrar_recomendacoes(recos_csv)

    if detalhe_recos_csv.exists():
        mostrar_detalhe_recomendacoes(detalhe_recos_csv)

    if not recos_csv.exists() and not detalhe_recos_csv.exists():
        st.warning("Nenhum CSV de recomendações encontrado.")


if __name__ == "__main__":
    main()