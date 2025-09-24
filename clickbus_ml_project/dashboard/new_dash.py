import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px


# ==============================
# Função utilitária
# ==============================
def carregar_csv(caminho: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(caminho, delimiter=';', encoding='utf-8')
        return df
    except Exception as e:
        st.error(f"Erro ao carregar CSV: {e}")
        return pd.DataFrame()
    

# ==============================
# Visão Geral
# ==============================
def mostrar_resumo_geral(df: pd.DataFrame):
    st.subheader("📌 Visão Geral do Negócio")

    total_clientes = df["qtd_clientes"].sum() if "qtd_clientes" in df else 0
    gmv_medio_geral = df["gmv_medio_cliente"].mean() if "gmv_medio_cliente" in df else 0
    passagens_media = df["passagens_media"].mean() if "passagens_media" in df else 0
    pct_retorno_medio = df["pct_retorno"].mean() if "pct_retorno" in df else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Base Ativa de Clientes", f"{int(total_clientes):,}".replace(",", "."))
    col2.metric("GMV Médio por Cliente", f"R${gmv_medio_geral:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    col3.metric("Passagens por Cliente", f"{passagens_media:.2f}")
    col4.metric("Taxa Média de Retorno", f"{pct_retorno_medio*100:.1f}%")


# ==============================
# Perfis de Clientes
# ==============================
def mostrar_legenda_clusters():
    st.subheader("📖 Perfis de Clientes (Clusters)")

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
# Detalhamento de Clusters
# ==============================
def mostrar_tabela_base_perfil_cluster(caminho_csv: str):
    st.header("📊 Detalhamento de Clusters")
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
# Análise de Comportamento por Horário
# ==============================
def mostrar_picos_horarios(caminho_csv: str):
    st.subheader("⏰ Análise de Comportamento por Horário")

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
        title="Distribuição de Compras por Horário",
        labels={"compras": "Nº Compras", "cluster": "Cluster", "hora": "Hora do Dia"}
    )
    st.plotly_chart(fig1, use_container_width=True)


# ==============================
# Ranking de Horários
# ==============================
def mostrar_rank_horarios(caminho_csv: str, top_n: int = 5):
    st.subheader("🏆 Top Horários de Engajamento")

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
# KPIs de Alta Performance
# ==============================
def mostrar_kpis_picos(caminho_csv: str):
    st.subheader("📊 KPIs de Alta Performance")

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
    col1.metric("Cluster com Maior Dependência de Heavy Users",
        f"Cluster {cluster_heavy['cluster']} ({cluster_heavy['heavy_ratio']*100:.1f}%)")
    col2.metric("Maior Potencial de Receita no Pico",
        f"R$ {cluster_gmv['gmv']:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."),
        f"Cluster {cluster_gmv['cluster']} às {cluster_gmv['hora_fmt']}")
    col3.metric("Maior Volume de Compras no Pico",
        f"{cluster_compras['compras']:,}".replace(",", "."),
        f"Cluster {cluster_compras['cluster']} às {cluster_compras['hora_fmt']}")
    col4.metric("Cluster com Melhor Desempenho Relativo", f"Cluster {cluster_score}")

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

    st.markdown("### 📌 Resumo Executivo por Cluster")
    st.table(resumo)


# ==============================
# Recomendações Estratégicas
# ==============================
def grafico_qtd_recomendacoes(df: pd.DataFrame):
    st.subheader("📊 Oportunidade de Tickets por Cluster")

    resumo = (
        df.groupby("cluster", as_index=False)["potencial_bruto_tickets"]
          .sum()
          .sort_values("potencial_bruto_tickets", ascending=False)
    )
    resumo["cluster_lab"] = resumo["cluster"].apply(lambda x: f"Cluster {int(x)}")

    fig = px.bar(
        resumo,
        x="cluster_lab",
        y="potencial_bruto_tickets",
        text="potencial_bruto_tickets",
        title="Oportunidade de Tickets por Cluster",
        labels={"cluster_lab": "Cluster", "potencial_bruto_tickets": "Qtd de Tickets"}
    )
    fig.update_traces(textposition="outside")
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=resumo["cluster_lab"].tolist()
    )
    st.plotly_chart(fig, use_container_width=True)


def mostrar_recomendacoes(caminho_csv: str):
    st.header("🎯 Recomendações Estratégicas")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de recomendações.")
        return
    
    df = df.reset_index(drop=True)

    total_recos = df["qtd_recomendacoes"].sum()
    total_tickets = df["potencial_ganho_tickets"].sum()
    total_ganho = df["potencial_ganho_reais_60"].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("Volume de Recomendações", f"{int(total_recos):,}".replace(",", "."))
    col2.metric("Oportunidade de Tickets (60%)", f"{int(total_tickets):,}".replace(",", "."))
    col3.metric("Receita Incremental Potencial (60%)", 
        f"R$ {total_ganho:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

    st.markdown("---")
    grafico_qtd_recomendacoes(df)


def mostrar_detalhe_recomendacoes(caminho_csv: str):
    st.subheader("🔎 Análise Detalhada de Recomendações")

    df = carregar_csv(caminho_csv)
    if df.empty:
        st.warning("Nenhum dado encontrado no CSV de detalhes de recomendações.")
        return

    df = df.reset_index(drop=True)

    if "cluster" not in df.columns:
        st.error("A coluna 'cluster' não existe no CSV de detalhe.")
        return

    clusters = sorted(df["cluster"].unique())
    cluster_sel = st.selectbox("Selecione o Cluster", clusters)

    df_filtrado = df[df["cluster"] == cluster_sel].copy()
    if df_filtrado.empty:
        st.info("Não há linhas para o cluster selecionado.")
        return

    st.dataframe(df_filtrado, use_container_width=True)
    st.markdown("---")

    if "viacao_recomendada" in df_filtrado.columns and "oportunidade" in df_filtrado.columns:
        df_filtrado["oportunidade"] = pd.to_numeric(df_filtrado["oportunidade"], errors="coerce")
        df_grafico = df_filtrado.groupby("viacao_recomendada", as_index=False)["oportunidade"].sum()

        fig = px.bar(
            df_grafico,
            x="viacao_recomendada",
            y="oportunidade",
            text="oportunidade",
            title=f"🚍 Receita Potencial por Parceiro (Viação) - Cluster {cluster_sel}",
            labels={"viacao_recomendada": "Viação", "oportunidade": "Receita Potencial (R$)"}
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)


# ==============================
# Main
# ==============================
def main():
    st.set_page_config(page_title="Painel Executivo de Clusters e Oportunidades - ClickBus", layout="wide")
    st.title("Painel Executivo de Clusters e Oportunidades - ClickBus")

    base_csv = Path("clickbus_ml_project/data/base_perfil_cluster.csv")
    picos_csv = Path("clickbus_ml_project/data/picos_horarios.csv")
    recos_csv = Path("clickbus_ml_project/data/top_vendas_por_hora_conversao_60.csv")
    detalhe_recos_csv = Path("clickbus_ml_project/data/detalhe_recomendacao.csv")

    if base_csv.exists():
        df_base = carregar_csv(base_csv)
        if not df_base.empty:
            mostrar_resumo_geral(df_base)
            mostrar_legenda_clusters()
            mostrar_tabela_base_perfil_cluster(base_csv)
    else:
        st.warning("CSV base_perfil_cluster.csv não encontrado.")

    if picos_csv.exists():
        mostrar_kpis_picos(picos_csv)
        mostrar_picos_horarios(picos_csv)
        mostrar_rank_horarios(picos_csv)
    else:
        st.warning("CSV picos_horarios.csv não encontrado.")

    if recos_csv.exists():
        mostrar_recomendacoes(recos_csv)

    if detalhe_recos_csv.exists():
        mostrar_detalhe_recomendacoes(detalhe_recos_csv)

    if not recos_csv.exists() and not detalhe_recos_csv.exists():
        st.warning("Nenhum CSV de recomendações encontrado.")


if __name__ == "__main__":
    main()
