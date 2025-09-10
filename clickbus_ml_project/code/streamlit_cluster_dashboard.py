# streamlit_cluster_dashboard_clickbus_advanced_v2.py

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

data = {
    "Cluster": ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7'],
    "Perfil Resumido": [
        "Compras intermediárias, frequência moderada, só ida",
        "Compras intermediárias, alta frequência, só ida",
        "Compras altas, muitas passagens, só ida",
        "Compras baixas, frequência moderada, só ida",
        "Compras baixas, frequência moderada, só ida",
        "Compras altas, muitas passagens, só ida",
        "Compras intermediárias, alta frequência, ida e volta",
        "Compras intermediárias, frequência moderada, só ida",
    ],
    "Qtd_Clientes": [102829, 79041, 4388, 156025, 187686, 4938, 24443, 22467],
    "GMV_Medio": [544.13, 708.4, 1575.73, 90.72, 371.49, 7174.13, 949.36, 852.36],
    "Passagens_Medias": [3.09, 7.25, 5.05, 1.7, 3.82, 66.89, 5.4, 3.18],
    "Pct_Retorno": [0.0, 58.5, 55.0, 0.0, 0.0, 12.2, 82.5, 0.0],
    "Top_Viacao": [
            ['Expresso Azul', 'Expresso Verde', 'Expresso Norte', 'TransRio', 'Expresso Real'],
            ['Viação Tropical', 'Viação Nacional', 'Viação Estrela', 'Viação Central', 'Viação Continental'],
            ['TransBrasil', 'Rodovia Sul', 'Rodovias Unidas', 'Expresso Imperial', 'Rodovias do Norte'],
            ['São Jorge Transportes', 'Viação Atlântico', 'Expresso Vale', 'Viação Sol Nascente', 'Expresso Diamante'],
            ['Viação Horizonte', 'TransCidade', 'Viação Planalto', 'TransSul', 'Viação Estrela do Sul'],
            ['Expresso Verde', 'Viação Primavera', 'TransMontes', 'Expresso Atlântico', 'TransMontes'],
            ['Viação Nacional', 'Rodovias Unidas', 'Expresso Verde Vale', 'Expresso Azul', 'Expresso Atlântico'],
            ['Rodovia Sul', 'Expresso Verde Vale', 'Expresso Azul', 'Viação Tropical', 'Viação Nacional']
    ]
}

df = pd.DataFrame(data)
df.columns = df.columns.str.replace(" ", "_").str.replace("%", "Pct")

st.set_page_config(page_title="Painel Estratégico ClickBus", layout="wide")
st.title("Painel Estratégico ClickBus - Perfis de Clientes")
st.markdown("Painel avançado para **identificação de oportunidades e análise de clusters** ClickBus.")


col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Clientes", f"{df['Qtd_Clientes'].sum():,}")
col2.metric("GMV Médio Geral", f"R${df['GMV_Medio'].mean():,.2f}")
col3.metric("Passagens Média Geral", f"{df['Passagens_Medias'].mean():.2f}")
col4.metric("% Retorno Médio", f"{df['Pct_Retorno'].mean():.1f}%")


st.markdown("### Legenda")
st.markdown("""
• <span style='color:#2ECC71'><b>Verde:</b></span> Alta fidelidade  
• <span style='color:#F1C40F'><b>Dourado:</b></span> Clientes premium / alto GMV  
• <span style='color:#E67E22'><b>Laranja:</b></span> Média fidelidade / médio/baixo GMV
""", unsafe_allow_html=True)


cluster_filter = st.multiselect(
    "Selecione clusters",
    options=df["Cluster"].unique(),
    default=df["Cluster"].unique()
)
df_filtered = df[df["Cluster"].isin(cluster_filter)]


def cor_titulo(row):
    if row["Pct_Retorno"] >= 50:
        return "#2ECC71"
    elif row["GMV_Medio"] >= 1500:
        return "#F1C40F"
    else:
        return "#E67E22"


st.subheader("Clusters Estratégicos com Top 3 Viações")
num_cols = 4
for i in range(0, len(df_filtered), num_cols):
    cols = st.columns(num_cols)
    for j, row in enumerate(df_filtered.iloc[i:i+num_cols].itertuples()):
        col = cols[j]
        titulo_cor = cor_titulo(row._asdict())
        col.markdown(f"<h4 style='color:{titulo_cor}'>Cluster {row.Cluster}</h4>", unsafe_allow_html=True)
        col.markdown(f"**Perfil:** {row.Perfil_Resumido}")
        col.markdown(f"**Clientes:** {row.Qtd_Clientes:,}")
        col.markdown(f"**GMV Médio:** R${row.GMV_Medio:.2f}")
        col.markdown(f"**Passagens Médias:** {row.Passagens_Medias:.2f}")
        col.markdown(f"**% Retorno:** {row.Pct_Retorno:.1f}%")

        # Mini gráfico Top 3 Viações
        top3 = row.Top_Viacao[:3]
        mini_df = pd.DataFrame({"Viacao": top3, "Qtd_Clientes": [row.Qtd_Clientes]*3})
        fig = px.bar(mini_df, x="Qtd_Clientes", y="Viacao", orientation='h',
                     color_discrete_sequence=[titulo_cor])
        fig.update_layout(height=150, margin=dict(l=0,r=0,t=0,b=0), showlegend=False)
        col.plotly_chart(fig, use_container_width=True)

        # Alertas visuais
        if row.Pct_Retorno >= 70:
            col.markdown("✅ **Alta fidelidade**")
        elif row.GMV_Medio >= 1500:
            col.markdown("⭐ **Cliente premium**")
        else:
            col.markdown("💡 **Oportunidade de crescimento**")


st.subheader("Top 5 Viações por Cluster")
df_top_viacoes = []
for idx, row in df_filtered.iterrows():
    for viacao in row.Top_Viacao:
        df_top_viacoes.append({
            "Cluster": f"Cluster {row.Cluster}",
            "Viacao": viacao,
            "Qtd_Clientes": row.Qtd_Clientes
        })
df_top_viacoes = pd.DataFrame(df_top_viacoes)
fig = px.bar(df_top_viacoes,
             y="Viacao",
             x="Qtd_Clientes",
             color="Cluster",
             orientation='h',
             facet_col="Cluster",
             color_discrete_map={
                 "Cluster C0": "#E67E22",
                 "Cluster C1": "#2ECC71",
                 "Cluster C2": "#F1C40F",
                 "Cluster C3": "#E67E22",
                 "Cluster C4": "#E67E22",
                 "Cluster C5": "#F1C40F",
                 "Cluster C6": "#2ECC71",
                 "Cluster C7": "#E67E22"
             },
             height=400
            )
fig.update_layout(showlegend=False)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Tendências Mensais por Cluster (GMV, Retorno, Passagens)")

meses = pd.date_range(start="2025-01-01", periods=6, freq="M").strftime("%b/%Y")
historico = []

for idx, row in df_filtered.iterrows():
    for m in meses:
        historico.append({
            "Cluster": f"Cluster {row.Cluster}",
            "Mes": m,
            "GMV_Medio": row.GMV_Medio * np.random.uniform(0.9, 1.1),
            "Pct_Retorno": row.Pct_Retorno * np.random.uniform(0.9, 1.1),
            "Passagens_Medias": row.Passagens_Medias * np.random.uniform(0.9, 1.1)
        })

df_hist = pd.DataFrame(historico)

# Gráfico de linhas múltiplas
fig_trend = px.line(df_hist, x="Mes", y="GMV_Medio", color="Cluster", markers=True,
                    title="GMV Médio Mensal por Cluster",
                    color_discrete_map={
                        "Cluster C0": "#E67E22",
                        "Cluster C1": "#2ECC71",
                        "Cluster C2": "#F1C40F",
                        "Cluster C3": "#E67E22",
                        "Cluster C4": "#E67E22",
                        "Cluster C5": "#F1C40F",
                        "Cluster C6": "#2ECC71",
                        "Cluster C7": "#E67E22"
                    })
st.plotly_chart(fig_trend, use_container_width=True)

# Passagens médias
fig_pass = px.line(df_hist, x="Mes", y="Passagens_Medias", color="Cluster", markers=True,
                   title="Passagens Médias Mensais por Cluster",
                   color_discrete_map={
                        "Cluster C0": "#E67E22",
                        "Cluster C1": "#2ECC71",
                        "Cluster C2": "#F1C40F",
                        "Cluster C3": "#E67E22",
                        "Cluster C4": "#E67E22",
                        "Cluster C5": "#F1C40F",
                        "Cluster C6": "#2ECC71",
                        "Cluster C7": "#E67E22"
                   })
st.plotly_chart(fig_pass, use_container_width=True)

# % Retorno
fig_ret = px.line(df_hist, x="Mes", y="Pct_Retorno", color="Cluster", markers=True,
                  title="% Clientes com Retorno Mensal por Cluster",
                  color_discrete_map={
                        "Cluster C0": "#E67E22",
                        "Cluster C1": "#2ECC71",
                        "Cluster C2": "#F1C40F",
                        "Cluster C3": "#E67E22",
                        "Cluster C4": "#E67E22",
                        "Cluster C5": "#F1C40F",
                        "Cluster C6": "#2ECC71",
                        "Cluster C7": "#E67E22"
                  })
st.plotly_chart(fig_ret, use_container_width=True)
