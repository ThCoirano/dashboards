# dashboard/streamlit_cluster_dashboard_clickbus.py

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Painel Estratégico ClickBus", layout="wide")
st.title("Painel Estratégico ClickBus")
st.markdown("Dashboard para **clusters, engajamento, horários de pico e recomendações de viações menores** (via CSVs).")

# ======================
# Paths dos CSVs
# ======================
DATA_DIR = Path("data")
CSV_PERFIS = DATA_DIR / "base_perfil_cluster.csv"
CSV_PICOS  = DATA_DIR / "viagens_picos_horarios_top5_horarios_de_pico_por_cluster.csv"
CSV_RECOS  = DATA_DIR / "ranked_Top_hora_pico_por_cluster.csv"
CSV_ROTAS  = DATA_DIR / "pares_clientes_mais_uma_rota.csv"

# ======================
# Helpers
# ======================
@st.cache_data
def load_csvs():
    df_perfis = pd.read_csv(CSV_PERFIS, sep=';')
    df_picos  = pd.read_csv(CSV_PICOS,  sep=';')
    df_recos  = pd.read_csv(CSV_RECOS,  sep=';')
    df_rotas  = pd.read_csv(CSV_ROTAS,  sep=';')

    # Tipagem segura
    df_perfis = df_perfis.astype({
        "cluster": "int64",
        "qtd_clientes": "int64",
        "gmv_medio_cliente": "float64",
        "gmv_mediano_cliente": "float64",
        "passagens_media": "float64",
        "pct_retorno": "float64"
    }, errors="ignore")

    df_picos = df_picos.astype({
        "cluster": "int64",
        "hora": "int64",
        "tickets": "int64",
        "gmv": "float64",
        "pct_heavy_users": "float64",
        "score": "float64",
        "rank_hora": "int64"
    }, errors="ignore")

    df_recos = df_recos.astype({
        "cluster": "int64",
        "hora": "string",  # tratamos depois
        "tickets_peak": "int64",
        "pct_heavy_users": "float64",
        "share_atual_pct": "float64",
        "oportunidade": "float64"
    }, errors="ignore")

    return df_perfis, df_picos, df_recos, df_rotas


def color_title(row):
    if row.get("pct_retorno", 0) >= 0.5:
        return "#2ECC71"   # Verde
    if row.get("gmv_medio_cliente", 0) >= 1500:
        return "#F1C40F"   # Dourado
    return "#E67E22"       # Laranja


# ======================
# Load
# ======================
if not (CSV_PERFIS.exists() and CSV_PICOS.exists() and CSV_RECOS.exists()):
    st.error("CSV(s) não encontrados em ./data. Gere os arquivos e tente novamente.")
    st.stop()

df_perfis, df_picos, df_recos, df_rotas = load_csvs()

# ======================
# KPIs
# ======================
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Clientes", f"{int(df_perfis['qtd_clientes'].sum()):,}")
col2.metric("GMV Médio Geral", f"R${df_perfis['gmv_medio_cliente'].mean():,.2f}")
col3.metric("Passagens Média Geral", f"{df_perfis['passagens_media'].mean():.2f}")
col4.metric("% Retorno Médio", f"{(df_perfis['pct_retorno'].mean()*100):.1f}%")

# ======================
# Filtro de clusters
# ======================
clusters = sorted(df_perfis["cluster"].unique().tolist())
cluster_filter = st.multiselect(
    "Selecione clusters",
    options=clusters,
    default=clusters
)
df_perfis = df_perfis[df_perfis["cluster"].isin(cluster_filter)]
df_picos  = df_picos[df_picos["cluster"].isin(cluster_filter)]
df_recos  = df_recos[df_recos["cluster"].isin(cluster_filter)]

# ======================
# Perfis por cluster (cards)
# ======================
st.subheader("Clusters Estratégicos (Perfis de Engajamento)")
num_cols = 4
for i in range(0, len(df_perfis), num_cols):
    cols = st.columns(num_cols)
    for j, row in enumerate(df_perfis.iloc[i:i+num_cols].to_dict(orient="records")):
        col = cols[j]
        titulo_cor = color_title(row)
        col.markdown(f"<h4 style='color:{titulo_cor}'>Cluster {row['cluster']}</h4>", unsafe_allow_html=True)
        col.markdown(f"**Clientes:** {int(row['qtd_clientes']):,}")
        col.markdown(f"**GMV Médio:** R${row['gmv_medio_cliente']:.2f}")
        col.markdown(f"**Passagens Médias:** {row['passagens_media']:.2f}")
        col.markdown(f"**% Retorno:** {row['pct_retorno']*100:.1f}%")
        col.markdown(f"**Perfil:** {row['perfil_textual']}")

        # Badges simples
        if row["pct_retorno"] >= 0.7:
            col.markdown("✅ **Alta fidelidade**")
        elif row["gmv_medio_cliente"] >= 1500:
            col.markdown("⭐ **Cliente premium**")
        else:
            col.markdown("💡 **Oportunidade de crescimento**")

# ======================
# Horários de pico
# ======================
st.subheader("Horários de Pico por Cluster")
if len(df_picos):
    fig_pico = px.bar(
        df_picos.sort_values(["cluster", "rank_hora"]),
        x="hora", y="score", color="cluster", barmode="group",
        hover_data=["tickets", "gmv", "pct_heavy_users", "rank_hora"],
        title="Score de Demanda por Hora e Cluster (ponderado por heavy users)"
    )
    st.plotly_chart(fig_pico, use_container_width=True)
else:
    st.info("Sem dados de picos para os clusters selecionados.")

# ======================
# Recomendações por cluster (quadros)
# ======================
st.subheader("Recomendações para Viação Menor por Cluster")
if len(df_recos):
    cluster_sel = st.selectbox("Selecione um cluster", sorted(df_recos["cluster"].unique().tolist()))
    recos_cluster = df_recos[df_recos["cluster"] == cluster_sel].copy()

    # Força hora para numérico
    recos_cluster["hora"] = pd.to_numeric(recos_cluster["hora"], errors="coerce")
    recos_cluster.sort_values("oportunidade", ascending=False, inplace=True)

    for _, r in recos_cluster.iterrows():
        hora_fmt = f"{int(r['hora']):02d}h" if pd.notna(r['hora']) else "N/A"
        st.markdown(f"""
**{r['rota']}** — **{hora_fmt}**  
- **Viação recomendada:** {r['viacao_recomendada']}  
- **Tickets no pico:** {int(r['tickets_peak'])}  
- **% Heavy users:** {r['pct_heavy_users']:.1f}%  
- **Share atual:** {r['share_atual_pct']:.1f}%  
- **Oportunidade:** {r['oportunidade']:.1f}  
- 📝 {r['recomendacao']}
""")
else:
    st.info("Sem recomendações para os clusters selecionados.")

# ======================
# Rotas relacionadas (extra)
# ======================
if len(df_rotas):
    st.subheader("Rotas Relacionadas (Coocorrência de Clientes)")
    df_rotas = df_rotas.sort_values("qtd_clientes", ascending=False).head(20)
    fig_rotas = px.scatter(
        df_rotas,
        x="rota_origem", y="rota_relacionada",
        size="qtd_clientes", color="pct_associacao",
        hover_data=["qtd_clientes", "pct_associacao"],
        title="Top 20 Rotas Relacionadas por Coocorrência de Clientes"
    )
    fig_rotas.update_layout(height=700)
    st.plotly_chart(fig_rotas, use_container_width=True)

# ======================
# Rodapé
# ======================
st.markdown("---")
st.markdown("🔎 **ClickBus Intelligence** – Detecção de clusters, análise de picos e recomendações para fortalecer companhias menores.")
