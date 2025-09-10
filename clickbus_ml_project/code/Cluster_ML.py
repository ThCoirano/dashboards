import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, text
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime
from utils.database import connectdatabase


class ML_Clickbus:
    """
    Solução de Inteligência de Dados para Companhias Regionais:
    - Segmenta clientes em clusters (alta fidelidade, premium, baixa recorrência).
    - Detecta heavy users (alta compra) por cluster.
    - Analisa horários de pico ponderando heavy users.
    - Recomenda viações menores em rotas e horários promissores.
    """

    def __init__(self, db_uri, tabela_origem, tabela_destino,
                 n_clusters=8, chunksize=100000, schema="estudo"):
        self.db_uri = db_uri
        self.tabela_origem = tabela_origem
        self.tabela_destino = tabela_destino
        self.n_clusters = n_clusters
        self.chunksize = chunksize
        self.schema = schema
        self.engine = None
        self.con_iEvo = None
        self.modelo = None
        self.scaler = None

    # ==============================================
    # Setup / Infra
    # ==============================================
    def criar_conexao(self):
        self.con_iEvo = connectdatabase("db/iEvo")
        self.engine = create_engine(self.db_uri)
        print("Conexão com banco criada.")

    def criar_tabela_destino(self):
        create_table_sql = f"""
        DROP TABLE IF EXISTS {self.schema}.{self.tabela_destino};
        CREATE TABLE {self.schema}.{self.tabela_destino} (
            fk_contact VARCHAR,
            cluster INT NOT NULL,
            gmv_success NUMERIC,
            total_tickets_quantity_success INT,
            ticket_medio NUMERIC,
            tem_retorno INT,
            hora_compra INT,
            dia_semana INT,
            nmcidadeorigem VARCHAR(200),
            nmuforigem VARCHAR(5),
            nmcidadedestino VARCHAR(200),
            nmufdestino VARCHAR(5),
            nm_company VARCHAR(200),
            date_purchase TIMESTAMP,
            data_processamento TIMESTAMP,
            algoritmo VARCHAR(50),
            n_clusters INT
        );
        CREATE INDEX IF NOT EXISTS idx_cluster ON {self.schema}.{self.tabela_destino}(cluster);
        CREATE INDEX IF NOT EXISTS idx_fk_contact ON {self.schema}.{self.tabela_destino}(fk_contact);
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_table_sql))
        print(f"Tabela {self.tabela_destino} criada no schema {self.schema}.")

    def upload_csv(self, caminho_csv="./desafio_clickbus/dados_desafio_fiap/hash/df_t.csv"):
        cur = self.con_iEvo.cursor()
        with open(caminho_csv, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {self.schema}.{self.tabela_origem} FROM STDIN WITH CSV HEADER DELIMITER ','",
                f
            )
        cur.close()
        self.con_iEvo.commit()
        print(f"Upload de {caminho_csv} concluído.")

    # ==============================================
    # Treinamento de Clusters
    # ==============================================
    def treinar_modelo(self, amostra_limite=200000):
        sql_clientes = f"""
        WITH base AS (
            SELECT fk_contact,
                   gmv_success::numeric AS gmv,
                   total_tickets_quantity_success::numeric AS tickets,
                   date_purchase::timestamp AS dt_compra,
                   CASE WHEN place_origin_return IS NULL OR place_origin_return='0' THEN 0 ELSE 1 END AS tem_retorno
            FROM {self.schema}.{self.tabela_origem}
        )
        SELECT fk_contact,
               AVG(gmv) AS gmv_medio,
               SUM(tickets) AS total_tickets,
               MAX(tem_retorno) AS tem_retorno,
               AVG(EXTRACT(HOUR FROM dt_compra)) AS hora_media_compra,
               MODE() WITHIN GROUP (ORDER BY EXTRACT(DOW FROM dt_compra)) AS dia_semana_moda
        FROM base
        GROUP BY fk_contact
        ORDER BY RANDOM()
        LIMIT {amostra_limite}
        """
        clientes = pd.read_sql(sql_clientes, self.engine)

        clientes["ticket_medio"] = clientes.apply(
            lambda row: row["gmv_medio"] / row["total_tickets"] if row["total_tickets"] > 0 else 0,
            axis=1
        )

        X = clientes[[
            "gmv_medio",
            "total_tickets",
            "ticket_medio",
            "tem_retorno",
            "hora_media_compra",
            "dia_semana_moda"
        ]]

        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        self.modelo = MiniBatchKMeans(
            n_clusters=self.n_clusters,
            random_state=42,
            batch_size=10000
        )
        self.modelo.fit(X_scaled)

        print("Modelo KMeans treinado com features de compra e temporais.")

    # ==============================================
    # Aplicar clusterização à base inteira
    # ==============================================
    def aplicar_clusterizacao(self):
        if self.scaler is None or self.modelo is None:
            raise ValueError("É necessário rodar treinar_modelo() antes de aplicar_clusterizacao().")

        sql_clientes = f"""
        WITH base AS (
            SELECT fk_contact,
                   gmv_success::numeric AS gmv,
                   total_tickets_quantity_success::numeric AS tickets,
                   date_purchase::timestamp AS dt_compra,
                   CASE WHEN place_origin_return IS NULL OR place_origin_return='0' THEN 0 ELSE 1 END AS tem_retorno
            FROM {self.schema}.{self.tabela_origem}
        )
        SELECT fk_contact,
               AVG(gmv) AS gmv_medio,
               SUM(tickets) AS total_tickets,
               MAX(tem_retorno) AS tem_retorno,
               AVG(EXTRACT(HOUR FROM dt_compra)) AS hora_media_compra,
               MODE() WITHIN GROUP (ORDER BY EXTRACT(DOW FROM dt_compra)) AS dia_semana_moda
        FROM base
        GROUP BY fk_contact
        """
        clientes = pd.read_sql(sql_clientes, self.engine)

        clientes["ticket_medio"] = clientes.apply(
            lambda row: row["gmv_medio"] / row["total_tickets"] if row["total_tickets"] > 0 else 0,
            axis=1
        )

        X = clientes[[
            "gmv_medio",
            "total_tickets",
            "ticket_medio",
            "tem_retorno",
            "hora_media_compra",
            "dia_semana_moda"
        ]]

        X_scaled = self.scaler.transform(X)
        clientes["cluster"] = self.modelo.predict(X_scaled)

        sql_viagens = f"SELECT * FROM {self.schema}.{self.tabela_origem}"
        for chunk in pd.read_sql(sql_viagens, self.engine, chunksize=self.chunksize):
            chunk["tem_retorno"] = chunk["place_origin_return"].fillna("0").astype(str).apply(
                lambda x: 0 if x.strip() == "0" else 1
            )
            chunk["hora_compra"] = pd.to_datetime(chunk["date_purchase"]).dt.hour
            chunk["dia_semana"] = pd.to_datetime(chunk["date_purchase"]).dt.dayofweek

            chunk = chunk.merge(clientes[["fk_contact", "cluster"]], on="fk_contact", how="left")
            chunk["ticket_medio"] = chunk["gmv_success"] / chunk["total_tickets_quantity_success"]
            chunk["data_processamento"] = datetime.now()
            chunk["algoritmo"] = "MiniBatchKMeans"
            chunk["n_clusters"] = self.n_clusters

            cols_destino = [
                "fk_contact", "cluster",
                "gmv_success", "total_tickets_quantity_success", "ticket_medio",
                "tem_retorno", "hora_compra", "dia_semana",
                "nmcidadeorigem", "nmuforigem",
                "nmcidadedestino", "nmufdestino",
                "nm_company", "date_purchase",
                "data_processamento", "algoritmo", "n_clusters"
            ]

            with self.engine.begin() as conn:
                chunk[cols_destino].to_sql(
                    self.tabela_destino,
                    conn,
                    schema=self.schema,
                    if_exists="append",
                    index=False
                )
            print(f"Chunk gravado com {len(chunk)} registros.")

        print("Clusterização propagada para todas as viagens.")

    # ==============================================
    # Análise de Engajamento por Cluster
    # ==============================================
    def gerar_analise_engajamento(self, tabela_perfis="viagens_clusterizadas_perfis_engajamento", top_n_val=5):
        if self.engine is None:
            raise ValueError("É necessário criar a conexão antes de gerar análise de engajamento.")

        sql = f"SELECT * FROM {self.schema}.{self.tabela_destino}"
        dfs = [chunk for chunk in pd.read_sql(sql, self.engine, chunksize=self.chunksize)]
        df = pd.concat(dfs, ignore_index=True)

        cliente_col = "fk_contact"
        df_clientes = df.groupby([cliente_col, "cluster"]).agg(
            gmv_total=("gmv_success", "sum"),
            qtd_passagens_total=("total_tickets_quantity_success", "sum"),
            pct_com_retorno=("tem_retorno", "mean")
        ).reset_index()

        df_clientes["engajamento_score"] = (
            df_clientes["qtd_passagens_total"].rank(pct=True) * 100
        ).round(0)

        def classifica(score):
            if score >= 90: return "Alta"
            elif score >= 40: return "Média"
            return "Baixa"

        df_clientes["engajamento_class"] = df_clientes["engajamento_score"].apply(classifica)

        resumo = df_clientes.groupby("cluster").agg(
            qtd_clientes=(cliente_col, "count"),
            gmv_medio_cliente=("gmv_total", "mean"),
            gmv_mediano_cliente=("gmv_total", "median"),
            qtd_passagens_media_cliente=("qtd_passagens_total", "mean"),
            pct_com_retorno_cliente=("pct_com_retorno", "mean"),
            engajamento_medio=("engajamento_score", "mean")
        ).reset_index()

        def top_n(series, n=5): return series.value_counts().head(n).to_dict()

        top_origem = df.groupby("cluster")["nmcidadeorigem"].apply(lambda x: top_n(x, top_n_val)).reset_index()
        top_destino = df.groupby("cluster")["nmcidadedestino"].apply(lambda x: top_n(x, top_n_val)).reset_index()
        top_bus = df.groupby("cluster")["nm_company"].apply(lambda x: top_n(x, top_n_val)).reset_index()

        engajamento_counts = df_clientes.groupby(["cluster", "engajamento_class"]).size().unstack(fill_value=0).reset_index()
        for col in ["Alta", "Média", "Baixa"]:
            if col not in engajamento_counts.columns:
                engajamento_counts[col] = 0
        total = engajamento_counts[["Alta", "Média", "Baixa"]].sum(axis=1)
        engajamento_counts["pct_alta"] = engajamento_counts["Alta"] / total
        engajamento_counts["pct_media"] = engajamento_counts["Média"] / total
        engajamento_counts["pct_baixa"] = engajamento_counts["Baixa"] / total

        cluster_perfis = resumo.merge(top_origem, on="cluster") \
                               .merge(top_destino, on="cluster") \
                               .merge(top_bus, on="cluster") \
                               .merge(engajamento_counts[["cluster", "pct_alta", "pct_media", "pct_baixa"]], on="cluster")

        cluster_perfis["nmcidadeorigem"] = cluster_perfis["nmcidadeorigem"].apply(json.dumps)
        cluster_perfis["nmcidadedestino"] = cluster_perfis["nmcidadedestino"].apply(json.dumps)
        cluster_perfis["nm_company"] = cluster_perfis["nm_company"].apply(json.dumps)

        def gerar_perfil(row):
            if row["gmv_medio_cliente"] > resumo["gmv_medio_cliente"].quantile(0.75): gasto = "alto valor"
            elif row["gmv_medio_cliente"] < resumo["gmv_medio_cliente"].quantile(0.25): gasto = "baixo valor"
            else: gasto = "valor intermediário"
            if row["qtd_passagens_media_cliente"] >= 4: passagens = "muitas passagens"
            elif row["qtd_passagens_media_cliente"] <= 1.5: passagens = "poucas passagens"
            else: passagens = "quantidade moderada de passagens"
            retorno = "ida e volta" if row["pct_com_retorno_cliente"] > 0.6 else "só ida"
            if row["pct_alta"] > 0.3: engaj = "com forte presença de heavy users"
            elif row["pct_media"] > 0.5: engaj = "com perfil de compra mediano"
            else: engaj = "com maioria de clientes de baixo engajamento"
            return f"Clientes de {gasto}, {passagens}, geralmente {retorno}, {engaj}."

        cluster_perfis["perfil_textual"] = cluster_perfis.apply(gerar_perfil, axis=1)

        cluster_perfis = cluster_perfis.rename(columns={
            "nmcidadeorigem": "top_origens",
            "nmcidadedestino": "top_destinos",
            "nm_company": "top_viacoes"
        })

        ddl = f"""
        DROP TABLE IF EXISTS {self.schema}.{tabela_perfis};
        CREATE TABLE {self.schema}.{tabela_perfis} (
            cluster INT PRIMARY KEY,
            qtd_clientes INT,
            gmv_medio_cliente NUMERIC,
            gmv_mediano_cliente NUMERIC,
            qtd_passagens_media_cliente NUMERIC,
            pct_com_retorno_cliente NUMERIC,
            engajamento_medio NUMERIC,
            top_origens JSON,
            top_destinos JSON,
            top_viacoes JSON,
            pct_alta NUMERIC,
            pct_media NUMERIC,
            pct_baixa NUMERIC,
            perfil_textual VARCHAR(500)
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
            cluster_perfis.to_sql(tabela_perfis, conn, schema=self.schema, if_exists="replace", index=False)

        print(f"Perfis de engajamento salvos em {self.schema}.{tabela_perfis}")
        return cluster_perfis

    # ==============================================
    # Análise de Horários de Pico + Recomendações
    # ==============================================

    def analisar_horarios_pico(
        self,
        tabela_picos: str = "viagens_picos_horarios",
        tabela_recos: str = "recomendacoes_viacao_menor",
        top_n_horas: int = 5,
        top_n_rotas: int = 5,
        pct_heavy: float = 0.20,            # top 20% por passagens = heavy users
        share_small_company: float = 0.10   # <=10% de market share na rota => "menor"
    ):
        """
        Gera (1) ranking de horários de pico por cluster, ponderando heavy users,
        e (2) recomendações de viações menores por rota e hora de pico.
        """

        if self.engine is None:
            raise ValueError("Crie a conexão antes de rodar analisar_horarios_pico().")

        # =========================================
        # 1) Ler base clusterizada (em chunks)
        # =========================================
        sql = f"SELECT * FROM {self.schema}.{self.tabela_destino}"
        dfs = [chunk for chunk in pd.read_sql(sql, self.engine, chunksize=self.chunksize)]
        if not dfs:
            raise ValueError("Tabela de destino está vazia. Rode aplicar_clusterizacao() antes.")
        df = pd.concat(dfs, ignore_index=True)

        # Valida colunas essenciais
        required_cols = {
            "fk_contact", "cluster", "date_purchase",
            "gmv_success", "total_tickets_quantity_success",
            "nmcidadeorigem", "nmcidadedestino", "nm_company"
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Faltam colunas na base clusterizada: {missing}")

        # =========================================
        # 2) Deriva atributos temporais e métricas
        # =========================================
        df["date_purchase"] = pd.to_datetime(df["date_purchase"])
        df["hora"] = df["date_purchase"].dt.hour
        df["dia_semana"] = df["date_purchase"].dt.dayofweek
        df["tickets"] = pd.to_numeric(df["total_tickets_quantity_success"], errors="coerce").fillna(0)
        df["gmv"] = pd.to_numeric(df["gmv_success"], errors="coerce").fillna(0.0)

        # =========================================
        # 3) Heavy users: percentil por passagens (por cluster)
        #    - Calcula total de passagens por cliente dentro do cluster
        #    - Marca heavy se rank pct >= (1 - pct_heavy)
        # =========================================
        agg_cli = (
            df.groupby(["cluster", "fk_contact"], as_index=False)
            .agg(total_tickets_cli=("tickets", "sum"))
        )
        # rank percentual dentro de cada cluster
        agg_cli["rank_pct"] = agg_cli.groupby("cluster")["total_tickets_cli"].rank(pct=True)
        cutoff = 1.0 - pct_heavy
        agg_cli["is_heavy"] = (agg_cli["rank_pct"] >= cutoff).astype(int)

        # Merge flag heavy para o nível de viagem
        df = df.merge(
            agg_cli[["cluster", "fk_contact", "is_heavy"]],
            on=["cluster", "fk_contact"],
            how="left"
        )
        df["is_heavy"] = df["is_heavy"].fillna(0).astype(int)

        # =========================================
        # 4) Picos por cluster e hora
        #    Score simples: tickets * (1 + 0.5 * heavy_ratio)
        # =========================================
        grp_ch = (
            df.groupby(["cluster", "hora"], as_index=False)
            .agg(
                compras=("fk_contact", "count"),
                tickets=("tickets", "sum"),
                gmv=("gmv", "sum"),
                heavy_tickets=("is_heavy", "sum")  # cada linha representa uma compra; aproximação
            )
        )
        # heavy_ratio baseado em "compras" (linhas) heavy; alternativa: ponderar por tickets se necessário
        grp_ch["heavy_ratio"] = np.where(grp_ch["compras"] > 0, grp_ch["heavy_tickets"] / grp_ch["compras"], 0.0)
        grp_ch["score"] = grp_ch["tickets"] * (1.0 + 0.5 * grp_ch["heavy_ratio"])

        # Top N horas por cluster
        grp_ch["rank_hora"] = grp_ch.groupby("cluster")["score"].rank(method="first", ascending=False)
        picos_top = grp_ch.loc[grp_ch["rank_hora"] <= top_n_horas].copy()

        # =========================================
        # 5) Market share por rota e viação (para achar viações "menores")
        # =========================================
        rota_bus = (
            df.groupby(["nmcidadeorigem", "nmcidadedestino", "nm_company"], as_index=False)
            .agg(tickets_bus=("tickets", "sum"))
        )
        rota_tot = (
            rota_bus.groupby(["nmcidadeorigem", "nmcidadedestino"], as_index=False)
                    .agg(tickets_rota=("tickets_bus", "sum"))
        )
        rota_bus = rota_bus.merge(rota_tot, on=["nmcidadeorigem", "nmcidadedestino"], how="left")
        rota_bus["share"] = np.where(rota_bus["tickets_rota"] > 0,
                                    rota_bus["tickets_bus"] / rota_bus["tickets_rota"], 0.0)

        # Marca viação "menor" por share na rota
        rota_bus["is_small_company"] = (rota_bus["share"] <= share_small_company).astype(int)

        # =========================================
        # 6) Para cada CLUSTER x HORA de pico, pegar TOP rotas e sugerir as viações menores
        # =========================================
        # Demanda por (cluster, hora, rota)
        dem_ch_rota = (
            df.groupby(["cluster", "hora", "nmcidadeorigem", "nmcidadedestino"], as_index=False)
            .agg(
                tickets_rota=("tickets", "sum"),
                compras=("fk_contact", "count"),
                heavy_compras=("is_heavy", "sum"),
                heavy_tickets=("is_heavy", "sum")  # aprox
            )
        )
        dem_ch_rota["heavy_ratio"] = np.where(dem_ch_rota["compras"] > 0,
                                            dem_ch_rota["heavy_compras"] / dem_ch_rota["compras"], 0.0)
        # mantém só horas de pico
        dem_top = dem_ch_rota.merge(
            picos_top[["cluster", "hora"]],
            on=["cluster", "hora"],
            how="inner"
        )

        # Top rotas por demanda dentro das horas de pico
        dem_top["rank_rota"] = dem_top.groupby(["cluster", "hora"])["tickets_rota"].rank(method="first", ascending=False)
        dem_top = dem_top.loc[dem_top["rank_rota"] <= top_n_rotas].copy()

        # Junta market share & small companies por rota/companhia
        recos = dem_top.merge(
            rota_bus,
            on=["nmcidadeorigem", "nmcidadedestino"],
            how="left",
            suffixes=("", "_rota")
        )

        # Fica só as viações menores (candidatas)
        recos = recos.loc[recos["is_small_company"] == 1].copy()

        # Score de oportunidade (simples e transparente):
        #   oportunidade = tickets_rota * (1 - share) * (0.5 + 0.5 * heavy_ratio)
        recos["opportunity_score"] = recos["tickets_rota"] * (1 - recos["share"]) * (0.5 + 0.5 * recos["heavy_ratio"])

        # Monta texto de recomendação
        def mk_reco(row):
            return (
                f"Sugerir {row['nm_company']} na rota {row['nmcidadeorigem']} → {row['nmcidadedestino']} "
                f"no horário {int(row['hora']):02d}h. Share atual: {row['share']:.1%}. "
                f"Demanda (pico) no cluster: {int(row['tickets_rota'])} tickets; "
                f"heavy users: {row['heavy_ratio']:.0%}."
            )
        recos["recomendacao"] = recos.apply(mk_reco, axis=1)

        # =========================================
        # 7) DDLs + Persistência
        # =========================================
        ddl_picos = f"""
        DROP TABLE IF EXISTS {self.schema}.{tabela_picos};
        CREATE TABLE {self.schema}.{tabela_picos} (
            cluster INT,
            hora INT,
            compras BIGINT,
            tickets BIGINT,
            gmv NUMERIC,
            heavy_tickets BIGINT,
            heavy_ratio NUMERIC,
            score NUMERIC,
            rank_hora INT,
            data_processamento TIMESTAMP
        );
        """
        ddl_recos = f"""
        DROP TABLE IF EXISTS {self.schema}.{tabela_recos};
        CREATE TABLE {self.schema}.{tabela_recos} (
            cluster INT,
            hora INT,
            nmcidadeorigem VARCHAR(200),
            nmcidadedestino VARCHAR(200),
            nm_company VARCHAR(200),
            tickets_peak BIGINT,
            heavy_ratio NUMERIC,
            company_share NUMERIC,
            opportunity_score NUMERIC,
            recomendacao VARCHAR(800),
            data_processamento TIMESTAMP
        );
        """

        now = datetime.now()
        picos_out = picos_top.copy()
        picos_out["data_processamento"] = now

        recos_out = recos[[
            "cluster", "hora", "nmcidadeorigem", "nmcidadedestino", "nm_company",
            "tickets_rota", "heavy_ratio", "share", "opportunity_score", "recomendacao"
        ]].copy()
        recos_out = recos_out.rename(columns={
            "tickets_rota": "tickets_peak",
            "share": "company_share"
        })
        recos_out["data_processamento"] = now

        with self.engine.begin() as conn:
            conn.execute(text(ddl_picos))
            conn.execute(text(ddl_recos))

        with self.engine.begin() as conn:
            picos_out.to_sql(tabela_picos, conn, schema=self.schema, if_exists="append", index=False)
            recos_out.to_sql(tabela_recos, conn, schema=self.schema, if_exists="append", index=False)

        print(f"[OK] Horários de pico salvos em {self.schema}.{tabela_picos} e recomendações em {self.schema}.{tabela_recos}.")
        return picos_out, recos_out


ml = ML_Clickbus(
    db_uri="postgresql+psycopg2://postgres:M4st3rPasw0rd@ievo4action.chxlx3odlwt6.us-east-1.rds.amazonaws.com:5432/dw",
    tabela_origem="vw_clickbus",
    tabela_destino="viagens_clusterizadas_v2",
    n_clusters=8,
    chunksize=100000,
    schema="estudo"
)

# 1. Cria conexão com o banco
ml.criar_conexao()

# 2. (Opcional) Upload CSV caso ainda não tenha a base carregada
# ml.upload_csv("./desafio_clickbus/dados_desafio_fiap/hash/df_t.csv")

# # 3. Cria tabela de destino
# ml.criar_tabela_destino()

# # 4. Treina modelo
# ml.treinar_modelo(amostra_limite=200000)

# # 5. Aplica clusterização
# ml.aplicar_clusterizacao()

# # 6. Analisa perfis de engajamento
# df_perfis = ml.gerar_analise_engajamento()
# print(df_perfis.head())

# 7. Analisa horários de pico + recomendações
picos, recos = ml.analisar_horarios_pico()
print(picos.head())
print(recos.head())
