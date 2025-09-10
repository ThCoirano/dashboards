-- 1️⃣ Estatísticas básicas por cluster
WITH resumo AS (
    SELECT
        cluster,
        COUNT(nk_ota_localizer_id) AS qtd_compras,
        AVG(gmv_success) AS ticket_medio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gmv_success) AS ticket_mediano,
        AVG(total_tickets_quantity_success) AS qtd_passagens_media,
        AVG(tem_retorno) AS pct_com_retorno
    FROM estudo.viagens_clusterizadas
    GROUP BY cluster
),
-- 2️⃣ NPS por cliente
clientes_cluster AS (
    SELECT
        fk_contact,
        cluster,
        COUNT(*) AS qtd_viagens
    FROM estudo.viagens_clusterizadas
    GROUP BY fk_contact, cluster
),
nps_cliente AS (
    SELECT
        *,
        ROUND( (qtd_viagens::numeric / MAX(qtd_viagens) OVER ()) * 10 ) AS nps_score,
        CASE 
            WHEN ROUND( (qtd_viagens::numeric / MAX(qtd_viagens) OVER ()) * 10 ) >= 9 THEN 'Promotor'
            WHEN ROUND( (qtd_viagens::numeric / MAX(qtd_viagens) OVER ()) * 10 ) >= 7 THEN 'Neutro'
            ELSE 'Detrator'
        END AS nps_class
    FROM clientes_cluster
),
-- 3️⃣ Agrega o NPS por cluster usando cada cliente uma vez
nps_cluster AS (
    SELECT
        cluster,
        SUM(CASE WHEN nps_class = 'Promotor' THEN 1 ELSE 0 END)::numeric / COUNT(*) AS pct_promotor,
        SUM(CASE WHEN nps_class = 'Neutro' THEN 1 ELSE 0 END)::numeric / COUNT(*) AS pct_neutro,
        SUM(CASE WHEN nps_class = 'Detrator' THEN 1 ELSE 0 END)::numeric / COUNT(*) AS pct_detrator,
        SUM(CASE WHEN nps_class = 'Promotor' THEN 1 ELSE 0 END)::numeric / COUNT(*) -
        SUM(CASE WHEN nps_class = 'Detrator' THEN 1 ELSE 0 END)::numeric / COUNT(*) AS nps
    FROM nps_cliente
    GROUP BY cluster
),
-- 4️⃣ Top 3 origens por cluster
top_origem AS (
    SELECT cluster,
          -- ARRAY_AGG(place_origin_departure ORDER BY count_orig DESC) AS top_origens
    		place_origin_departure as top_origens
    FROM (
        SELECT cluster, place_origin_departure, COUNT(*) AS count_orig,
               ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY COUNT(*) DESC) AS rn
        FROM estudo.viagens_clusterizadas
        GROUP BY cluster, place_origin_departure
    ) t
    WHERE rn <= 10
    GROUP BY cluster, place_origin_departure
),
-- 5️⃣ Top 3 destinos por cluster
top_destino AS (
    SELECT cluster,
           --ARRAY_AGG(place_destination_departure ORDER BY count_dest DESC) AS top_destinos
    		place_destination_departure as top_destinos
    FROM (
        SELECT cluster, place_destination_departure, COUNT(*) AS count_dest,
               ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY COUNT(*) DESC) AS rn
        FROM estudo.viagens_clusterizadas
        GROUP BY cluster, place_destination_departure
    ) t
    WHERE rn <= 10
    GROUP BY cluster, place_destination_departure
),
-- 6️⃣ Top 10 viações por cluster
top_viacao AS (
    SELECT cluster,
           --ARRAY_AGG(fk_departure_ota_bus_company ORDER BY count_bus DESC) AS top_viacoes
    		fk_departure_ota_bus_company as top_viacoes
    FROM (
        SELECT cluster, fk_departure_ota_bus_company, COUNT(*) AS count_bus,
               ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY COUNT(*) DESC) AS rn
        FROM estudo.viagens_clusterizadas
        GROUP BY cluster, fk_departure_ota_bus_company
    ) t
    WHERE rn <= 10
    GROUP BY cluster,fk_departure_ota_bus_company
)
-- 7️⃣ Junta tudo
SELECT r.*,
       n.pct_promotor,
       n.pct_neutro,
       n.pct_detrator,
       n.nps,
       o.top_origens,
       d.top_destinos,
       v.top_viacoes
FROM resumo r
LEFT JOIN nps_cluster n ON r.cluster = n.cluster
LEFT JOIN top_origem o ON r.cluster = o.cluster
LEFT JOIN top_destino d ON r.cluster = d.cluster
LEFT JOIN top_viacao v ON r.cluster = v.cluster
ORDER BY r.cluster;


select
	distinct rotulo,
	"cluster",
	qtd_clientes,
	gmv_medio_cliente,
	gmv_mediano_cliente,
	qtd_passagens_media_cliente,
	pct_com_retorno_cliente,
	--top_origens,
	--top_destinos,
	top_viacoes
from
	estudo.viagens_clusterizadas_perfis_nps vcpn
 
