import pandas as pd
import utils.conn as con2 
from utils.database import connectdatabase 
from utils.functions import config_log

log = config_log()

con_iEvo = connectdatabase('db/iEvo')
cur = con_iEvo.cursor()

with open("./desafio_clickbus/dados_desafio_fiap/hash/df_t.csv", "r", encoding="utf-8") as f:
    cur.copy_expert(
        "COPY estudo.dl_clickbus FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

cur.close()
con_iEvo.commit()
con_iEvo.close()
