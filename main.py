import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from db import fetch_query


dados, colunas = fetch_query(
    """
        SELECT 
            ano, 
            SUM(valor_empenho)::money AS total_empenho, 
            SUM(valor_pagamento)::money AS valor_pagamento
          FROM 
                (
        SELECT 
            id_orgao, 
            cod_ne, 
            dt.ano, 
            valor_empenho, 
            SUM(valor_pago) AS valor_pagamento
        FROM 
            data_warehouse.fato_execucao_financeira fef
        INNER JOIN 
            data_warehouse.dim_tempo dt 
            ON dt.id = fef.id_data_empenho
        LEFT JOIN 
            data_warehouse.dim_tempo dtp 
            ON dtp.id = fef.id_data_pagamento
        GROUP BY 
            id_orgao, cod_ne, dt.ano, valor_empenho
        ) 
        tb GROUP BY 
            ano;

    """
)

df = pd.DataFrame(dados, columns=colunas)

print(df)
