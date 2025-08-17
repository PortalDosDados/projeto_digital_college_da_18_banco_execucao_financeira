import pandas as pd
from db import fetch_query, execute_query

# Executa query
dados, colunas = fetch_query("SELECT * FROM data_warehouse.dim_item_elemento;")

# Cria DataFrame
df = pd.DataFrame(dados, columns=colunas)

# Visualiza os primeiros registros
print(df.head())
