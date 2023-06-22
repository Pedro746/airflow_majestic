import pandas as pd




# Configurações do arquivo
file_path = 'data/cliente.csv'

# Nome das colunas
#columns_names = ['id_venda', 'id_nota', 'data', 'frente', 'desconto', 'vendedor', 'cliente', 'produto', 'quantidade', 'filial', 'forma_de_pagamento', 'data_de_faturamento']

# Lendo o arquivo
df = pd.read_csv(file_path)

df = df.rename(columns = {
    'idCliente' : 'id_venda',
    'Nome' : 'nome',
    'Tipo' : 'tipo',
    'Media de Alunos' : 'media_de_alunos',
    'Classificacao' : 'classificacao'
})

print(df)