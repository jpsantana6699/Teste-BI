import pandas as pd

df_produto = pd.read_csv('tb_produto.csv')
df_cliente = pd.read_csv('tb_cliente.csv')
df_venda = pd.read_csv('tb_venda.csv')

df_produto.drop_duplicates(inplace=True)
df_cliente.drop_duplicates(inplace=True)
df_venda.drop_duplicates(inplace=True)

df_produto.fillna({'Cor': 'Desconhecida', 'Tipo': 'Desconhecido'}, inplace=True)
df_cliente.fillna({'Numero de Filhos': 0, 'Genero': 'Não informado'}, inplace=True)
df_venda.fillna({'Forma de Pagamento': 'Desconhecida'}, inplace=True)


df_vendas_completas = df_venda.merge(df_produto, on='SKU').merge(df_cliente, left_on='Cliente', right_on='Codigo do Cliente')


df_vendas_completas.to_csv('vendas_completas.csv', index=False)

print("ETL concluído e dados salvos em vendas_completas.csv")
