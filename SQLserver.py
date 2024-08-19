import pandas as pd
import pyodbc

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

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=SEU_SERVIDOR;"
    r"Database=NOME_DO_BANCO_DE_DADOS;"
    r"UID=SEU_USUARIO;"
    r"PWD=SUA_SENHA;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

for index, row in df_vendas_completas.iterrows():
    cursor.execute("""
        INSERT INTO tb_vendas_completas (SKU, Dia, Loja, Cliente, Quantidade, Forma_Pagamento, Marca, Custo_Compra, Valor_Venda, Cor, Tipo, Nome_Sobrenome, Codigo_Cliente, Email, Genero, Numero_Filhos, Data_Nascimento)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row['SKU'], row['Dia'], row['Loja'], row['Cliente'], row['Quantidade'], row['Forma de Pagamento'], 
        row['Marca'], row['Custo de compra'], row['Valor de Venda'], row['Cor'], row['Tipo'],
        row['Nome - Sobrenome'], row['Codigo do Cliente'], row['E-mail'], row['Genero'], 
        row['Numero de Filhos'], row['Data de Nascimento'])

conn.commit()
cursor.close()
conn.close()

print("Dados inseridos com sucesso no SQL Server!")
