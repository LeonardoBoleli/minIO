import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# Conecta ao banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="produtos",
    user="admin",
    password="admin"
)

# Executa a consulta SQL para obter os dados da tabela produtos
query = "SELECT * FROM produtos"
data = pd.read_sql(query, conn)

# Manipula e analisa os dados conforme necessário
# Aqui estão alguns exemplos de manipulação dos dados:
# - Contagem de produtos por site
produtos_por_site = data['site'].value_counts()

# - Valor médio dos produtos por site
valor_medio_por_site = data.groupby('site')['valor'].mean()

# - Gráfico de barras dos produtos por site
plt.figure(figsize=(8, 6))
produtos_por_site.plot(kind='bar')
plt.title('Produtos por Site')
plt.xlabel('Site')
plt.ylabel('Quantidade')
plt.xticks(rotation=45)
plt.tight_layout()

# Imprime as informações e gráficos no terminal
print("Dashboard")
print("---------")
print("Produtos por Site:")
print(produtos_por_site)
print()
print("Valor Médio dos Produtos por Site:")
print(valor_medio_por_site)
print()

# Exibe o gráfico de barras
plt.show()

# Fecha a conexão com o banco de dados
conn.close()
