import psycopg2
import pandas as pd
import numpy as np

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

links = data['link'].tolist()

# Limpeza dos links
links_limpos = [link.strip() for link in links]
for link in links_limpos:
    print(link)
    print(data.loc[data['link'] == link, 'valor'])

# Remova os pontos da coluna "valor"
data['valor'] = data['valor'].str.replace('.', '').str.replace(',', '.').astype(float)

# Valor médio dos produtos por site
valor_medio_por_site = data.groupby('site')['valor'].mean()

# Filtra os dados para os links dos componentes
link_processador = "https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM#position=11&search_layout=grid&type=item&tracking_id=a1976802-4bbe-4d4d-a00b-dfbda8b60ce9"
link_gabinete = "https://www.mercadolivre.com.br/gabinete-gamer-lian-li-redragon-modelo-o11dynamic-mini-branc/p/MLB23190291?pdp_filters=category:MLB1696#searchVariation=MLB23190291&position=2&search_layout=grid&type=product&tracking_id=dab59008-df8b-46f0-bd34-2f8053eca38f"
link_placa_de_video = "https://www.mercadolivre.com.br/placa-de-video-nvidia-galax-geforce-rtx-30-series-rtx-3060-36nsl8md6occ-oc-edition-8gb/p/MLB20736337?pdp_filters=category:MLB1658#searchVariation=MLB20736337&position=3&search_layout=grid&type=product&tracking_id=d3ba3a55-4cda-4a8e-8f88-fd6382009246"
link_placa_mae = "https://produto.mercadolivre.com.br/MLB-1676543787-placa-me-asus-tuf-b460m-plus-b460-lga1200-ddr4-10a-ger-_JM#position=25&search_layout=grid&type=item&tracking_id=6f6be4a6-644a-43c2-8e5b-285259d18b1e"
link_memoria_ram = "https://www.mercadolivre.com.br/memoria-ram-fury-color-preto-16gb-1-hyperx-hx426c16fb16/p/MLB14728888?pdp_filters=category:MLB1694#searchVariation=MLB14728888&position=8&search_layout=grid&type=product&tracking_id=b0b0bebf-c99d-42c0-b721-f62d1a64d3a1"
link_water_cooler = "https://produto.mercadolivre.com.br/MLB-3381940936-water-cooler-corsair-h100-rgb-240mm-radiator-preto-_JM#position=6&search_layout=grid&type=item&tracking_id=137704c3-2bb1-4abe-8809-bd43e8c8f05d"

# Dicionário para armazenar os valores de cada componente
valores_componentes = {}

# Filtra os valores dos outros componentes
for componente, link in {
    "Processador": link_processador,
    "Gabinete": link_gabinete,
    "Placa de Vídeo": link_placa_de_video,
    "Placa Mãe": link_placa_mae,
    "Memória RAM": link_memoria_ram,
    "Water Cooler": link_water_cooler
}.items():
    if link in links_limpos:
        valores = data.loc[data['link'] == link, 'valor'].tolist()
        valores_componentes[componente] = valores
    else:
        print(f"Não há valores para o {componente}")
        print("Componente:", link)
        print()

# Imprime as informações no terminal
print("Dashboard")
print("---------")

for componente, valores in valores_componentes.items():
    print(f"Valores do {componente}:")
    print(valores)
    print()

    if len(valores) > 0:
        max_valor = np.max(valores)
        min_valor = np.min(valores)
        media_valor = np.mean(valores)

        print("Máximo:", max_valor)
        print("Mínimo:", min_valor)
        print("Média:", media_valor)
        print()
    else:
        print(f"Não há valores para o {componente}")
        print()

# Fecha a conexão com o banco de dados
conn.close()
