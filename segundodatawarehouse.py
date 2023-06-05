import psycopg2
import pandas as pd

def get_product_stats(link, id):
    with conn.cursor() as cursor:
        query = """
            SELECT MIN(valor::FLOAT), AVG(valor::FLOAT), MAX(valor::FLOAT)
            FROM produtos
            WHERE link = %s AND id < %s
        """
        cursor.execute(query, (link, id))
        result = cursor.fetchone()
        if result:
            min_valor, avg_valor, max_valor = result
            return min_valor, avg_valor, max_valor
        else:
            return 0, 0, 0


# Conecta ao banco de dados
conn = psycopg2.connect(
    host="localhost", database="produtos", user="admin", password="admin"
)

# Cria a tabela do Data segundo_warehouse caso não exista
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS segundo_warehouse (
        id SERIAL PRIMARY KEY,
        produto VARCHAR(100),
        valor_atual FLOAT,
        data_hora VARCHAR,
        min_valor FLOAT,
        avg_valor FLOAT,
        max_valor FLOAT
    );
    """
)
conn.commit()

# Obtém os novos dados do Datalake
new_data_query = "SELECT * FROM produtos"
data = pd.read_sql(new_data_query, conn)

# Cria o objeto cursor
cursor = conn.cursor()

# Itera sobre os dados do dataframe
for row in data.itertuples(index=False):
    link = row.link
    if (
        "https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM"
        in link
    ):
        produto = "Processador"
    elif (
        "https://www.mercadolivre.com.br/gabinete-gamer-lian-li-redragon-modelo-o11dynamic-mini-branc/p/MLB23190291?pdp_filters=category:MLB1696"
        in link
    ):
        produto = "Gabinete"
    elif (
        "https://www.mercadolivre.com.br/placa-de-video-nvidia-galax-geforce-rtx-30-series-rtx-3060-36nsl8md6occ-oc-edition-8gb/p/MLB20736337?pdp_filters=category:MLB1658"
        in link
    ):
        produto = "Placa de Vídeo"
    elif (
        "https://produto.mercadolivre.com.br/MLB-1676543787-placa-me-asus-tuf-b460m-plus-b460-lga1200-ddr4-10a-ger-_JM"
        in link
    ):
        produto = "Placa Mãe"
    elif (
        "https://www.mercadolivre.com.br/memoria-ram-fury-color-preto-16gb-1-hyperx-hx426c16fb16/p/MLB14728888?pdp_filters=category:MLB1694"
        in link
    ):
        produto = "Memória RAM"
    elif (
        "https://produto.mercadolivre.com.br/MLB-3381940936-water-cooler-corsair-h100-rgb-240mm-radiator-preto-_JM"
        in link
    ):
        produto = "Water Cooler"
    else:
        produto = "Outro Produto"

    valor_produto = float(row.valor)
    hora, minuto, segundo = row.hora.split(":")
    ano, mes, dia = row.data.split("-")
    data_hora = f"{hora}:{minuto}:{segundo} - {dia}/{mes}/{ano}"

    # Obtém os valores estatísticos do produto até o momento
    min_valor, avg_valor, max_valor = get_product_stats(link, row.id)
    
    # Verifica se algum dos valores é igual a 0
    if min_valor != 0:
        min_valor = round(min_valor, 2)
    if avg_valor != 0:
        avg_valor = round(avg_valor, 2)
    if max_valor != 0:
        max_valor = round(max_valor, 2)

    # Insere os dados para o novo produto
    insert_query = """
        INSERT INTO segundo_warehouse (produto, valor_atual, data_hora, min_valor, avg_valor, max_valor)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(
        insert_query,
        (
            produto,
            valor_produto,
            data_hora,
            min_valor,
            avg_valor,
            max_valor,
        ),
    )

    # Atualiza os valores de min_valor, avg_valor e max_valor na tabela para a linha atual
    cursor.execute(
        "UPDATE segundo_warehouse SET min_valor = %s, avg_valor = %s, max_valor = %s WHERE produto = %s",
        (min_valor, avg_valor, max_valor, produto),
    )

conn.commit()
print("Inserção concluída com sucesso!")

# Fecha o cursor
cursor.close()

# Fecha a conexão com o banco de dados
conn.close()
