import psycopg2
import pandas as pd


def get_product_stats(link):
    with conn.cursor() as cursor:
        query = """
            SELECT MIN(valor::FLOAT), AVG(valor::FLOAT), MAX(valor::FLOAT)
            FROM produtos
            WHERE link = %s
        """
        cursor.execute(query, (link,))
        result = cursor.fetchone()
        min_valor, avg_valor, max_valor = result
        return min_valor, avg_valor, max_valor


# Conecta ao banco de dados
conn = psycopg2.connect(
    host="localhost", database="produtos", user="admin", password="admin"
)

# Cria a tabela do Data Warehouse caso não exista
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS warehouse (
        id SERIAL PRIMARY KEY,
        produto VARCHAR(100),
        ultimo_valor FLOAT,
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

# Dicionário para armazenar o valor e a data/hora da última linha de cada produto
ultimas_linhas = {}

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

    valor_produto = row.valor
    hora, minuto, segundo = row.hora.split(":")
    ano, mes, dia = row.data.split("-")
    data_hora = f"{hora}:{minuto}:{segundo} - {dia}/{mes}/{ano}"

    # Atualiza o valor e a data/hora da última linha para o produto atual
    ultimas_linhas[produto] = (valor_produto, data_hora)

    # Obtém os valores estatísticos do produto até o momento
    min_valor, avg_valor, max_valor = get_product_stats(link)
    min_valor, avg_valor, max_valor = (
        round(min_valor, 2),
        round(avg_valor, 2),
        round(max_valor, 2),
    )

    # Verifica se já existe uma entrada para o produto na tabela warehouse
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM warehouse WHERE produto = %s", (produto,))
        existing_entry = cursor.fetchone()

        if existing_entry:
            # Atualiza os valores para o produto existente
            update_query = """
                UPDATE warehouse
                SET ultimo_valor = %s, data_hora = %s, min_valor = %s, avg_valor = %s, max_valor = %s
                WHERE id = %s
            """

            cursor.execute(
                update_query,
                (
                    valor_produto,
                    data_hora,
                    min_valor,
                    avg_valor,
                    max_valor,
                    existing_entry[0],
                ),
            )
        else:
            # Insere os dados para o novo produto
            insert_query = """
                INSERT INTO warehouse (produto, ultimo_valor, data_hora, min_valor, avg_valor, max_valor)
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

# Atualiza os valores de valor_produto e data_hora com base no dicionário das últimas linhas
for produto, (valor, data_hora) in ultimas_linhas.items():
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE warehouse SET ultimo_valor = %s, data_hora = %s WHERE produto = %s",
            (valor, data_hora, produto),
        )

conn.commit()
print("Inserção concluída com sucesso!")

# Fecha a conexão com o banco de dados
conn.close()
