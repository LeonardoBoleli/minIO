import psycopg2
import pandas as pd


def get_product_stats(link):
    with conn.cursor() as cursor:
        query = """
            SELECT MIN(CAST(REPLACE(valor, '.', '') AS FLOAT)), AVG(CAST(REPLACE(valor, '.', '') AS FLOAT)), MAX(CAST(REPLACE(valor, '.', '') AS FLOAT))
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
        valor FLOAT,
        link VARCHAR,
        data_hora VARCHAR,
        data VARCHAR,
        hora VARCHAR,
        min_valor FLOAT,
        avg_valor FLOAT,
        max_valor FLOAT
    );
    """
)
conn.commit()

# Obtém os novos dados do Datalake
new_data_query = "SELECT * FROM produtos"
new_data = pd.read_sql(new_data_query, conn)

# Obtém os dados existentes na tabela do Data Warehouse
query = "SELECT * FROM warehouse"
existing_data = pd.read_sql(query, conn)
print("aqui deu bom")

# Concatena os dados existentes com os novos dados
data = pd.concat([existing_data, new_data], ignore_index=True)
print("aqui deu bom 2")

# Converte as colunas de data e hora para string
data["data"] = data["data"].astype(str)
data["hora"] = data["hora"].astype(str)
print("aqui deu bom 3")

# Imprime o tipo de dados de uma coluna específica
print(data["data"], ": ", data["data"].dtypes)
print(data["hora"], ": ", data["hora"].dtypes)


# Insere os dados enriquecidos na tabela do Data Warehouse
with conn.cursor() as cursor:
    for row in data.itertuples(index=False):
        print("row: ", row)
        for column, value in row._asdict().items():
            print(f"{column}: {value}")

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

        data_hora = f"{row.data} {row.hora}"

        # Obtém os valores estatísticos do produto até o momento
        min_valor, avg_valor, max_valor = get_product_stats(link)

        print("----------------------------------")
        print("link: ", link)
        print("produto: ", produto)
        print("data_hora: ", data_hora)
        print("min_valor: ", min_valor)
        print("avg_valor: ", avg_valor)
        print("max_valor: ", max_valor)

        # Resto do código para inserção na tabela
        insert_query = """
            INSERT INTO warehouse (produto, valor, link, data_hora, data, hora, min_valor, avg_valor, max_valor)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (
                produto,
                row.valor,
                link,
                data_hora,
                row.data,
                row.hora,
                min_valor,
                avg_valor,
                max_valor,
            ),
        )

conn.commit()
print("Inserção concluída com sucesso!")


# Fecha a conexão com o banco de dados
conn.close()
