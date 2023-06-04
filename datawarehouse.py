import psycopg2
import pandas as pd

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
        for value in row:
            print(value, ": ", type(value))
        insert_query = """
            INSERT INTO warehouse (produto, valor, link, data, hora, min_valor, avg_valor, max_valor)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, row)
conn.commit()
print("Inserção concluída com sucesso!")

# Fecha a conexão com o banco de dados
conn.close()
