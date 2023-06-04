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

# Obtém os dados do Datalake
query = "SELECT * FROM produtos"
data = pd.read_sql(query, conn)
print("aqui deu bom")

# Combina as colunas 'data' e 'hora' em uma nova coluna 'data_hora'
data["data_hora"] = pd.to_datetime(data["data"] + " " + data["hora"])

# Remove as colunas 'data' e 'hora'
data = data.drop(["data", "hora"], axis=1)
data["data"] = data["data"].astype(str)
data["hora"] = data["hora"].astype(str)
print("aqui deu bom 2")

# Calcula os valores mínimos, médios e máximos para cada produto
aggregated_data = data.groupby("produto").agg({"valor": ["min", "mean", "max"]})
aggregated_data.columns = ["min_valor", "avg_valor", "max_valor"]
aggregated_data.reset_index(inplace=True)
print("aqui deu bom 3")

# Combina os dados com os valores agregados
enriched_data = data.merge(aggregated_data, on="produto")
print("aqui deu bom 4")

# Insere os dados enriquecidos na tabela do Data Warehouse
with conn.cursor() as cursor:
    for row in enriched_data.itertuples(index=False):
        insert_query = """
            INSERT INTO warehouse (produto, valor, link, data_hora, data, hora, min_valor, avg_valor, max_valor)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, row)
conn.commit()
print("aqui deu bom 5")

# Fecha a conexão com o banco de dados
conn.close()
