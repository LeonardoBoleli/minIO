import csv
import datetime
import os.path
import scrapy
import time
import psycopg2
import io
from minio import Minio
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import warnings
warnings.filterwarnings("ignore")
start_urls = [
    "https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM#position=11&search_layout=grid&type=item&tracking_id=a1976802-4bbe-4d4d-a00b-dfbda8b60ce9"
]
bucket_name = "meu-bucket"
csv_file_path = "dados-scrapy.csv"
class ProductSpider(scrapy.Spider):
    name = "product_spider"
    def __init__(self, *args, **kwargs):
        super(ProductSpider, self).__init__(*args, **kwargs)
        self.start_urls = kwargs.get("start_urls", [])
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
                },
            )
    def parse(self, response):
        site = ""
        if "mercadolivre.com.br" in response.url:
            site = "Mercado Livre"
            preco_produto = response.css(".andes-money-amount__fraction::text").get()
            centavos_produto = response.css(
                ".andes-money-amount__cents.andes-money-amount__cents--superscript-36::text"
            ).get()
            preco_completo = str(preco_produto.replace(".", "") + "." + centavos_produto)
        # Pega a data e hora atual
        data = datetime.datetime.now().strftime("%Y-%m-%d")
        hora = datetime.datetime.now().strftime("%H:%M:%S")
        # Envia as informações para o arquivo CSV no bucket
        try:
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
            
            # Lê o arquivo CSV do bucket, se existir
            csv_data = ""
            if minio_client.bucket_exists(bucket_name):
                try:
                    obj = minio_client.get_object(bucket_name, csv_file_path)
                    csv_data = obj.data.decode("utf-8")
                except Exception as e:
                    print("Erro ao ler o arquivo CSV do bucket:", e)
            
            # Atualiza o conteúdo do arquivo CSV com as novas informações
            csv_data += f"{site},{response.url},{data},{hora},{preco_completo}\n"
            
            # Envia o arquivo CSV atualizado para o bucket
            minio_client.put_object(
                bucket_name,
                csv_file_path,
                io.BytesIO(csv_data.encode("utf-8")),
                len(csv_data),
                content_type="text/csv"
            )
            print("Arquivo CSV atualizado no bucket com sucesso!")
        except Exception as e:
            print("Erro ao enviar o arquivo CSV para o bucket:", e)
if __name__ == "__main__":
    start_time = time.time()
    # Cria o cliente Minio
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    process = CrawlerProcess()
    process.crawl(ProductSpider, start_urls=start_urls)
    process.start()
    # Conecta ao banco de dados
    conn = psycopg2.connect(
        host="localhost", database="produtos", user="admin", password="admin"
    )
    # Cria uma tabela chamada 'produtos'
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS produtos (
            site TEXT,
            link TEXT,
            data TEXT,
            hora TEXT,
            valor TEXT
        )
        """
    )
    conn.commit()
    # Lê o arquivo CSV diretamente do bucket
    try:
        csv_object = minio_client.get_object(bucket_name, csv_file_path)
        csv_content = csv_object.data.decode("utf-8")
        # Insere os dados no banco de dados
        csv_io = io.StringIO(csv_content)
        csv_reader = csv.DictReader(csv_io)

        for row in csv_reader:
            # Insere os dados no banco de dados
            link = row["link"]
            data = row["data"]
            hora = row["hora"]

            # Verifica se a linha já existe na tabela utilizando a data e hora como critério
            cur.execute(
                """
                INSERT INTO produtos (site, link, data, hora, valor)
                VALUES (%s, %s, %s, %s, %s)
                SELECT COUNT(*) FROM produtos WHERE link = %s AND data = %s AND hora = %s
                """,
                (row["site"], row["link"], row["data"], row["hora"], row["valor"]),
                (link, data, hora),
            )
            count = cur.fetchone()[0]

            if count == 0:
                # Insere os dados no banco de dados
                cur.execute(
                    """
                    INSERT INTO produtos (site, link, data, hora, valor)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row["site"], link, data, hora, row["valor"]),
                )

        conn.commit()
        print("Dados do arquivo CSV inseridos no banco de dados com sucesso!")
    except Exception as err:
        print("Erro ao ler o arquivo CSV do bucket:", err)



    # Fecha a conexão com o banco de dados
    cur.close()
    conn.close()
    end_time = time.time()
    # Calcula o tempo total de execução
    total_time = end_time - start_time
    print(f"Tempo total de execução: {total_time} segundos")