import csv
import datetime
import os.path
import scrapy
import time
from minio import Minio
from minio.error import ResponseError
from scrapy.crawler import CrawlerProcess
import psycopg2


import warnings

warnings.filterwarnings("ignore")


start_urls = ["https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM#position=11&search_layout=grid&type=item&tracking_id=a1976802-4bbe-4d4d-a00b-dfbda8b60ce9"]


csv_exists = os.path.isfile("dados-scrapy.csv")
print("CSV_EXISTS: ", csv_exists)


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
            preco_completo = preco_produto.replace(".", "") + "," + centavos_produto

        # Pega a data e hora atual
        data = datetime.datetime.now().strftime("%Y-%m-%d")
        hora = datetime.datetime.now().strftime("%H:%M:%S")

        # Escreve as informações no arquivo CSV
        with open(
            "dados-scrapy.csv", mode="a+" if csv_exists else "w+", newline=""
        ) as csv_file:
            
            fieldnames = ["site", "link", "data", "hora", "valor"]

            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            # Escreve o cabeçalho do arquivo CSV se ele não existir
            csv_file.seek(0)

            first_char = csv_file.read(1)
            if not csv_exists:
                writer.writeheader()
            if not first_char:
                writer.writeheader()

            writer.writerow(
                {
                    "site": site,
                    "link": response.url,
                    "data": data,
                    "hora": hora,
                    "valor": preco_completo,
                }
            )

        # Envia o arquivo CSV para o bucket do MinIO
        try:
            minio_client.fput_object(
                "meu-bucket", "dados-scrapy.csv", "dados-scrapy.csv"
            )
            self.logger.info("Arquivo CSV enviado para o bucket com sucesso!")

        except ResponseError as err:
            self.logger.error(err)


if __name__ == "__main__":
    
    start_time = time.time()

    # Configuração do cliente MinIO
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # Configuração do logging do Scrapy
    from scrapy.utils.log import configure_logging

    configure_logging()

    # Executa o spider
    process = CrawlerProcess()
    process.crawl(ProductSpider, start_urls=start_urls, minio_client=minio_client)
    process.start()   

    # Conecta ao banco de dados
    conn = psycopg2.connect(
        host="localhost",
        database="produtos",
        user="admin",
        password="admin"
    )

    # Cria uma tabela chamada 'produtos'
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS produtos (
            site TEXT,
            link TEXT,
            data DATE,
            hora TIME,
            valor NUMERIC(10,2)
        )
        """
    )
    conn.commit()

    # Insere os dados no banco de dados
    with open("dados-scrapy.csv", mode="r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            valor = row["valor"].replace(",", ".")
            cur.execute(
                """
                INSERT INTO produtos (site, link, data, hora, valor)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row["site"], row["link"], row["data"], row["hora"], valor)
            )
        conn.commit()

    # Fecha a conexão com o banco de dados
    cur.close()
    conn.close()

    end_time = time.time()

    # Calcula o tempo total de execução
    total_time = end_time - start_time
    print(f"Tempo total de execução: {total_time} segundos")
