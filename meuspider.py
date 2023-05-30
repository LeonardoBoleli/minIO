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
    "https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM#position=11&search_layout=grid&type=item&tracking_id=a1976802-4bbe-4d4d-a00b-dfbda8b60ce9",
    "https://www.mercadolivre.com.br/gabinete-gamer-lian-li-redragon-modelo-o11dynamic-mini-branc/p/MLB23190291?pdp_filters=category:MLB1696#searchVariation=MLB23190291&position=2&search_layout=grid&type=product&tracking_id=dab59008-df8b-46f0-bd34-2f8053eca38f",
    "https://www.mercadolivre.com.br/placa-de-video-nvidia-galax-geforce-rtx-30-series-rtx-3060-36nsl8md6occ-oc-edition-8gb/p/MLB20736337?pdp_filters=category:MLB1658#searchVariation=MLB20736337&position=3&search_layout=grid&type=product&tracking_id=d3ba3a55-4cda-4a8e-8f88-fd6382009246",
    "https://produto.mercadolivre.com.br/MLB-1676543787-placa-me-asus-tuf-b460m-plus-b460-lga1200-ddr4-10a-ger-_JM#position=25&search_layout=grid&type=item&tracking_id=6f6be4a6-644a-43c2-8e5b-285259d18b1e",
    "https://www.mercadolivre.com.br/memoria-ram-fury-color-preto-16gb-1-hyperx-hx426c16fb16/p/MLB14728888?pdp_filters=category:MLB1694#searchVariation=MLB14728888&position=8&search_layout=grid&type=product&tracking_id=b0b0bebf-c99d-42c0-b721-f62d1a64d3a1",
    "https://produto.mercadolivre.com.br/MLB-3381940936-water-cooler-corsair-h100-rgb-240mm-radiator-preto-_JM#position=6&search_layout=grid&type=item&tracking_id=137704c3-2bb1-4abe-8809-bd43e8c8f05d"
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
            if centavos_produto:
                preco_completo = f"{preco_produto}.{centavos_produto}"
            else:
                preco_completo = f"{preco_produto}.00"

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
        csv_data = ""
        if minio_client.bucket_exists(bucket_name):
            try:
                obj = minio_client.get_object(bucket_name, csv_file_path)
                csv_data = obj.data.decode("utf-8")
            except Exception as e:
                print("Erro ao ler o arquivo CSV do bucket:", e)

        csv_io = io.StringIO(csv_data)
        csv_reader = csv.reader(csv_io)
        rows = list(csv_reader)

        print("csv_io: ", csv_io)
        print("csv_reader: ", csv_reader)
        print("csv_data: ", csv_data)

        # Verifica se o arquivo CSV possui pelo menos uma linha
        if csv_data:
            # Substitui a primeira linha do arquivo CSV pelos nomes de colunas desejados
            column_names = ["site", "link", "data", "hora", "valor"]
            csv_data = ",".join(column_names) + csv_data[csv_data.find("\n"):]

            # Divide o conteúdo do arquivo CSV em linhas
            lines = csv_data.split("\n")

            # Obtém a primeira linha do arquivo CSV
            first_line = lines[0]

            # Separa os valores da primeira linha por vírgula (ou outro delimitador utilizado no arquivo)
            column_names = first_line.split(",")

            print("Nomes das colunas:")
            print(column_names)

        for row in csv_reader:
            # Insere os dados no banco de dados
            link = row.get("link")
            data = row.get("data")
            hora = row.get("hora")
            print("link: ", link)
            print("data: ", data)
            print("hora: ", hora)

            if link and data and hora:
                # Verifica se a linha já existe na tabela utilizando a URL, data e hora como critério
                cur.execute(
                    """
                    SELECT COUNT(*) FROM produtos WHERE link = %s AND data = %s AND hora = %s
                    """,
                    (link, data, hora),
                )
                count = cur.fetchone()[0]
                print("count: ", count)

                if count == 0:
                    # Insere os dados no banco de dados
                    cur.execute(
                        """
                        INSERT INTO produtos (site, link, data, hora, valor)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (row.get("site"), link, data, hora, row.get("valor")),
                    )
                    print("Dados inseridos:", link, data, hora)

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