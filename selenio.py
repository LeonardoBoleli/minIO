import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from minio import Minio
from minio.error import ResponseError
import csv
from datetime import datetime

# URL do produto no Mercado Livre
url_mercadolivre = "https://produto.mercadolivre.com.br/MLB-2644395073-processador-intel-core-i7-10700-box-lga-1200-bx8070110700-_JM#position=11&search_layout=grid&type=item&tracking_id=a1976802-4bbe-4d4d-a00b-dfbda8b60ce9"

# URL do produto na Amazon
url_amazon = "https://www.amazon.com.br/PROCESSADOR-I7-10700-NUCLEOS-THREADS-BX8070110700/dp/B0883NPRL9/ref=sr_1_1?__mk_pt_BR=%C3%85M%C3%85%C5%BD%C3%95%C3%91&crid=20KG0SP4GJPQW&keywords=intel+core+i7+10700&qid=1681846177&sprefix=intel+core+i7+10700%2Caps%2C254&sr=8-1&ufe=app_do%3Aamzn1.fos.25548f35-0de7-44b3-b28e-0f56f3f96147"

# Configurações do Selenium para usar o user agent do Google Chrome
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")

# Inicializa o driver do Selenium
service = Service('/path/to/chromedriver')  # troque pelo caminho correto do seu chromedriver
driver = webdriver.Chrome(service=service, options=options)

# Faz a requisição GET na URL do Mercado Livre e pega o HTML da página
driver.get(url_mercadolivre)
wait = WebDriverWait(driver, 10)
wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".andes-money-amount__fraction")))

# Pegando o preço do produto
preco_produto_ml = driver.find_element(By.CSS_SELECTOR, ".andes-money-amount__fraction").text.strip()

# Pegando os centavos
centavos_ml = driver.find_element(By.CSS_SELECTOR, ".andes-money-amount__cents.andes-money-amount__cents--superscript-36").text.strip()

# Concatenando os valores
preco_completo_ml = preco_produto_ml.replace('.', '') + ',' + centavos_ml

# Faz a requisição GET na URL da Amazon e pega o HTML da página
driver.get(url_amazon)
time.sleep(5)
wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".a-price-whole")))

# Encontra as tags com os valores do preço do produto na Amazon
preco_produto_amazon = driver.find_element(By.CSS_SELECTOR, ".a-price-whole").text
centavos_produto_amazon = driver.find_element(By.CSS_SELECTOR, ".a-price-fraction").text

# Concatena os valores do preço do produto na Amazon em um único valor
preco_amazon = preco_produto_amazon + "." + centavos_produto_amazon

# Fecha o driver do Selenium
driver.quit()

data = datetime.now().strftime("%Y-%m-%d")
hora = datetime.now().strftime("%H:%M:%S")

import boto3
import csv


# Configuração do cliente MinIO
minio_client = Minio(
    endpoint="localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Verifica se o arquivo CSV já existe no bucket
try:
    objects = minio_client.list_objects("meu-bucket")
    file_exists = False
    for obj in objects:
        if obj.object_name == "dados.csv":
            file_exists = True
            break
except ResponseError as err:
    print(err)

# Abre o arquivo CSV em modo de escrita (append) ou cria um novo arquivo
with open('dados.csv', mode='a' if file_exists else 'w') as csv_file:
    fieldnames = ['site', 'link', 'data', 'hora', 'valor']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
    # Escreve o cabeçalho do arquivo CSV se ele não existir
    if not file_exists:
        writer.writeheader()

    # Pega a data e hora atual
    data = datetime.now().strftime("%Y-%m-%d")
    hora = datetime.now().strftime("%H:%M:%S")

    # Escreve as informações no arquivo CSV
    writer.writerow({'site': 'Mercado Livre', 'link': url_mercadolivre, 'data': data, 'hora': hora, 'valor': preco_completo_ml})
    writer.writerow({'site': 'Amazon', 'link': url_amazon, 'data': data, 'hora': hora, 'valor': preco_amazon})

    # Envia o arquivo CSV para o bucket do MinIO
    try:
        minio_client.fput_object("meu-bucket", "dados.csv", "dados.csv")
        print("Arquivo CSV enviado para o bucket com sucesso!")
    except ResponseError as err:
        print(err)
