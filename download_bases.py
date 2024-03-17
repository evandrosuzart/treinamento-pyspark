import urllib.request
import zipfile
from datetime import datetime
import os
import shutil


URL_EMPRESAS = "https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/empresas.zip"
URL_ESTABELECIMENTOS = "https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/estabelecimentos.zip"
URL_SOCIO = "https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/socios.zip"

first_time = datetime.now()

def rm_dir_recursive(base_path):
    try:
        shutil.rmtree(base_path)
        print(f"{datetime.now()} | Sucesso ao excluir o diretório {base_path}")
    except FileNotFoundError:
        print(f"{datetime.now()} | Diretório não localizado {base_path}")
    except PermissionError:
        print(f"{datetime.now()} | Permição negada")
    except Exception as e:
        print(f"{datetime.now()} | Erro não esperado ao tentar excluir diretório | {str(e)}")

def download_and_extract_database(url,file_name):
    print(f"{datetime.now()} | Iniciando o download | {url} | {file_name}")
    urllib.request.urlretrieve(url, file_name)
    print(f"{datetime.now()} | Finalizado o download, iniciando a extração do arquivo | {file_name}")
    zipfile.ZipFile(file_name).extractall('data_bases/input')
    print(f"{datetime.now()} | Finalizando a extração do arquivo | {file_name}")
    print(f"{datetime.now()} | Iniciando a exclusão do arquivo | {file_name}")
    os.remove(file_name) 
    print(f"{datetime.now()} | Finalizando a exclusão do arquivo | {file_name}")
    

now  = datetime.now()
print(f"{now} | Iniciando processo")
rm_dir_recursive('data_bases')

download_and_extract_database(URL_EMPRESAS,'empresas.zip')
download_and_extract_database(URL_ESTABELECIMENTOS,'estabelecimentos.zip')
download_and_extract_database(URL_SOCIO,'socios.zip')


duration = now - first_time
duracao = duration.total_seconds() 
print(f"{datetime.now()} | Finalizando a carga de dados em {duracao} segundos")
