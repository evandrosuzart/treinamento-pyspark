# Começando o Trabalho

---

Projeto de estudo com a funcionalidade aprender a manipular dados com Apache Spark

## Linguagem : Python

## Versões de softwares necessárias

|**biblioteca**|**Versão**|
|:--:|:--:|
|JAVA|8 ou superior|
|Python| 3|
|PySpark|spark-3.5.1-bin-hadoop3|

## Configurando o ambiente

### Certifique-se que você instalou as bibliotecas necessárias

- JAVA_HOME = Caminho de instalação do JAVA
- SPARK_HOME  = Caminho do diretório do spark spark-3.5.1-bin-hadoop3
- HADOOP_HOME = %SPARK_HOME%
- PYSPARK_PYTHON = C:\ProgramData\anaconda3\python.exe
- ARROW_PRE_0_15_IPC_FORMAT = 1
- Adcione na variável path o valor HADOOP_HOME\bin
- Acese o link da ferramenta  [winutils](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin) e baixe os arquivos hadoop.dll e winutils.exe
- Salve os arquivos no diretório $HADOOP_HOME\bin

#### Arquivos de dataframe

|**Nome do Arquivo**|**Conteúdo**|
|:--:|:--:|
|empresas.py|Informações de empresas|
|socios.py|Informações dos sócios|
|estabelecimentos.py|Informações cadastrais de estabelecimentos|

---

Após realizar esses procedimentos você pode executar a aplicação por meio do comando spark-submit {Nome do Arquivo}


# Utilizando notebooks

O arquivo 'notebooks/meus_exemplos_noteobook.ipynb' contém um pequeno exemplo para manipular a tabela empresas com notebook, possibilitando uma interface mais amigável para a exibição de cada tabela, você pode customizar esse arquivo caso tenha receio de alterar os outros scripts