from spark_utils import SparkUtils

class SociosDataProcessor:
    def __init__(self, path, columns):
        self.spark_utils = SparkUtils()
        self.path = path
        self.columns = columns
        self.spark = self.spark_utils.create_spark_session()
        
        
        
    def rename_columns(self):
        self.socios = self.spark_utils.rename_columns(self.socios, self.columns)
        
    def load_data(self):
        self.socios = self.spark_utils.load_data(self.path, self.spark)
        self.rename_columns()

    def convert_date_column(self):
        self.socios = self.spark_utils.convert_date_column("data_de_entrada_sociedade", self.socios)

    def show_data(self):
        self.socios.printSchema()
        self.socios.show(truncate=False)


columns = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio', 'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']
path = 'data_bases/input/socios'
processor = SociosDataProcessor(path, columns)
processor.convert_date_column()
processor.show_data()
