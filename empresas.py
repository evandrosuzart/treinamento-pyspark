from pyspark.sql.types import  DoubleType
from pyspark.sql.functions import regexp_replace
from spark_utils import SparkUtils

class EmpresaProcessor:
    def __init__(self, path, columns):
        self.spark_utils = SparkUtils()
        self.path = path
        self.columns = columns
        self.spark = self.spark_utils.create_spark_session()
    
    
    def rename_columns(self):
        self.empresas = self.spark_utils.rename_columns(self.empresas, columns)
        
    def load_data(self):
        self.empresas = self.spark_utils.load_data(path, self.spark)
        self.rename_columns()
        
    def format_capital_social(self):
        self.empresas = self.empresas.withColumn('capital_social_da_empresa', regexp_replace('capital_social_da_empresa', ',', '.'))
        self.empresas = self.empresas.withColumn('capital_social_da_empresa', self.empresas['capital_social_da_empresa'].cast(DoubleType()))

    def show_schema(self):
        self.empresas.printSchema()

    def show_data(self):
        self.empresas.show(truncate=False)


if __name__ == "__main__":
    path = 'data_bases/input/empresas'
    columns = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                        'qualificacao_do_responsavel', 'capital_social_da_empresa',
                        'porte_da_empresa', 'ente_federativo_responsavel']
    processor = EmpresaProcessor(path, columns)
    processor.format_capital_social()
    processor.show_schema()
    processor.show_data()
    
