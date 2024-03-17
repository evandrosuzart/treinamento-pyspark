from spark_utils import SparkUtils
import json
from pyspark.sql.dataframe import DataFrame

spark_utils = SparkUtils()

class DynamicDataFrame:
    def __init__(self, path, columns):
        self.path = path
        self.columns = columns
        self.spark = spark_utils.create_spark_session()
        self.data_frame = spark_utils.load_data(self.path, self.spark)
        self.data_frame = spark_utils.rename_columns(self.data_frame, self.columns)
    
        
    def convert_date_columns(self, date_columns):
        spark_utils.log(f"DynamicDataFrame.convert_date_column : {json.dumps(date_columns)}")
        return spark_utils.convert_date_columns(date_columns, self.data_frame)
        

    def show_data(self):
        self.data_frame.printSchema()
        self.data_frame.show(truncate=False)

if __name__ == "__main__":
    
    path = 'data_bases/input/socios'
    columns = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio', 'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']
    date_columns = ['data_de_entrada_sociedade']
    ddf = DynamicDataFrame(path, columns)
    ddf.convert_date_columns(date_columns)
    ddf.show_data()
