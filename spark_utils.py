from pyspark.sql import SparkSession
from spark_session_exception import SparkSessionException
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date, regexp_replace
from pyspark.sql.types import  DoubleType
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
import json

class SparkUtils:
    
    def log(self, message, level="INFO"):
        print(f"{datetime.strftime(datetime.now(),'%Y-%m-%d-%H:%M:%S')} | {level} | {message}")
        
    def create_spark_session(self):
        self.log("SparkUtils.create_spark_session | iniciando a criação de spark session")
        try:
            spark = SparkSession.builder \
                .appName("Iniciando com Spark") \
                .config('spark.ui.port', '4041') \
                .master('local[1]') \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
                .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")\
                .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")\
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")\
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")\
                .getOrCreate()
            self.log("SparkUtils.create_spark_session | Finalizando a criação de spark session")
            return spark
        except Exception as e:
            error_msg = f"{datetime.now()} | Criando objeto SparkSession {e}"
            self.log(message=f"SparkUtils.create_spark_session: {error_msg}", level="ERROR")
            
            raise SparkSessionException(error_msg)
        
    def rename_columns(self,data_frame, columns):
        self.log(f"SparkUtils.rename_columns lista de colunas -> {json.dumps(columns)}")
        for index, colName in enumerate(columns):
            self.log(f"SparkUtils.convert_date_column convertendo coluna item -> _c{index} : {colName}")
            data_frame = data_frame.withColumnRenamed(f"_c{index}", colName)
        self.log(f"SparkUtils.rename_columns | terminou de renomear as colunas ")
        return data_frame
    
    def load_data(self, path, spark):
        self.log("SparkUtils.load_data | iniciando a leitura dos dados")
        data_frame = spark.read.csv(path,sep=';',inferSchema=True)
        self.log("SparkUtils.load_data | finalizando a leitura dos dados")
        self.log(f"SparkUtils.load_data className -> {data_frame.__class__.__name__}")
        return data_frame
    
    def convert_date_columns(self, date_columns, data_frame):
        self.log(f"SparkUtils.convert_date_column lista de colunas -> {json.dumps(date_columns)}")
        for date_column in date_columns:
            self.log(f"SparkUtils.convert_date_column convertendo coluna item -> {date_column}")
            data_frame = data_frame.withColumn(date_column, to_date(data_frame[date_column].cast(StringType()), 'yyyyMMdd'))
            self.log(f"SparkUtils.convert_date_column terminou de converter coluna item -> {date_column}")
        self.log(f"SparkUtils.convert_date_column finalizando ")
        return data_frame
    
    def convert_double_columns(self, double_columns, data_frame):
        self.log(f"SparkUtils.double_columns lista de colunas -> {json.dumps(double_columns)}")
        for double_column in double_columns:
            self.log(f"SparkUtils.double_columns convertendo coluna item -> {double_column}")
            data_frame = data_frame.withColumn(double_column, regexp_replace(double_column, ',', '.'))
            data_frame = data_frame.withColumn(double_column, data_frame[double_column].cast(DoubleType()))
            self.log(f"SparkUtils.double_columns terminou de converter coluna item -> {double_column}")
        self.log(f"SparkUtils.double_columns finalizando")
        return data_frame

    def format_double_value_column(self, column_name):
        self.data_frame = self.data_frame.withColumn(column_name, regexp_replace(column_name, ',', '.'))
        self.data_frame = self.data_frame.withColumn(column_name, self.data_frame[column_name].cast(DoubleType()))
    
    def save_dataframe_as_parquet_file(self, data_frame, path):
        self.log(f"SparkUtils.save_dataframe_as_parquet_file | Iniciando escrita de arquivo parquet -> {path}")
        data_frame.write.parquet(
            path=path,
            mode='overwrite'
            )
        self.log(f"SparkUtils.save_dataframe_as_parquet_file | Iniciando escrita de arquivo parquet -> {path}")
        
    def load_dataframe_as_parquet_file(self, spark, path):
        self.log(f"SparkUtils.load_dataframe_as_parquet_file | Iniciando leitura de arquivo csv -> {path}")
        try:
            data_frame = spark.read.parquet(path)
            self.log(f"SparkUtils.load_dataframe_as_parquet_file | Finalizando a leitura de arquivo csv -> {path}")
        except FileNotFoundError as  error:
            self.log(f"SparkUtils.load_dataframe_as_parquet_file | Erro ao localizar arquivo -> {error}", level="ERROR")
        return data_frame
    
    def save_dataframe_as_csv_file(self,data_frame,path):
        self.log(f"SparkUtils.save_dataframe_as_csv_file | Iniciando escrita de arquivo .csv -> {path}")
        data_frame.write.csv(
            path=path,
            mode='overwrite',
            sep=';',
            header=True
        )
        self.log(f"SparkUtils.save_dataframe_as_csv_file | Iniciando escrita de arquivo .csv -> {path}")
        
    def load_dataframe_csv_file_with_headers(self, spark, path):
        self.log(f"SparkUtils.load_dataframe_csv_file_with_headers | Iniciando leitura de arquivo .csv -> {path}")
        try:
            data_frame =  spark.read.csv(
                path,
                sep=';',
                inferSchema=True,
                header=True
            )
            self.log(f"SparkUtils.load_dataframe_csv_file_with_headers | Finalizando a leitura de arquivo .csv -> {path}")
        except FileNotFoundError as  error:
            self.log(f"SparkUtils.load_dataframe_csv_file_with_headers | Erro ao localizar arquivo -> {error}", level="ERROR")
        return data_frame
        
        
    def save_dataframe_as_orc_file(self, data_frame, path):
        self.log(f"SparkUtils.save_dataframe_as_orc_file | Iniciando escrita de arquivo .orc -> {path}")
        data_frame.write.orc(
            path=path,
            mode='overwrite'
        )
        self.log(f"SparkUtils.save_dataframe_as_orc_file | Finalizando escrita de arquivo .orc -> {path}")
        
    def load_dataframe_as_orc_file(self, spark, path):
        self.log(f"SparkUtils.load_dataframe_as_orc_file | Iniciando leitura de arquivo .orc -> {path}")
        try:
            data_frame =  spark.read.orc(path)
            self.log(f"SparkUtils.load_dataframe_as_orc_file | Finalizando a leitura de arquivo .orc -> {path}")
        except FileNotFoundError as  error:
            self.log(f"SparkUtils.load_dataframe_as_orc_file | Erro ao localizar arquivo -> {error}", level="ERROR")
        return data_frame
