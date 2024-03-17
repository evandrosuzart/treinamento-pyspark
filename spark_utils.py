from pyspark.sql import SparkSession
from spark_session_exception import SparkSessionException
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date
from datetime import datetime

class SparkUtils:
    
    def log(self, message, level="INFO"):
        print(f"{datetime.strftime(datetime.now(),'%Y-%m-%d-%H:%M:%S')} | {level} | {message}")
        
    def create_spark_session(self):
        try:
            spark = SparkSession.builder \
                .appName("Iniciando com Spark") \
                .config('spark.ui.port', '4041') \
                .master('local[*]') \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
                .getOrCreate()
            return spark
        except Exception as e:
            error_msg = f"{datetime.now()} | Criando objeto SparkSession {e}"
            self.log(message="SparkUtils.create_spark_session: {error_msg}", level="ERROR")
            
            raise SparkSessionException(error_msg)
        
    def rename_columns(self, dynamic_frame, columns):
        for index, colName in enumerate(columns):
            dynamic_frame = dynamic_frame.withColumnRenamed(f"_c{index}", colName)
        return dynamic_frame
    
    def load_data(self, path, spark):
        data = spark.read.csv(path,sep=';',inferSchema=True)
        return data
    
    def convert_date_column(self,column_name, dynamic_frame ):
        return dynamic_frame.withColumn(column_name, to_date(dynamic_frame[column_name].cast(StringType()), 'yyyyMMdd'))
        
