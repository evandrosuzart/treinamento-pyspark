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
    
    def convert_double_columns(self, double_columns):
        spark_utils.log(f"DynamicDataFrame.convert_double_columns : {json.dumps(double_columns)}")
        return spark_utils.convert_double_columns(double_columns, self.data_frame)

    def show_data(self):
        self.data_frame.printSchema()
        self.data_frame.show(truncate=False)

