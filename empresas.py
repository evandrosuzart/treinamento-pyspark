from pyspark.sql.functions import regexp_replace
from dynamic_dataframe import DynamicDataFrame


source_path = 'data_bases/input/empresas'
target_csv_path = 'data_bases/input/empresas/csv'
target_parquet_path = 'data_bases/input/empresas/parquet'
target_orc_path = 'data_bases/input/empresas/orc'

columns = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                    'qualificacao_do_responsavel', 'capital_social_da_empresa',
                    'porte_da_empresa', 'ente_federativo_responsavel']
double_columns = ['capital_social_da_empresa']

empresas_df = DynamicDataFrame(source_path,columns)
empresas_df.data_frame = empresas_df.convert_double_columns(double_columns)
empresas_df.show_data()

empresas_df.save_dataframe_as_csv_file(target_csv_path)

new_csv_df = empresas_df.load_dataframe_csv_file_with_headers(target_csv_path)
new_csv_df.show()

empresas_df.save_dataframe_as_parquet_file(target_parquet_path)

new_parquet_df = empresas_df.load_dataframe_as_parquet_file(target_parquet_path)
new_parquet_df.show()

empresas_df.save_dataframe_as_orc_file(target_orc_path)

new_orc_df = empresas_df.load_dataframe_as_orc_file(target_orc_path)
new_orc_df.show()