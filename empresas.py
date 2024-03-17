from dynamic_dataframe import DynamicDataFrame


source_path = 'data_bases/input/empresas'
target_csv_path = 'data_bases/input/empresas/csv'
target_parquet_path = 'data_bases/input/empresas/parquet'
target_orc_path = 'data_bases/input/empresas/orc'

columns = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                    'qualificacao_do_responsavel', 'capital_social_da_empresa',
                    'porte_da_empresa', 'ente_federativo_responsavel']
double_columns = ['capital_social_da_empresa']

empresas = DynamicDataFrame(source_path,columns)
empresas.data_frame = empresas.convert_double_columns(double_columns)
empresas.show_data()

empresas.save_dataframe_as_csv_file(target_csv_path)

csv_data_frame = empresas.load_dataframe_csv_file_with_headers(target_csv_path)
csv_data_frame.show()

empresas.save_dataframe_as_parquet_file(target_parquet_path)

parquet_data_frame = empresas.load_dataframe_as_parquet_file(target_parquet_path)
parquet_data_frame.show()

empresas.save_dataframe_as_orc_file(target_orc_path)

orc_data_frame = empresas.load_dataframe_as_orc_file(target_orc_path)
orc_data_frame.show()