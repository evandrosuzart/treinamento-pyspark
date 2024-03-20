from dynamic_dataframe import DynamicDataFrame


source_path = 'data_bases/input/empresas'
target_csv_path = 'data_bases/input/empresas/csv'
target_parquet_path = 'data_bases/input/empresas/parquet'
target_orc_path = 'data_bases/input/empresas/orc'

columns = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                    'qualificacao_do_responsavel', 'capital_social_da_empresa',
                    'porte_da_empresa', 'ente_federativo_responsavel']
double_columns = ['capital_social_da_empresa']



empresas = DynamicDataFrame()
empresas.load_dataframe_csv_file_without_headers(source_path)
empresas.rename_columns(columns)
empresas.convert_double_columns(double_columns)
empresas.show_data()
empresas.save_dataframe_as_csv_file(target_csv_path)
empresas.save_dataframe_as_parquet_file(target_parquet_path)
empresas.save_dataframe_as_orc_file(target_orc_path)
empresas.end_session()

csv_data_frame = DynamicDataFrame()
csv_data_frame.load_dataframe_csv_file_with_headers(target_csv_path)
csv_data_frame.show_data()
csv_data_frame.end_session()

parquet_data_frame = DynamicDataFrame()
parquet_data_frame.load_dataframe_as_parquet_file(target_parquet_path)
parquet_data_frame.show_data()
parquet_data_frame.end_session()

orc_data_frame = DynamicDataFrame()
orc_data_frame.load_dataframe_as_orc_file(target_orc_path)
orc_data_frame.show_data()
orc_data_frame.end_session()