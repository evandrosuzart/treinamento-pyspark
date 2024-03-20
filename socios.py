from dynamic_dataframe import DynamicDataFrame

source_path = 'data_bases/input/socios'
target_csv_path = 'data_bases/input/socios/csv'
target_parquet_path = 'data_bases/input/socios/parquet'
target_orc_path = 'data_bases/input/socios/orc'

columns = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio', 'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']
date_columns = ['data_de_entrada_sociedade']


socios = DynamicDataFrame()

socios.load_dataframe_csv_file_without_headers(source_path)
socios.rename_columns(columns)
socios.convert_date_columns(date_columns)
socios.show_data()
socios.save_dataframe_as_csv_file(target_csv_path)
socios.save_dataframe_as_orc_file(target_orc_path)
socios.end_session()


csv_data_frame = DynamicDataFrame()
csv_data_frame.load_dataframe_csv_file_with_headers(target_csv_path)
csv_data_frame.show_data()
csv_data_frame.save_dataframe_as_parquet_file(target_parquet_path)
csv_data_frame.end_session()

parquet_data_frame = DynamicDataFrame()
parquet_data_frame.load_dataframe_as_parquet_file(target_parquet_path)
parquet_data_frame.show_data()
parquet_data_frame.end_session()


orc_data_frame = DynamicDataFrame()
orc_data_frame.load_dataframe_as_orc_file(target_orc_path)
orc_data_frame.show_data()
orc_data_frame.end_session()

