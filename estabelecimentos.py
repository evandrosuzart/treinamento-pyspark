from dynamic_dataframe import DynamicDataFrame

source_path = 'data_bases/input/estabelecimentos'
target_csv_path = 'data_bases/input/estabelecimentos/csv'
target_parquet_path = 'data_bases/input/estabelecimentos/parquet'
target_orc_path = 'data_bases/input/estabelecimentos/orc'

columns = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
        'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais',
        'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
        'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
        'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
        'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial',
        'data_da_situacao_especial'
    ]
date_columns = ['data_situacao_cadastral', 'data_de_inicio_atividade', 'data_da_situacao_especial']

estabelecimentos = DynamicDataFrame(source_path,columns)
estabelecimentos.data_frame = estabelecimentos.convert_date_columns(date_columns)

#estabelecimentos.save_dataframe_as_csv_file(target_csv_path)

#csv_data_frame = estabelecimentos.load_dataframe_csv_file_with_headers(target_csv_path)
#csv_data_frame.show()


estabelecimentos.save_dataframe_as_parquet_file(target_parquet_path)
parquet_data_frame = estabelecimentos.load_dataframe_as_parquet_file(target_parquet_path)
parquet_data_frame.show()


#estabelecimentos.save_dataframe_as_orc_file(target_orc_path)
#orc_data_frame = estabelecimentos.load_dataframe_as_orc_file(target_orc_path)
#orc_data_frame.show()