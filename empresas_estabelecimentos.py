from dynamic_dataframe import  DynamicDataFrame

empresas_path = 'data_bases/input/empresas/parquet'
estabelecimentos_path = 'data_bases/input/estabelecimentos/parquet'

empresas_estabelecimentos_parquet_target = 'data_bases/input/estabelecimentos_empresas/parquet'
empresas_estabelecimentos_csv_target = 'data_bases/input/estabelecimentos_empresas/csv'
empresas_estabelecimentos_orc_target = 'data_bases/input/estabelecimentos_empresas/orc'

estabelecimentos_df = DynamicDataFrame()
empresas_df = DynamicDataFrame()

estabelecimentos_df.load_dataframe_as_parquet_file(estabelecimentos_path)
empresas_df.load_dataframe_as_parquet_file(empresas_path)

estabelecimentos_df.show_data()
empresas_df.show_data()

empresas_estabelecimentos = empresas_df
empresas_estabelecimentos.data_frame = empresas_estabelecimentos.data_frame.withColumnRenamed('cnpj_basico', 'cnpj')
empresas_estabelecimentos.data_frame = empresas_estabelecimentos.data_frame.join(estabelecimentos_df.data_frame, empresas_estabelecimentos.data_frame['cnpj'] == estabelecimentos_df.data_frame['cnpj_basico'], how='left')

empresas_estabelecimentos.show_data()
empresas_estabelecimentos.save_dataframe_as_csv_file(empresas_estabelecimentos_csv_target)
empresas_estabelecimentos.save_dataframe_as_orc_file(empresas_estabelecimentos_orc_target)
empresas_estabelecimentos.save_dataframe_as_parquet_file(empresas_estabelecimentos_parquet_target)

