from dynamic_dataframe import  DynamicDataFrame


socios_path = 'data_bases/input/socios/parquet'
empresas_path = 'data_bases/input/empresas/parquet'

socio_empresa_parquet_target = 'data_bases/input/socios_empresas/parquet'
socio_empresa_csv_target = 'data_bases/input/socios_empresas/csv'
socio_empresa_orc_target = 'data_bases/input/socios_empresas/orc'

socio_df = DynamicDataFrame()
empresa_df = DynamicDataFrame()

socio_df.load_dataframe_as_parquet_file(socios_path)
empresa_df.load_dataframe_as_parquet_file(empresas_path)


empresa_df.show_data()
socio_df.show_data()

socio_empresa = socio_df

socio_empresa.data_frame = socio_empresa.data_frame.withColumnRenamed('cnpj_basico','cnpj')
socio_empresa.show_data()
socio_empresa.data_frame = socio_empresa.data_frame.join(empresa_df.data_frame, socio_empresa.data_frame['cnpj'] == empresa_df.data_frame['cnpj_basico'], how='left')
socio_empresa.show_data()

socio_empresa.save_dataframe_as_parquet_file(socio_empresa_parquet_target)
socio_empresa.save_dataframe_as_csv_file(socio_empresa_csv_target)
socio_empresa.save_dataframe_as_orc_file(socio_empresa_orc_target)

empresa_df.end_session()
socio_df.end_session()

