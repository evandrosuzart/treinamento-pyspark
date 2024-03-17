from pyspark.sql.functions import regexp_replace
from dynamic_dataframe import DynamicDataFrame


path = 'data_bases/input/empresas'
columns = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                    'qualificacao_do_responsavel', 'capital_social_da_empresa',
                    'porte_da_empresa', 'ente_federativo_responsavel']
double_columns = ['capital_social_da_empresa']

empresas_df = DynamicDataFrame(path,columns)
empresas_df.data_frame = empresas_df.convert_double_columns(double_columns)
empresas_df.show_data()