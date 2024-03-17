from spark_utils import SparkUtils

class EstabelecimentosProcessor:
    def __init__(self, path, columns):
        
        self.spark_utils = SparkUtils()
        self.path = path
        self.columns = columns
        self.spark = self.spark_utils.create_spark_session()
        
        
    def rename_columns(self):
        self.estabelecimentos = self.spark_utils.rename_columns(self.estabelecimentos, self.columns)
        
    def load_data(self):
        self.estabelecimentos = self.spark_utils.load_data(self.path, self.spark)
        self.rename_columns()

    def convert_dates(self):
        self.estabelecimentos = self.spark_utils.convert_date_column('data_situacao_cadastral', self.estabelecimentos)
        self.estabelecimentos = self.spark_utils.convert_date_column('data_de_inicio_atividade', self.estabelecimentos)
        self.estabelecimentos = self.spark_utils.convert_date_column('data_da_situacao_especial', self.estabelecimentos)

    def process_data(self):
        self.convert_dates()
        return self.estabelecimentos
    
    def show_data(self):
        self.estabelecimentos.show(truncate=False)

if __name__ == '__main__':
    path = 'data_bases/input/estabelecimentos'
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
    
    processor = EstabelecimentosProcessor(path,columns)
    processed_data = processor.process_data()
    processor.show_data()
