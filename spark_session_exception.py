class SparkSessionException(Exception):
    def __init__(self, mensagem):
        super().__init__(mensagem)