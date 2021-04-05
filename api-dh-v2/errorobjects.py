class L20notfound(Exception):
    def __init__(self, message, payload=None):
        self.message = "Produto ({0}) nao encontrados! ".format(payload['Produto'],payload['L20'],)
        self.payload = payload # you could add more args
    def __str__(self):
        return str(self.message)
    